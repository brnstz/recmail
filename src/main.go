package main

import (
    "flag"
    "fmt"
    "encoding/json"
    "io/ioutil"
    "encoding/csv"
    "os"
    "net/http"
    "text/template"
    "bytes"
    "net/smtp"
    "io"
    "time"
)

const (
    numRoutines = 30
    chanBuff = 100
)

type RecConfig struct {
    RecUrl     string
    SmtpServer string
    SmtpFrom   string
    EnvelopeFrom   string
}

type RecMailer struct {
    Config   RecConfig
    Template *template.Template
    Http     *http.Client
}

type EmailData struct {
    FromAddress string
    ToAddress   string
    Subject     string
    Date        string
    RecResponse RecResponse
}

type RecSuggestions struct {
    Url string
    Title string
    Abstract string
    Section string
    Byline string
    Thumbnail RecThumbnail
    Des_facet []string
}

type RecThumbnail struct {
    Url string
}

type RecResponse struct {
    Suggestions []RecSuggestions

}

func (mailer *RecMailer) launchProcessor(recsChan chan []string, respChan chan int) {
    for {
        rec := <- recsChan
        //fmt.Printf("Trying: %s, %s\n", rec[0], rec[1])
        resp := mailer.processOneRecord(rec[0], rec[1])

        if resp == 0 {
            fmt.Printf("Success for %s, %s\n", rec[0], rec[1])
        }
        //fmt.Println("hey! now thread")
        respChan <- resp
        //fmt.Println("hey! now thread 2")
    }
}

func (mailer *RecMailer) processOneRecord(id string, email string) int {
    var (
        recResponse RecResponse
    )

    fullUrl := fmt.Sprintf(mailer.Config.RecUrl, id)
    resp, err := mailer.Http.Get(fullUrl)

    if err != nil {
        fmt.Printf("Unable to get URL %s\n", fullUrl)
        fmt.Println(err)
        return 1
    }
    
    defer resp.Body.Close()
   
    readBytes, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        fmt.Printf("Unable to read from URL %s\n", fullUrl)
        fmt.Println(err)
        return 1
    }

    err = json.Unmarshal(readBytes, &recResponse)
    if (err != nil) {
        fmt.Printf("Unable to parse JSON http resp for user %s\n", id)
        fmt.Println(err)
        return 1
    }

    if len(recResponse.Suggestions) == 0 {
        fmt.Printf("No suggestions for user %s\n", id)
        return 1
    }

    // If there's no thumbnail, use a blank gif
    for recIndex := 0; recIndex < len(recResponse.Suggestions); recIndex++ {
        if len(recResponse.Suggestions[recIndex].Thumbnail.Url) == 0 {
            recResponse.Suggestions[recIndex].Thumbnail.Url = "http://graphics8.nytimes.com/images/misc/spacer.gif"
        }
    }

    emails := make([]string, 1)
    emails[0] = email

    localTime := time.Now()
    dateStr := localTime.Format(time.RFC1123Z)
    edata := new(EmailData)
    edata.FromAddress = mailer.Config.EnvelopeFrom
    edata.ToAddress   = email
    edata.Subject     = "Recommendations for you"
    edata.RecResponse = recResponse
    edata.Date        = dateStr

    buff := new(bytes.Buffer)
    mailer.Template.Execute(buff, edata)

    err = smtp.SendMail(mailer.Config.SmtpServer, nil, mailer.Config.SmtpFrom, emails, buff.Bytes())

    if err != nil {
        fmt.Printf("There was an error sending for user %s\n", id)
        fmt.Println(err)
        return 1
    }
    
    return 0
}

func parseArgs() (RecConfig, string, *template.Template) {
    var ( 
        recConfig    RecConfig
        configFile   string
        dataFile     string
        templateFile string
    )

    // Parse command line arguments
    flag.StringVar(&configFile,   "config",   "", "config file") 
    flag.StringVar(&dataFile,     "data",     "", "data file") 
    flag.StringVar(&templateFile, "template", "", "template file") 
    flag.Parse()

    // Read configFile into a JSON byte array
    configJsonBytes, err := ioutil.ReadFile(configFile)
    if (err != nil) {
        fmt.Printf("Unable to read file %s\n", configFile)
        fmt.Println(err)
        os.Exit(1)
    }

    // Convert into a RecConfig struct
    err = json.Unmarshal(configJsonBytes, &recConfig)
    if (err != nil) {
        fmt.Printf("Unable to parse JSON in %s\n", configFile)
        fmt.Println(err)
        os.Exit(1)
    }

    t, err := template.ParseFiles(templateFile)
    if (err != nil) {
        fmt.Printf("Unable to parse template file %s\n", templateFile)
        fmt.Println(err)
        os.Exit(1)
    }

    return recConfig, dataFile, t

}


func readDataFile(dataFile string, recsChan chan []string, doneReadingChan chan int) {

    dataReader, err := os.Open(dataFile)

    if err != nil {
        fmt.Printf("Unable to open data file %s\n", dataFile)
        fmt.Println(err)
        os.Exit(1)
    }

    defer dataReader.Close()

    dataCsvReader := csv.NewReader(dataReader)

    numLines := 0
    for {
        recs, err := dataCsvReader.Read()

        if (err == io.EOF) {
            break
        } else if (err != nil) {
            fmt.Printf("Error reading data file %s at line %d\n", dataFile, numLines + 1)
            fmt.Println(err)
            os.Exit(1)
        }
        
        recsChan <- recs 

        numLines++
    }
    
    doneReadingChan <- numLines
}

func readResults(respChan chan int, allRequestsDoneChan chan int, doneReadingChan chan int) {
    numLines := -1

    finishedRequests := 0 
    results := [...]int{0, 0}
    for {

        result := <- respChan
        results[result]++
        finishedRequests++

        // See if we've finished reading the input file yet
        if numLines == -1 {
            select {
                case numLines = <- doneReadingChan:
                default:
            }
        } 

        if (numLines != -1) && (finishedRequests >= numLines) {
            fmt.Println(results)
            allRequestsDoneChan <- 1
            break
        }
    }
}

func main() {

    recConfig, dataFile, t := parseArgs()
    client := new(http.Client)
    mailer := new(RecMailer)

    mailer.Config   = recConfig
    mailer.Template = t
    mailer.Http     = client
    
    recsChan := make(chan []string, chanBuff)
    respChan := make(chan int, chanBuff)
    doneReadingChan := make(chan int)
    allRequestsDoneChan := make(chan int)
  
    for i := 0; i < numRoutines; i++ {
        go mailer.launchProcessor(recsChan, respChan)
    }

    go readDataFile(dataFile, recsChan, doneReadingChan)

    go readResults(respChan, allRequestsDoneChan, doneReadingChan)

    <-allRequestsDoneChan
    // Infinite wait
    //<-make(chan interface{}); 

    //go readResults(respChan)
}
