package main

import (
    "flag"
    "fmt"
    "json"
    "io/ioutil"
    "csv"
    "os"
    "http"
    "template"
    "bytes"
    "smtp"
//    "time"
)

const (
    numRoutines = 10
)

type RecConfig struct {
    RecUrl     string
    SmtpServer string
    SmtpFrom   string
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
        //respChan <- resp
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

    emails := make([]string, 1)
    emails[0] = email

    /* WTF, why doesn't this work?
    localTime := time.LocalTime
    dateStr   := localTime.Format("%a, %d %b %Y %H:%M:%S %z")
    */
    edata := new(EmailData)
    edata.FromAddress = mailer.Config.SmtpFrom
    edata.ToAddress   = email
    edata.Subject     = "Recommendations for you"
    edata.RecResponse = recResponse
    //edata.Date        = dateStr

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

    t, err := template.ParseFile(templateFile)
    if (err != nil) {
        fmt.Printf("Unable to parse template file %s\n", templateFile)
        fmt.Println(err)
        os.Exit(1)
    }

    return recConfig, dataFile, t

}

func readResults(respChan chan int) {
    for {
        resp := <- respChan
        fmt.Println(resp)
    }
}

func main() {

    recConfig, dataFile, t := parseArgs()
    client := new(http.Client)
    mailer := new(RecMailer)

    mailer.Config   = recConfig
    mailer.Template = t
    mailer.Http     = client


    dataReader, err := os.Open(dataFile)
    defer dataReader.Close()
    if err != nil {
        fmt.Printf("Unable to open data file %s\n", dataFile)
        fmt.Println(err)
        os.Exit(1)
    }
    
    dataCsvReader := csv.NewReader(dataReader)
    recsChan := make(chan []string)
    respChan := make(chan int)
  
    for i := 0; i < numRoutines; i++ {
        go mailer.launchProcessor(recsChan, respChan)
    }

    for j := 0; ; j++ {
        recs, err := dataCsvReader.Read()

        if (err == os.EOF) {
            break
        } else if (err != nil) {
            fmt.Printf("Error reading data file %s at line %d\n", dataFile, j)
            fmt.Println(err)
            os.Exit(1)
        }

        recsChan <- recs
    }

    //go readResults(respChan)
}
