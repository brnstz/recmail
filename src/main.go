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

func (mailer *RecMailer) processOneRecord(id string, email string) {
    var (
        recResponse RecResponse
    )

    fullUrl := fmt.Sprintf(mailer.Config.RecUrl, id)
    resp, err := mailer.Http.Get(fullUrl)
    defer resp.Body.Close()
    // FIXME: not a fatal error
    if err != nil {
        fmt.Printf("Unable to get URL %s\n", fullUrl)
        fmt.Println(err)
        os.Exit(1)
    }
   
    readBytes, err := ioutil.ReadAll(resp.Body)
    // FIXME: not a fatal error
    if err != nil {
        fmt.Printf("Unable to read from URL %s\n", fullUrl)
        fmt.Println(err)
        os.Exit(1)
    }

    err = json.Unmarshal(readBytes, &recResponse)
    if (err != nil) {
        fmt.Println("Unable to parse JSON http resp")
        fmt.Println(err)
        os.Exit(1)
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
        fmt.Println("There was an error")
        fmt.Println(err)
    }

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
    
    i := 0
    for {
        i += 1
        recs, err := dataCsvReader.Read()
        
        if (err == os.EOF) {
            break
        } else if (err != nil) {
            fmt.Printf("Error reading data file %s at line %d\n", dataFile, i)
            fmt.Println(err)
            os.Exit(1)
        }

        mailer.processOneRecord(recs[0], recs[1])
    }
}
