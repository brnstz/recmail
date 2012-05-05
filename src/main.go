package main

import (
    "flag"
    "fmt"
    "encoding/json"
    "io/ioutil"
    "encoding/csv"
    "os"
    "text/template"
    "bytes"
    "io"
    "net/smtp"
    "net/http"
    "time"
    "strconv"
)

const (
    numRoutines = 20
    recsChanBuff = 1000
    respChanBuff = 500
)

type RecConfig struct {
    RecUrl     string
    UserUrl     string
    SmtpServer string
    SmtpFrom   string
    EnvelopeFrom   string
}

type RecMailer struct {
    Config   RecConfig
    Template *template.Template
    Http     *http.Client
    DataFile string
}

type EmailData struct {
    FromAddress string
    ToAddress   string
    Subject     string
    Date        string
    RecResponse RecResponse
    HumanDate   string 
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

type CrackResp struct {
    Status int
    Result map[string]interface{}
}

type SendResp struct {
    NumSent int
    Seconds int64
}

func (mailer *RecMailer) launchProcessor(recsChan chan []string, respChan chan int, w io.Writer) {

    tr := &http.Transport{
            DisableKeepAlives: false,
            DisableCompression: false,
    }
    httpClient := &http.Client{Transport: tr}

    smtpClient, _ := smtp.Dial(mailer.Config.SmtpServer)
    for {
        rec := <- recsChan
        resp := mailer.processOneRecord(rec[0], rec[1], w, smtpClient, httpClient)

        respChan <- resp
    }
}

func (mailer *RecMailer) processOneRecord(id string, email string, w io.Writer, smtpClient *smtp.Client, httpClient *http.Client) int {
    var (
        recResponse RecResponse
    )

    fullUrl := fmt.Sprintf(mailer.Config.RecUrl, id)
    resp, err := httpClient.Get(fullUrl)

    if err != nil {
        fmt.Fprintf(w, "Unable to get URL %s\n", fullUrl)
        fmt.Println(err)
        resp.Body.Close()
        return 1
    }
    
   
    readBytes, err := ioutil.ReadAll(resp.Body)
    resp.Body.Close()
    if err != nil {
        fmt.Fprintf(w, "Unable to read from URL %s\n", fullUrl)
        return 1
    }

    err = json.Unmarshal(readBytes, &recResponse)
    if (err != nil) {
        fmt.Fprintf(w, "Unable to parse JSON http resp for user %s\n", id)
        fmt.Println(err)
        return 1
    }

    if len(recResponse.Suggestions) == 0 {
        fmt.Fprintf(w, "No recommendations for user %s\n", id)
        return 1
    }

    // If there's no thumbnail, use a blank gif
    for recIndex := 0; recIndex < len(recResponse.Suggestions); recIndex++ {
        if len(recResponse.Suggestions[recIndex].Thumbnail.Url) == 0 {
            recResponse.Suggestions[recIndex].Thumbnail.Url = "http://graphics8.nytimes.com/images/misc/spacer.gif"
        }
    }

    //emails := make([]string, 1)
    //emails[0] = email

    localTime := time.Now()
    dateStr := localTime.Format(time.RFC1123Z)
    edata := new(EmailData)
    edata.FromAddress = mailer.Config.EnvelopeFrom
    edata.ToAddress   = email
    edata.Subject     = "Recommendations for you"
    edata.RecResponse = recResponse
    edata.Date        = dateStr
    edata.HumanDate   = fmt.Sprintf("%s %d, %d", localTime.Month(), 
        localTime.Day(), localTime.Year())

    buff := new(bytes.Buffer)
    mailer.Template.Execute(buff, edata)


    //err = smtp.SendMail(mailer.Config.SmtpServer, nil, mailer.Config.SmtpFrom, emails, buff.Bytes())

    errReset := smtpClient.Reset()
    if errReset != nil {
        fmt.Printf("There was an error sending for user %s\n", id)
        fmt.Println(errReset)
        return 1
    }
    errMail := smtpClient.Mail(mailer.Config.SmtpFrom)
    if errMail != nil {
        fmt.Printf("There was an error sending for user %s\n", id)
        fmt.Println(errMail)
        return 1
    }
    errRcpt := smtpClient.Rcpt(email)
    if errRcpt != nil {
        fmt.Printf("There was an error sending for user %s\n", id)
        fmt.Println(errRcpt)
        return 1
    }
    smtpWriter, errData := smtpClient.Data()
    if errData != nil {
        fmt.Printf("There was an error sending for user %s\n", id)
        fmt.Println(errData)
        return 1
    }
    smtpWriter.Write(buff.Bytes())
    smtpWriter.Close()

    /*
    if err != nil {
        fmt.Printf("There was an error sending for user %s\n", id)
        fmt.Println(err)
        return 1
    }
    */
    
    fmt.Fprintf(w, "Success for %s, %s\n", id, email)
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
            allRequestsDoneChan <- finishedRequests
            break
        }
    }
}

func startMailing(mailer *RecMailer, email string, uid int, w io.Writer) (SendResp) {
    
    recsChan := make(chan []string, recsChanBuff)
    respChan := make(chan int, respChanBuff)
    doneReadingChan := make(chan int)
    allRequestsDoneChan := make(chan int)
  
    start := time.Now().Unix()
    for i := 0; i < numRoutines; i++ {
        go mailer.launchProcessor(recsChan, respChan, w)
    }

    go readDataFile(mailer.DataFile, recsChan, doneReadingChan)

    go readResults(respChan, allRequestsDoneChan, doneReadingChan)

    // Wait until everything is done
    numSent := <-allRequestsDoneChan
   
    tr := &http.Transport{
            DisableKeepAlives: false,
            DisableCompression: false,
    }
    httpClient := &http.Client{Transport: tr}

    smtpClient, _ := smtp.Dial(mailer.Config.SmtpServer)
    mailer.processOneRecord(strconv.Itoa(uid), email, w, smtpClient, httpClient)
    end := time.Now().Unix()

    return SendResp{NumSent: numSent + 1, Seconds: end - start}
}

func getUserInfo(config *RecConfig, nyts string) (*CrackResp, error) {

    // Create crack request json
    crackRequest := map[string]string{ "nyts": nyts, "caller_id": "1234" }
    b, encodeErr := json.Marshal(crackRequest)
    if encodeErr != nil {
        return nil, encodeErr
    }

    // Read results of crack
    bReader := bytes.NewReader(b)
    resp, httpErr := http.Post(config.UserUrl, "application/json", bReader)
    if httpErr != nil {
        return nil, httpErr
    }

    readBytes, readErr := ioutil.ReadAll(resp.Body)
    if readErr != nil {
        return nil, readErr
    }


    // Parse info out
    var userInfo CrackResp
    decodeErr := json.Unmarshal(readBytes, &userInfo)
    if decodeErr != nil {
        return nil, decodeErr
    }

    return &userInfo, nil
}

func userInfoHandler(w http.ResponseWriter, r *http.Request, mailer *RecMailer) {
    nyts, err := r.Cookie("NYT-S")
    config := mailer.Config
   
    errorResult := map[string]interface{} { "email": "Not logged in"}
    errorResp := CrackResp{Status:500, Result: errorResult}
    errorJson, _ := json.Marshal(errorResp)

    if err != nil {
        w.Write(errorJson)
    } else {
        userInfo, err := getUserInfo(&config, nyts.Value)
        if err != nil {
            w.Write(errorJson)
        } else {
            successJson, _ := json.Marshal(userInfo)
            w.Write(successJson)
        }
    }
}

func sendHandler(w http.ResponseWriter, r *http.Request, mailer *RecMailer) {
    nyts, _ := r.Cookie("NYT-S")
    userInfo, _ := getUserInfo(&mailer.Config, nyts.Value)

    email := userInfo.Result["email"].(string)
    uid := int(userInfo.Result["_UID"].(float64))

    sendResp := startMailing(mailer, email, uid, w)
    sendJson, _ := json.Marshal(sendResp)
    w.Write(sendJson)
}


func viewHandler(w http.ResponseWriter, r *http.Request, mailer *RecMailer) {
    t,_ := template.ParseFiles("templates/index.html")

    t.Execute(w, nil)
}

func makeHandler(fn func(http.ResponseWriter, *http.Request, *RecMailer), mailer *RecMailer) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        fn(w, r, mailer)
    }
}

func main() {
    recConfig, dataFile, t := parseArgs()
    client := new(http.Client)
    mailer := new(RecMailer)

    fmt.Println("Using data file: ", dataFile)

    mailer.Config   = recConfig
    mailer.Template = t
    mailer.Http     = client
    mailer.DataFile = dataFile

    //cpuWriter, _ := os.Create("myprof.txt")
    //pprof.StartCPUProfile(cpuWriter)
    //startMailing(mailer, "bseitznyt@gmail.com", 60225968, os.NewFile(uintptr(syscall.Stdout), "/dev/stdout"))
    //pprof.StopCPUProfile()
    
    http.HandleFunc("/", makeHandler(viewHandler, mailer))
    http.HandleFunc("/user", makeHandler(userInfoHandler, mailer))
    http.HandleFunc("/send", makeHandler(sendHandler, mailer))
    http.ListenAndServe(":8080", nil)
}
