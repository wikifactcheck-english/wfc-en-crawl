package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"gopkg.in/fatih/set.v0"
)

func handle(e error, msg string) {
	if e != nil {
		log.Fatalln(msg, e)
	}
}

type JsonLineFormat struct {
	Url      string `json:"url"`
	Filename string `json:"evidence"`
}

var (
	badSet       = set.New()
	empty        = struct{}{}
	pdfBinary    string
	done               = make(chan struct{}, 1)
	articleCount int64 = 0
	failedCount  int64 = 0
	badFile      string
	inputFile    string
	transport    = &http.Transport{
		MaxIdleConns:          10,
		IdleConnTimeout:       10 * time.Millisecond,
		ResponseHeaderTimeout: 500 * time.Millisecond,
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	client = &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
)

func init() {
	if runtime.GOOS == "windows" {
		pdfBinary = "bin/pdftotext_win64.exe"
	} else if runtime.GOOS == "linux" {
		pdfBinary = "bin/pdftotext_linux64"
	} else {
		log.Panic("unsupported runtime")
	}

	flag.StringVar(&badFile, "badFile", "bad.txt", "file to store bad links in")
	flag.StringVar(&inputFile, "inputFile", "input.jsonl", "input file")
	flag.Parse()
}

func main() {
	if _, err := os.Stat("evidence"); os.IsNotExist(err) {
		os.Mkdir("evidence", 0700)
	}

	if _, err := os.Stat(badFile); err == nil {
		file, err := os.Open(badFile)
		handle(err, "opening badfile")

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			badSet.Add(scanner.Text())
		}
		handle(file.Close(), "closing badfile")
	} else if os.IsNotExist(err) {
		file, err := os.Create(badFile)
		handle(err, "creating badfile")
		handle(file.Close(), "closing badfile")
	}

	sigChan := make(chan os.Signal, 1)

	go func() {
		<-sigChan
		close(done)
		log.Println("shutting down...")
	}()

	signal.Notify(sigChan, syscall.SIGINT)

	go func() {
		f, err := os.OpenFile(badFile, os.O_WRONLY|os.O_TRUNC, 0644)
		handle(err, "opening badfile")
		defer f.Close()

		tick := time.Tick(500 * time.Millisecond)
		for {
			select {
			case <-tick:
				f.Seek(0, 0)
				log.Printf("%v articles complete", articleCount)
				for _, str := range set.StringSlice(badSet) {
					f.WriteString(str + "\n")
				}
			case <-done:
				return
			}
		}
	}()

	const maxItems = 100
	sem := make(chan struct{}, maxItems)
	defer close(sem)

	input, err := os.Open(inputFile)
	handle(err, "opening input file")

	inputScanner := bufio.NewScanner(input)

	log.Println("starting downloads")
	for inputScanner.Scan() {
		var jsonl JsonLineFormat
		err = json.NewDecoder(bytes.NewBuffer(inputScanner.Bytes())).Decode(&jsonl)
		handle(err, "decoding line")

		select {
		case _, ok := <-done:
			if !ok {
				break
			}
		case sem <- empty:
		}

		go func(jsonl JsonLineFormat) {
			defer func() {
				<-sem
				atomic.AddInt64(&articleCount, 1)
			}()

			retrieveRef(jsonl)
		}(jsonl)
	}

	for i := 0; i < maxItems; i++ {
		sem <- empty
	}

	log.Println("successfully downloaded", articleCount-failedCount, "articles out of", articleCount, "total (", failedCount, "failed)")
}

func retrieveRef(jsonl JsonLineFormat) {
	hexDigest := strings.TrimSuffix(jsonl.Filename, ".txt")

	mBytes := md5.Sum([]byte(jsonl.Url))
	actualDigest := hex.EncodeToString(mBytes[:])

	if actualDigest != hexDigest {
		log.Printf("WARN: mismatched url (%q) and evidence filename (%q). naming file according to evidence field", jsonl.Url, jsonl.Filename)
	}

	if badSet.Has(hexDigest) {
		atomic.AddInt64(&failedCount, 1)
		return
	}

	targetFile := "evidence/" + hexDigest + ".txt"

	// file exists, don't redownload
	if _, err := os.Stat(targetFile); err == nil {
		return
	}

	// call head first to check filetype
	resp, err := client.Head(jsonl.Url)
	if err != nil {
		badSet.Add(hexDigest)
		atomic.AddInt64(&failedCount, 1)
		return
	}

	if !checkResp(resp) {
		badSet.Add(hexDigest)
		atomic.AddInt64(&failedCount, 1)
		return
	}

	// actually retrieve file
	resp, err = client.Get(jsonl.Url)
	if err != nil {
		atomic.AddInt64(&failedCount, 1)
		return
	}

	defer resp.Body.Close()

	if !checkResp(resp) {
		atomic.AddInt64(&failedCount, 1)
		_, err := ioutil.ReadAll(resp.Body)
		handle(err, "consuming body")
		resp.Body.Close()
		return
	}

	tmp, err := ioutil.TempFile("", "pdf_dl")
	handle(err, "creating tmp file")
	defer func() {
		handle(tmp.Close(), "closing tmp file")
		handle(os.Remove(tmp.Name()), "destroying tmp file")
	}()

	_, err = io.Copy(tmp, resp.Body)

	if err != nil {
		log.Printf("writing content to tmp file: %q", err)
		badSet.Add(hexDigest)
		atomic.AddInt64(&failedCount, 1)
		return
	}

	c := exec.Command(pdfBinary, "-nopgbrk", "-q", tmp.Name(), targetFile)

	out, err := c.CombinedOutput()
	if err != nil {
		log.Printf("converting %v errored: %v\n%v", jsonl.Url, err, string(out))
		badSet.Add(hexDigest)
		atomic.AddInt64(&failedCount, 1)
		return
	}

	log.Println("successfully downloaded ", jsonl.Url)
}

func checkResp(r *http.Response) bool {
	if r.StatusCode >= 299 || r.StatusCode < 200 {
		return false
	}

	contentType := strings.ToLower(r.Header.Get("content-type"))
	if contentType != "x-pdf" && contentType != "application/pdf" {
		return false
	}

	clString := r.Header.Get("content-length")

	if len(clString) > 0 {
		contentLength, err := strconv.Atoi(clString)
		handle(err, "content-length invalid")

		// an upper limit
		if contentLength > 100*1000*1000 {
			return false
		}
	}

	return true
}
