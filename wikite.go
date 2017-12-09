package main

import (
	"bufio"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
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

var (
	badSet    = set.New()
	empty     = struct{}{}
	pdfBinary string
	transport = &http.Transport{
		MaxIdleConns:          10,
		IdleConnTimeout:       10 * time.Millisecond,
		ResponseHeaderTimeout: 500 * time.Millisecond,
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	client = &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
	done               = make(chan struct{}, 1)
	articleCount int64 = 0
)

func init() {
	if runtime.GOOS == "windows" {
		pdfBinary = "bin/pdftotext_win64.exe"
	} else if runtime.GOOS == "linux" {
		pdfBinary = "bin/pdftotext_linux64"
	} else {
		log.Panic("unsupported runtime")
	}
}

func main() {
	if _, err := os.Stat("refdata"); os.IsNotExist(err) {
		os.Mkdir("refdata", 0700)
	}

	if _, err := os.Stat("index.txt"); os.IsNotExist(err) {
		outDir, err := os.Open("out")
		handle(err, "opening out dir")

		log.Println("reading directory")

		names, err := outDir.Readdirnames(-1)
		handle(err, "reading out dir")

		outDir.Close()
		log.Println("done reading directory")

		file, err := os.Create("index.txt")
		handle(err, "creating index.txt")

		for _, v := range names {
			file.WriteString(v + "\n")
		}

		file.Close()
	}

	if _, err := os.Stat("bad.txt"); err == nil {
		file, err := os.Open("bad.txt")
		handle(err, "opening bad.txt")

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			badSet.Add(scanner.Text())
		}
		handle(file.Close(), "closing bad.txt")
	} else if os.IsNotExist(err) {
		file, err := os.Create("bad.txt")
		handle(err, "creating bad.txt")
		handle(file.Close(), "closing bad.txt")
	}

	sigChan := make(chan os.Signal, 1)

	go func() {
		<-sigChan
		close(done)
		log.Println("shutting down...")
	}()

	signal.Notify(sigChan, syscall.SIGINT)

	go func() {
		f, err := os.OpenFile("bad.txt", os.O_WRONLY|os.O_TRUNC, 0644)
		handle(err, "opening bad.txt")
		defer f.Close()

		tick := time.Tick(500 * time.Millisecond)
		for {
			select {
			case <-tick:
				log.Printf("%v articles complete", articleCount)
				for _, str := range set.StringSlice(badSet) {
					f.WriteString(str + "\n")
				}
			case <-done:
				return
			}
		}
	}()

	index, err := os.Open("index.txt")
	handle(err, "reading index.txt")

	indexScanner := bufio.NewScanner(index)

	const maxItems = 100
	sem := make(chan struct{}, maxItems)
	defer close(sem)

	for indexScanner.Scan() {
		name := indexScanner.Text()

		select {
		case _, ok := <-done:
			if !ok {
				break
			}
		case sem <- empty:
		}

		go func(name string) {
			defer func() {
				<-sem
				atomic.AddInt64(&articleCount, 1)
			}()

			downloadRefs(name)
		}(name)
	}

	for i := 0; i < maxItems; i++ {
		sem <- empty
	}
}

type (
	SentenceRecord struct {
		Links []string `json:"links"`
		Text  string   `json:"text"`
	}

	ArticleRecord struct {
		Revision  int              `json:"revision"`
		Id        int              `json:"id"`
		Sentences []SentenceRecord `json:"sentences"`
	}
)

func downloadRefs(filename string) {
	f, err := os.Open("out/" + filename)
	handle(err, "opening article json file")

	var article ArticleRecord
	handle(json.NewDecoder(f).Decode(&article), "reading article")
	handle(f.Close(), "closing ref_data file")

	const maxItems = 10
	sem := make(chan struct{}, maxItems)
	defer close(sem)

outer:
	for _, sent := range article.Sentences {
		for _, link := range sent.Links {
			select {
			case _, ok := <-done:
				if !ok {
					break outer
				}
			case sem <- empty:
			}

			go func(link string) {
				defer func() {
					<-sem
				}()

				retrieveRef(link)
			}(link)
		}
	}

	for i := 0; i < maxItems; i++ {
		sem <- empty
	}
}

func retrieveRef(link string) {
	mBytes := md5.Sum([]byte(link))
	hexDigest := hex.EncodeToString(mBytes[:])

	if badSet.Has(hexDigest) {
		return
	}

	targetFile := "refdata/" + hexDigest + ".txt"

	// file exists, don't redownload
	if _, err := os.Stat(targetFile); err == nil {
		return
	}

	// call head first to check filetype
	resp, err := client.Head(link)
	if err != nil {
		badSet.Add(hexDigest)
		return
	}

	if !checkResp(resp) {
		badSet.Add(hexDigest)
		return
	}

	// actually retrieve file
	resp, err = client.Get(link)
	if err != nil {
		return
	}

	defer resp.Body.Close()

	if !checkResp(resp) {
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
		return
	}

	c := exec.Command(pdfBinary, "-nopgbrk", "-q", tmp.Name(), targetFile)

	out, err := c.CombinedOutput()
	if err != nil {
		log.Printf("converting %v errored: %v\n%v", link, err, string(out))
		badSet.Add(hexDigest)
		return
	}

	log.Println("successfully downloaded ", link)
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
