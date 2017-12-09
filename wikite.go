package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pdfcontent "github.com/unidoc/unidoc/pdf/contentstream"
	pdf "github.com/unidoc/unidoc/pdf/model"
	"gopkg.in/fatih/set.v0"
)

func handle(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

var (
	badSet   = set.New()
	setMutex sync.Mutex
	ctr      int64 = 0

	client = http.Client{
		Timeout: 750 * time.Millisecond,
	}
)

func main() {
	if _, err := os.Stat("refdata"); os.IsNotExist(err) {
		os.Mkdir("refdata", 0700)
	}

	if _, err := os.Stat("bad.txt"); err == nil {
		file, err := os.Open("bad.txt")
		handle(err)

		reader := bufio.NewReader(file)
		for {
			elt, err := reader.ReadString('\n')
			if err == io.EOF {
				break
			}
			handle(err)

			badSet.Add(elt)
		}
		file.Close()
	}

	outDir, err := os.Open("out")
	handle(err)

	log.Println("reading directory")

	names, err := outDir.Readdirnames(-1)
	handle(err)

	outDir.Close()
	log.Println("done reading directory")

	var wg sync.WaitGroup
	for _, name := range names {
		wg.Add(1)

		go func() {
			downloadRefs(name)

			result := atomic.AddInt64(&ctr, 1)

			if result%10000 == 0 {
				setMutex.Lock()
				defer setMutex.Unlock()

				var file *os.File

				if _, err := os.Stat("bad.txt"); err == nil {
					file, err = os.Open("bad.txt")
					handle(err)
				} else {
					file, err = os.Create("bad.txt")
					handle(err)
				}

				for _, v := range set.StringSlice(badSet) {
					file.WriteString(v + "\n")
				}
			}

			wg.Done()
		}()
	}
	wg.Wait()
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
	handle(err)

	var article ArticleRecord
	handle(json.NewDecoder(f).Decode(&article))
	f.Close()

	for _, sent := range article.Sentences {
		for _, link := range sent.Links {
			retrieveRef(link)
		}
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
	if err != nil || !checkResp(resp) {
		badSet.Add(hexDigest)
		return
	}

	log.Println("downloading ", link)

	// actually retrieve file
	resp, err = client.Get(link)
	if err != nil || !checkResp(resp) {
		return
	}

	b, err := ioutil.ReadAll(resp.Body)
	handle(err)

	pdfReader, err := pdf.NewPdfReader(bytes.NewReader(b))
	if enc, err := pdfReader.IsEncrypted(); err != nil || enc {
		badSet.Add(hexDigest)
		return
	}

	pdfPages, err := pdfReader.GetNumPages()
	handle(err)

	f, err := os.Create(targetFile)
	handle(err)
	defer f.Close()

	for i := 0; i < pdfPages; i++ {
		page, err := pdfReader.GetPage(i + 1)
		handle(err)

		cstreams, err := page.GetAllContentStreams()
		handle(err)

		//preParse := ""
		//for i := range cstreams {
		//	preParse += cstreams[i]
		//}

		parser := pdfcontent.NewContentStreamParser(cstreams)

		text, err := parser.ExtractText()
		handle(err)

		f.WriteString(text)
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

	/*
		contentLength, err := strconv.Atoi(r.Header.Get("content-length"))
		handle(err)

		if contentLength > 50 * 1000 * 1000 {
			return false
		}
	*/

	return true
}
