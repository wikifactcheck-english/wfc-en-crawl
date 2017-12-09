package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"gopkg.in/fatih/set.v0"
)

func handle(e error, msg string) {
	if e != nil {
		log.Fatalln(msg, e)
	}
}

const (
	maxCvtProcs  = 50
	maxOpenFiles = 128
)

var (
	badSet   = set.New()
	setMutex sync.Mutex
	ctr      int64 = 0

	empty   = struct{}{}
	procSem = make(chan struct{}, maxCvtProcs)
	fileSem = make(chan struct{}, maxOpenFiles)

	pdfBinary string
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

		reader := bufio.NewReader(file)
		for {
			elt, err := reader.ReadString('\n')
			if err == io.EOF {
				badSet.Add(elt)
				break
			}
			handle(err, "reading bad.txt")

			badSet.Add(elt)
		}
		file.Close()
	}

	index, err := os.Open("index.txt")
	handle(err, "reading index.txt")

	indexScanner := bufio.NewScanner(index)

	wg := &sync.WaitGroup{}
	for indexScanner.Scan() {
		name := indexScanner.Text()

		wg.Add(1)

		go func(name string) {
			downloadRefs(name)

			result := atomic.AddInt64(&ctr, 1)

			if result%10000 == 0 {
				setMutex.Lock()
				defer setMutex.Unlock()

				var file *os.File

				if _, err := os.Stat("bad.txt"); err == nil {
					file, err = os.Open("bad.txt")
					handle(err, "overwriting bad.txt")
				} else {
					file, err = os.Create("bad.txt")
					handle(err, "creating bad.txt")
				}

				defer func() {
					handle(file.Close(), "closing bad.txt")
				}()

				for _, v := range set.StringSlice(badSet) {
					file.WriteString(v + "\n")
				}
			}

			wg.Done()
		}(name)
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
	fileSem <- empty
	f, err := os.Open("out/" + filename)
	handle(err, "opening article json file")

	var article ArticleRecord
	handle(json.NewDecoder(f).Decode(&article), "reading article")
	handle(f.Close(), "closing ref_data file")
	<-fileSem

	log.Println("downloading refs for", filename)

	wg := &sync.WaitGroup{}
	for _, sent := range article.Sentences {
		for _, link := range sent.Links {
			wg.Add(1)
			go func(link string) {
				retrieveRef(link)
				wg.Done()
			}(link)
		}
	}
	wg.Wait()
}

func retrieveRef(link string) {
	client := http.Client{}

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
	if err != nil {
		return
	}

	if !checkResp(resp) {
		resp.Body.Close()
		return
	}

	fileSem <- empty
	tmp, err := ioutil.TempFile("", "pdf_dl")
	handle(err, "creating tmp file")
	defer func() {
		handle(tmp.Close(), "closing tmp file")
		<-fileSem
		handle(os.Remove(tmp.Name()), "destroying tmp file")
	}()

	if _, err = io.Copy(tmp, resp.Body); err != nil {
		log.Printf("writing content to tmp file: %q", err)
		badSet.Add(hexDigest)
		return
	}

	fileSem <- empty
	procSem <- empty
	defer func() {
		<-procSem
		<-fileSem
	}()

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
