package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/mammothbane/wikite_go"
)

const maxOpenFiles = 2048

var empty = struct{}{}

func handle(e error, msg string) {
	if e != nil {
		log.Fatalln(msg, e)
	}
}

func main() {
	refidx, err := os.OpenFile("refidx.json", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	handle(err, "opening refidx.json")
	defer refidx.Close()

	idx, err := os.Open("index.txt")
	handle(err, "opening index")
	defer idx.Close()

	refidx.WriteString("[\n\t")

	sentChan := make(chan wikite.ReferenceRecord)

	go func() {
		defer close(sentChan)

		sem := make(chan struct{}, maxOpenFiles)
		scanner := bufio.NewScanner(idx)
		for scanner.Scan() {
			file := scanner.Text()

			sem <- empty
			go func() {
				defer func() {
					<-sem
				}()

				processFile("out/"+file, sentChan)
			}()
		}

		for i := 0; i < maxOpenFiles; i++ {
			sem <- empty
		}
	}()

	count := 0
	start := true
	for rec := range sentChan {
		if !start {
			_, err := refidx.WriteString(",\n\t")
			handle(err, "writing comma-newline")
		}
		start = false

		b, err := json.MarshalIndent(rec, "\t", "\t")
		handle(err, "marshaling record")

		_, err = refidx.Write(b)
		handle(err, "writing record to file")

		count++
	}

	_, err = refidx.WriteString("\n]")
	handle(err, "writing final newline-bracket")

	log.Printf("wrote %v records", count)
}

func processFile(filename string, out chan<- wikite.ReferenceRecord) {
	f, err := os.Open(filename)
	handle(err, "reading file")
	defer f.Close()

	var rec wikite.ArticleRecord
	handle(json.NewDecoder(f).Decode(&rec), "reading json file")

	for _, sent := range rec.Sentences {
		for _, link := range sent.Links {
			mBytes := md5.Sum([]byte(link))
			hexDigest := hex.EncodeToString(mBytes[:])

			refFileName := fmt.Sprintf("refdata/%v.txt", hexDigest)

			if info, err := os.Stat(refFileName); os.IsNotExist(err) || info.Size() == 0 {
				continue
			} else if err != nil {
				handle(err, "statting file")
			}

			out <- wikite.ReferenceRecord{
				ArticleId: rec.Id,
				Text:      sent.Text,
				Reference: refFileName,
			}
		}
	}
}
