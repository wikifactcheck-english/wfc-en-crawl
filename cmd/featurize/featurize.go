package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"

	"github.com/bbalet/stopwords"
	"github.com/jdkato/prose/tokenize"
	"github.com/mammothbane/wikite_go"
	"golang.org/x/text/unicode/norm"
	"gopkg.in/fatih/set.v0"
)

var (
	pos           = flag.Bool("pos", false, "generate positive cases")
	neg           = flag.Bool("neg", false, "generate negative cases")
	negProp       = flag.Float64("negProp", 1.0, "proportion of negative cases to generate")
	wordTokenizer = tokenize.NewWordPunctTokenizer()
	sentTokenizer = tokenize.NewPunktSentenceTokenizer()
)

const (
	Label uint = iota
	DocCos1
	DocCos2
	SentCos1
	SentCos2
	SentCos3

	featureSize = 6
)

func handle(e error, msg string) {
	if e != nil {
		log.Fatalln(msg, e)
	}
}

func main() {
	flag.Parse()

	prof, err := os.Create("feat.pprof")
	handle(err, "creating profile file")
	handle(pprof.StartCPUProfile(prof), "couldn't start cpu profile")
	defer pprof.StopCPUProfile()
	//defer prof.Close()

	f, err := os.Open("refidx.json")
	handle(err, "opening refidx")

	var sents []wikite.ReferenceRecord
	handle(json.NewDecoder(f).Decode(&sents), "decoding json")
	f.Close()

	featureChan := make(chan [featureSize]float64)
	var wg sync.WaitGroup

	doFeaturize := func(text string, ref string, label int) {
		defer wg.Done()
		featurize(text, ref, label, featureChan)
	}

	if *pos {
		for _, sent := range sents {
			wg.Add(1)
			go doFeaturize(sent.Text, sent.Reference, 1)
		}
	}

	if *neg {
		for i := 0; i < int(*negProp*float64(len(sents))); i++ {
			caseA := sents[rand.Intn(len(sents))]
			if caseA.Text == "" {
				continue
			}

			caseB := sents[rand.Intn(len(sents))]
			for caseB.Text == "" || caseB.Text == caseA.Text || caseA.Reference == caseB.Reference {
				caseB = sents[rand.Intn(len(sents))]
			}

			wg.Add(1)
			go doFeaturize(caseA.Text, caseB.Reference, 0)
		}
	}

	go func() {
		wg.Wait()
		close(featureChan)
	}()

	if _, err := os.Stat("train-data"); os.IsNotExist(err) {
		handle(os.Mkdir("train-data", 0744), "creating train-data")
	} else {
		handle(err, "statting train-data")
	}

	f, err = os.Create("train-data/data.txt")
	handle(err, "creating output file")
	defer f.Close()

	for feature := range featureChan {
		str := ""

		for _, v := range feature {
			str += strconv.FormatFloat(v, 'g', -1, 64) + " "
		}

		_, err = f.WriteString(str[:len(str)-1] + "\n")
		handle(err, "writing to file")
	}

}

// specialized for binary occurrence on sets
func cosineSim(a *set.Set, b *set.Set) float64 {
	denom := math.Sqrt(float64(a.Size())) * math.Sqrt(float64(b.Size()))

	if denom == 0 {
		return 0
	}

	return float64(set.Intersection(a, b).Size()) / denom
}

func featurize(text string, refFile string, label int, featureChan chan<- [featureSize]float64) {
	words := wordTokenizer.Tokenize(stopwords.CleanString(text, "en", false))

	textNgrams := [...]*set.Set{set.New(), set.New(), set.New()}

	for i := 1; i <= 3; i++ {
		for j := 0; j < len(words)-i+1; j++ {
			textNgrams[i-1].Add(strings.Join(words[j:j+i], " "))
		}
	}

	f, err := os.Open(refFile)
	handle(err, "opening reference")

	b, err := ioutil.ReadAll(norm.NFKC.Reader(f))
	handle(err, "reading reference")

	var (
		sents       = sentTokenizer.Tokenize(string(b))
		sentOverlap = [...]float64{0, 0, 0}
		refNgrams   = [...]*set.Set{set.New(), set.New(), set.New()}
		sentNgrams  = [...]*set.Set{set.New(), set.New(), set.New()}
	)

	for _, sent := range sents {
		refWords := wordTokenizer.Tokenize(sent)

		for i := 1; i <= 3; i++ {
			for j := 0; j < len(refWords)-i+1; j++ {
				sentNgrams[i-1].Add(strings.Join(refWords[j:j+i], " "))
			}
		}

		for i := range sentNgrams {
			refNgrams[i].Merge(sentNgrams[i])
			sentOverlap[i] += cosineSim(textNgrams[i], sentNgrams[i])

			sentNgrams[i].Clear()
		}
	}

	feature := [featureSize]float64{}
	feature[Label] = float64(label)
	feature[DocCos1] = cosineSim(refNgrams[0], textNgrams[0])
	feature[DocCos2] = cosineSim(refNgrams[1], textNgrams[1])

	if len(sents) > 0 {
		feature[SentCos1] = sentOverlap[0] / float64(len(sents))
		feature[SentCos2] = sentOverlap[1] / float64(len(sents))
		feature[SentCos3] = sentOverlap[2] / float64(len(sents))
	}

	featureChan <- feature
}
