package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// measurements.txt Oklahoma City;13.6
type Record struct {
	City        string
	Temperature float64
}

type CityTemp struct {
	City  string
	Min   float64
	Max   float64
	Avg   float64
	Count int
}

var (
	cityTempMap = make(map[string]CityTemp)
	mutex       sync.Mutex
)

func main() {
	// Record the start time
	start := time.Now()
	input, err := os.Open("D:\\Go\\1brc\\measurements.txt")

	if err != nil {
		panic(err)
	}
	defer input.Close()

	fileScanner := bufio.NewScanner(input)

	fileScanner.Split(bufio.ScanLines)
	pipeline(fileScanner)
	// Record the end time
	end := time.Now()
	duration := end.Sub(start)

	// Print the duration
	fmt.Printf("Execution time: %v\n", duration)

}
func pipelineStage[IN any, OUT any](input <-chan IN, output chan<- OUT, process func(IN) OUT) {
	defer close(output)
	for data := range input {
		output <- process(data)
	}
}

func parse(data string) Record {

	rec, err := newRecord(data)
	if err != nil {
		panic(err)
	}
	return rec

}

func compute(rec Record) Record {
	mutex.Lock()
	defer mutex.Unlock()
	if val, ok := cityTempMap[rec.City]; ok {
		if rec.Temperature < val.Min {
			val.Min = rec.Temperature
		} else if rec.Temperature > val.Max {
			val.Max = rec.Temperature
		}
		val.Count++
		val.Avg = ((val.Avg * float64(val.Count-1)) + rec.Temperature) / float64(val.Count)
		cityTempMap[rec.City] = val
	} else {
		cityTempMap[rec.City] = CityTemp{rec.City, rec.Temperature, rec.Temperature, rec.Temperature, 1}
	}
	return rec
}

func encode(rec Record) []byte {
	mutex.Lock()
	defer mutex.Unlock()

	keys := make([]string, 0, len(cityTempMap))
	for k := range cityTempMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	result := "{"
	for i, k := range keys {
		temp := cityTempMap[k]
		if i > 0 {
			result += ","
		}
		result += fmt.Sprintf("%s:%.2f/%.2f/%.2f", temp.City, temp.Min, temp.Avg, temp.Max)
	}
	result += "}"
	return []byte(result)
}

func newRecord(in string) (rec Record, err error) {

	rec.City = strings.Split(in, ";")[0]
	rec.Temperature, err = strconv.ParseFloat(strings.Split(in, ";")[1], 64)
	return rec, err

}

func pipeline(fileScanner *bufio.Scanner) {

	fmt.Println("----Starting Pipeline ----")
	parseInputCh := make(chan string, 1000)
	computeCh := make(chan Record, 1000)
	encodeInput := make(chan Record, 1000)
	// We read the output of the pipeline from this channel
	outputCh := make(chan []byte, 1000)
	// We need this channel to wait for the printing of
	// the final result
	done := make(chan struct{})
	//start go routine to parse input

	go pipelineStage(parseInputCh, computeCh, parse)
	go pipelineStage(computeCh, encodeInput, compute)
	go pipelineStage(encodeInput, outputCh, encode)

	//start go rotuine to read from pipeline output

	//start go rotuine to read from pipeline output
	go func() {
		for data := range outputCh {
			fmt.Println(string(data))
		}
		close(done)
	}()

	for fileScanner.Scan() {

		parseInputCh <- fileScanner.Text()

	}
	// Wait until the last output is printed
	close(parseInputCh)
	<-done

}
