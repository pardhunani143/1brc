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
	cityTempMap sync.Map
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

func pipelineStage[IN any, OUT any](input <-chan IN, output chan<- OUT, process func(IN) OUT, numWorkers int) {
	defer close(output)
	wg := sync.WaitGroup{}
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for data := range input {
				output <- process(data)
			}
		}()
	}
	wg.Wait()
}

func parse(data []string) []Record {
	records := make([]Record, len(data))
	for i, line := range data {
		rec, err := newRecord(line)
		if err != nil {
			panic(err)
		}
		records[i] = rec
	}
	return records
}

func compute(records []Record) []Record {
	for _, rec := range records {
		val, _ := cityTempMap.LoadOrStore(rec.City, CityTemp{rec.City, rec.Temperature, rec.Temperature, rec.Temperature, 1})
		cityTemp := val.(CityTemp)

		if rec.Temperature < cityTemp.Min {
			cityTemp.Min = rec.Temperature
		} else if rec.Temperature > cityTemp.Max {
			cityTemp.Max = rec.Temperature
		}
		cityTemp.Count++
		cityTemp.Avg = ((cityTemp.Avg * float64(cityTemp.Count-1)) + rec.Temperature) / float64(cityTemp.Count)
		cityTempMap.Store(rec.City, cityTemp)
	}
	return records
}

func encode() string {
	keys := make([]string, 0)
	cityTempMap.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	sort.Strings(keys)
	result := ""
	for i, k := range keys {
		val, _ := cityTempMap.Load(k)
		temp := val.(CityTemp)
		if i > 0 {
			result += ";"
		}
		result += fmt.Sprintf("%s:%.2f/%.2f/%.2f", temp.City, temp.Min, temp.Avg, temp.Max)
	}
	return result
}

func newRecord(in string) (rec Record, err error) {
	rec.City = strings.Split(in, ";")[0]
	rec.Temperature, err = strconv.ParseFloat(strings.Split(in, ";")[1], 64)
	return rec, err
}

func pipeline(fileScanner *bufio.Scanner) {
	fmt.Println("----Starting Pipeline ----")
	parseInputCh := make(chan []string, 100)
	computeCh := make(chan []Record, 100)
	done := make(chan struct{})

	go pipelineStage(parseInputCh, computeCh, parse, 30)
	go pipelineStage(computeCh, nil, compute, 30)

	// Start a goroutine to signal when processing is done
	go func() {
		for range computeCh {
			// Just drain the channel
		}
		close(done)
	}()

	// Start a goroutine to scan the file and feed the parseInputCh channel
	go func() {
		batchSize := 50
		batch := make([]string, 0, batchSize)
		for fileScanner.Scan() {
			batch = append(batch, fileScanner.Text())
			if len(batch) == batchSize {
				parseInputCh <- batch
				batch = make([]string, 0, batchSize)
			}
		}
		if len(batch) > 0 {
			parseInputCh <- batch
		}
		// Close the input channel to signal no more data
		close(parseInputCh)
	}()

	<-done

	result := encode()
	fmt.Println(result)
}
