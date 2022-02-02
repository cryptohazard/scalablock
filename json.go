package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Block struct
type Block struct {
	BlockNumber int
	BlockHash   string
	TxNumber    int
	Timestamp   int
	Date        string
}

func main() {

	//JsonToCSV("./data/json/", "./data/rawcsv/")
	//JsonToComputedCSV("./data/json/", "./data/csv/")
	//ComputeForks("./data/csv/", "./data/computed/")
	Final("./data/computed/", "./data/csv/", "./data/final/")

}

func Final(computefForkDirectory, otherdatadirectory, destinationDirectory string) {
	files, err := ioutil.ReadDir(computefForkDirectory)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		if strings.HasPrefix(f.Name(), "resultsom") && strings.HasSuffix(f.Name(), "-forkscount-height.csv") {
			forkname := computefForkDirectory + f.Name()

			fmt.Println(forkname)
			if _, err := os.Stat(forkname); errors.Is(err, os.ErrNotExist) {
				fmt.Println(err)
			}
			propagname := strings.TrimSuffix(f.Name(), "-forkscount-height.csv")
			propagname = otherdatadirectory + propagname + "-all-height.csv"
			fmt.Println(propagname)
			if _, err := os.Stat(propagname); errors.Is(err, os.ErrNotExist) {
				fmt.Println(err)
			}
			xplorername := strings.TrimSuffix(f.Name(), "-forkscount-height.csv")
			xplorername = otherdatadirectory + xplorername + ".csv"
			fmt.Println(xplorername)
			if _, err := os.Stat(xplorername); errors.Is(err, os.ErrNotExist) {
				fmt.Println(err)
			}

			name := destinationDirectory + "final" + strings.TrimSuffix(f.Name(), "-forkscount-height.csv") + ".csv"

			fmt.Println("=====> " + name)

			csvFile, err := os.Create(name)

			if err != nil {
				fmt.Println(err)
			}
			defer csvFile.Close()

			writer := csv.NewWriter(csvFile)
			header := []string{"BlockNumber", "TxNumber", "Min total tx", "Max total tx",
				"Forkscount", "BlockTime", "5th percentile of propagation(ms)", "25th percentile of propagation(ms)",
				"50th percentile of propagation(ms)", "75th percentile of propagation(ms)", "95th percentile of propagation(ms)"}
			writer.Write(header)
			file, err := os.Open(forkname)
			if err != nil {
				fmt.Println(err)
			}
			fork := csv.NewReader(file)
			fork.Read()
			file, err = os.Open(propagname)
			if err != nil {
				fmt.Println(err)
			}
			propag := csv.NewReader(file)
			propag.Read()
			file, err = os.Open(xplorername)
			if err != nil {
				fmt.Println(err)
			}
			xplorer := csv.NewReader(file)
			xplorer.Read()
			counter := 0
			for {
				f, err1 := fork.Read()
				// bad but there should not be any issue if I did my job properly before
				p, err2 := propag.Read()
				x, err3 := xplorer.Read()
				if err1 == io.EOF || err2 == io.EOF || err3 == io.EOF {
					fmt.Println(len(p), " ", counter)
					break
				}
				if err1 != nil {
					log.Fatal(err)
				}
				writer.Write(append([]string{x[0], x[1], p[2], p[1], f[1], x[2]}, p[3:8]...))
				counter++

			}

			writer.Flush()

		}

	}

}

func ComputeForks(inputDirectory, outputDirectory string) {
	files, err := ioutil.ReadDir(inputDirectory)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), "forks-height.csv") {
			fmt.Println(f.Name())

			file, err := os.Open(inputDirectory + f.Name())
			if err != nil {
				fmt.Println(err)
			}
			name := strings.TrimSuffix(f.Name(), "forks-height.csv")
			csvFile, err := os.Create(outputDirectory + name + "forkscount-height.csv")
			writer := csv.NewWriter(csvFile)
			header := []string{"height", "forkscount"}
			writer.Write(header)
			if err != nil {
				fmt.Println(err)
			}
			defer csvFile.Close()
			r := csv.NewReader(file)
			height := 0
			counter := -1
			r.Read()

			for {
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				csvHeight, err := strconv.Atoi(record[0])
				if err != nil {
					log.Fatal(err)
				}
				if height == csvHeight {
					counter++
					//fmt.Println(record[0], " ", counter, " ", height)
				} else {
					//fmt.Println("Print ", height, " ", counter)
					writer.Write([]string{strconv.Itoa(height), strconv.Itoa(counter)})
					counter = 0
					height++
				}

			}
			//fmt.Println("Print ", height, " ", counter)
			writer.Write([]string{strconv.Itoa(height), strconv.Itoa(counter)})
			writer.Flush()
		}
	}

}
func JsonToComputedCSV(inputDirectory, outputDirectory string) {
	files, err := ioutil.ReadDir(inputDirectory)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			fmt.Println(file.Name())
			name := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))

			csvFile, err := os.Create(outputDirectory + name + ".csv")

			if err != nil {
				fmt.Println(err)
			}
			defer csvFile.Close()

			writer := csv.NewWriter(csvFile)
			header := []string{"BlockNumber", "TxNumber", "BlockTime"}
			writer.Write(header)
			// read data from file
			jsonDataFromFile, err := ioutil.ReadFile(inputDirectory + file.Name())

			if err != nil {
				fmt.Println(err)
			}

			// Unmarshal JSON data
			var jsonData []Block
			err = json.Unmarshal([]byte(jsonDataFromFile), &jsonData)

			if err != nil {
				fmt.Println(err)
			}
			previousTimestamp := jsonData[0].Timestamp

			for _, block := range jsonData {
				var row []string
				row = append(row, strconv.Itoa(block.BlockNumber))
				row = append(row, strconv.Itoa(block.TxNumber))
				row = append(row, strconv.Itoa(block.Timestamp-previousTimestamp))
				writer.Write(row)
				previousTimestamp = block.Timestamp
			}

			// remember to flush!
			writer.Flush()

		}
	}
}
func JsonToCSV(inputDirectory string, outputDirectory string) {
	files, err := ioutil.ReadDir(inputDirectory)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			fmt.Println(file.Name())
			name := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))

			csvFile, err := os.Create(outputDirectory + name + ".csv")

			if err != nil {
				fmt.Println(err)
			}
			defer csvFile.Close()

			writer := csv.NewWriter(csvFile)
			header := []string{"BlockNumber", "BlockHash", "TxNumber", "Timestamp", "Date"}
			writer.Write(header)
			// read data from file
			jsonDataFromFile, err := ioutil.ReadFile(inputDirectory + file.Name())

			if err != nil {
				fmt.Println(err)
			}

			// Unmarshal JSON data
			var jsonData []Block
			err = json.Unmarshal([]byte(jsonDataFromFile), &jsonData)

			if err != nil {
				fmt.Println(err)
			}

			for _, block := range jsonData {
				var row []string
				row = append(row, strconv.Itoa(block.BlockNumber))
				row = append(row, block.BlockHash)
				row = append(row, strconv.Itoa(block.TxNumber))
				row = append(row, strconv.Itoa(block.Timestamp))
				row = append(row, block.Date)
				writer.Write(row)
			}

			// remember to flush!
			writer.Flush()

		}
	}
}
