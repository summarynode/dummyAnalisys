package main

import (
	"fmt"
	"log"
	"os"
	"bufio"
	"strings"
	"strconv"
	"math"
	"sort"
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"io/ioutil"
)

type Pair struct {
	Key string
	Value float64
}
type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Less(i, j int) bool { return p[i].Value > p[j].Value }


func main()  {

	tmp := ""
	code := ""
	var totalPrice float64
	totalLine := 0
	stockMap := make(map[string]float64)

	fmt.Printf("start !!\n")


	//////////////////////////// mysql db connect ////////////////////////////
	db, err := sql.Open("mysql", "erpy:kiwitomato.com@tcp(erpyjun2.cafe24.com:3306)/day_data")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	//////////////////////////// read directory list ////////////////////////////
	files, err := ioutil.ReadDir("I:\\데이터백업\\")
	if err != nil {
		log.Fatal(err)
	}

	sDate := ""
	fileLen := 0
	filePath := ""
	for _, f := range files {
		filePath = fmt.Sprintf("%s%s","I:\\데이터백업\\", f.Name())
		fi, err := os.Stat(filePath)
		if err != nil {
			log.Fatal(err)
			return
		}

		if strings.Contains(filePath, "dummy-") == false {
			continue
		}

		switch mode := fi.Mode(); {
		case mode.IsDir():
		case mode.IsRegular():
			totalLine = 0
			stockMap = make(map[string]float64)
			fileLen = len(filePath)
			sDate = filePath[25:fileLen-4]
			fmt.Printf("name [%s], date [%s]\n", filePath, sDate)

			fp, err := os.Open(filePath)
			if err != nil {
				log.Fatal(err)
			}
			defer fp.Close()

			scanner := bufio.NewScanner(fp)
			for scanner.Scan() {
				fields := strings.Split(scanner.Text(),"^")
				if len(fields) != 13 {
					continue
				}

				// code
				code = fields[0]
				// sign
				sign, _ := strconv.ParseFloat(strings.TrimSpace(fields[1]), 64)
				tmpPrice, _ := strconv.ParseFloat(strings.TrimSpace(fields[3]), 64)
				// cur price
				curPrice := math.Abs(tmpPrice)

				if _, ok := stockMap[code]; ok {
					totalPrice = sign * curPrice / 100000000.0
					stockMap[code] = stockMap[code] + (totalPrice)
				} else {
					totalPrice = sign * curPrice / 100000000.0
					stockMap[code] = totalPrice
				}

				totalLine++
				if totalLine % 500000 == 0 {
					fmt.Printf("process [%d] %s\n", totalLine, filePath)
				}
			}

			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}

			fmt.Printf("================================\n")
			p := make(PairList, len(stockMap))
			i := 0
			for k, v := range stockMap {
				p[i] = Pair{k, v}
				i++
			}

			stmt, err := db.Prepare("INSERT acc_money SET s_code=?,s_date=?,s_money=?")
			if err != nil {
				log.Fatal(err)
			}

			sort.Sort(p)
			i = 0
			for _, k := range p {
				tmp = fmt.Sprintf("%.1f", k.Value)
				fMoney, _ := strconv.ParseFloat(tmp,32)
				res, err := stmt.Exec(k.Key, sDate, fMoney)
				if err != nil {
					log.Fatal(err)
				}

				id, err := res.LastInsertId()
				if err != nil {
					log.Fatal(err)
				}

				fmt.Printf("[%d] [%s] %s [%s] %s\n",i, k.Key, tmp, id, filePath)

				i++
			}
		}
	}
}
