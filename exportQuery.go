package exportrows

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const NOTE = `
Table Name or Quries should be semicolon seperated That you want to export:
Examples :-
 \/	
 /\ 	isg_team isg_sports;

  /
\/		select * from isg_team; isg_sports;

 \/	
 /\ 	 select * isg_team, select * from isg_sports;

  /
\/  	select * isg_team; select * from isg_sports;

  /
\/         	 isg_team           ;          isg_sports

  /
\/  	select * from isg_team

  /
\/  isg_team

Note: If the text contains single words separated by semicolons, 
each word will be treated as a table name. 
Otherwise, the entire text will be used as a direct query

Note : Batch Insert size will be 2000 for each query`

type Query struct {
	Query string
	Table string
}

var queries = []Query{}
var input string
var tablename string = ""
var filename string = "dump"
var num int = 1
var batchSize int = 2000
var GoThreads int = 1

type Config struct {
	PORT int
	DB   DB
}

var err error

type DB struct {
	DBType   string
	Username string
	Password string
	Host     string
	Dbname   string
	PORT     int
}

var Config1 = Config{
	PORT: 8000,
	DB: DB{
		DBType:   "mysql",
		Username: "root",
		Password: "",
		Host:     "localhost",
		Dbname:   "isports",
		PORT:     3306,
	},
}

func ExportQueryToSQL() {
	fmt.Println(`EZ exporter of funfact team by bhuvnesh
hit Enter if want to use default configuration while configuring exporter setup
default configuration : 
	Username = "root",
	Password = "",
	Host =    "localhost",
	Dbname =  "isports",
	PORT = 3306,`)
	homeDir := os.Getenv("USERPROFILE")
	path := filepath.Join(homeDir, "Downloads")
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("\nEnter Username:")
	username, _ := reader.ReadString('\n')
	username = strings.TrimSpace(username)
	if username != "" {
		Config1.DB.Username = username
	}
	reader = bufio.NewReader(os.Stdin)
	fmt.Println("\nEnter Password:")
	password, _ := reader.ReadString('\n')
	password = strings.TrimSpace(password)
	if password != "" {
		Config1.DB.Password = password
	}
	reader = bufio.NewReader(os.Stdin)
	fmt.Println("\nEnter Port:")
	port, _ := reader.ReadString('\n')
	port = strings.TrimSpace(port)
	if port != "" {
		Config1.DB.PORT, err = strconv.Atoi(port)
		if err != nil {
			return
		}
	}
	reader = bufio.NewReader(os.Stdin)
	fmt.Println("\nEnter No of Threads you want to use:")
	thread, _ := reader.ReadString('\n')
	thread = strings.TrimSpace(thread)
	if thread != "" {
		temp, err := strconv.Atoi(thread)
		if temp > 0 {
			GoThreads = temp
		} else {
			fmt.Println("invalid thread count using default 1")
		}
		if err != nil {
			fmt.Println("invalid thread count using default 1")
		}
	} else {
		fmt.Println("using thread default 1")
	}

	DB1, err := sql.Open(fmt.Sprintf("%s", Config1.DB.DBType), fmt.Sprintf("%s:%s@tcp(%s:%v)/%s", Config1.DB.Username, Config1.DB.Password, Config1.DB.Host, Config1.DB.PORT, Config1.DB.Dbname))
	if err != nil {
		fmt.Println(err, "some err while connecting to db", err)
		return
	} else {
		err = DB1.Ping()

		if err != nil {
			fmt.Println("some err while pinging to db", err)
			fmt.Println(fmt.Sprintf("%s", Config1.DB.DBType), fmt.Sprintf("%s:%s@tcp(%s:%v)/%s", Config1.DB.Username, Config1.DB.Password, Config1.DB.Host, Config1.DB.PORT, Config1.DB.Dbname))
			return
		} else {
			fmt.Printf("database connection successfull with db %s", Config1.DB.Dbname)
		}
	}
	fmt.Println(NOTE)
		fmt.Println(`
Note :- press Ctrl+Z + Enter on Windows, or Ctrl+D on Linux/macOS to finish writting Queries
`)
	for {
		fmt.Println("Enter Queries and Tables :")
		for {
			data, err := io.ReadAll(os.Stdin)
			if err != nil {
				fmt.Println(err)
			}
			lines := strings.TrimSpace(string(data))
			if lines != "" {
				tempQuery := strings.Split(lines, ";")
				if len(tempQuery) > 0 {
					for _, tq := range tempQuery {
						tq = strings.TrimSpace(tq)
						qw := strings.Split(tq, " ")
						for i := 0; i < len(qw); i++ {
							qw[i] = strings.TrimSpace(qw[i])
							if qw[i] == "" {
								qw = append(qw[:i], qw[i+1:]...)
								i--
							}
						}
						if len(qw) == 1 {
							queries = append(queries, Query{Query: `select * from ` + qw[0], Table: qw[0]})
						} else if len(qw) > 3 {
							for i := 0; i < len(qw)-1; i++ {
								if qw[i] != "" {
									if strings.ToLower(qw[i]) == "from" {
										queries = append(queries, Query{Query: tq, Table: qw[i+1]})
										break
									}
								}
							}
						} else {
							fmt.Println("Not exporting Query, seems invalid", tq)
						}
					}
				}
			}
			if len(queries) > 0 {
				break
			}
			fmt.Println("Please write some query or table name to export")
		}

		for _, query := range queries {
			filename = fmt.Sprintf("dump_%s_%d", time.Now().Format("20060102150405"), num)
			num++
			fmt.Println("query is = ", query.Query)
			tablename = query.Table
			rows, err := DB1.Query(query.Query)
			if err != nil {
				fmt.Printf("error executing query: %v", err)
				continue
			}
			defer rows.Close()

			columns, err := rows.Columns()
			if err != nil {
				fmt.Printf("error getting columns: %v", err)
				continue
			}

			file, err := os.Create(path + "\\" + filename + ".sql")
			if err != nil {
				fmt.Printf("error creating output file: %v", err)
				continue
			}

			header := fmt.Sprintf("/*\n-- Query: %s\n-- Table : %s\n-- Date: %s\n*/\n",
				query.Query,
				query.Table,
				time.Now().Format("2006-01-02 15:04"))

			_, err = file.WriteString(header)
			if err != nil {
				fmt.Printf("error writing header: %v", err)
				continue
			}

			var allRows [][]interface{}
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))

			for i := range columns {
				valuePtrs[i] = &values[i]
			}

			for rows.Next() {
				err := rows.Scan(valuePtrs...)
				if err != nil {
					fmt.Printf("error scanning row: %v", err)
					continue
				}

				rowValues := make([]interface{}, len(columns))
				for i, v := range values {
					rowValues[i] = v
				}
				allRows = append(allRows, rowValues)
			}

			rowsPerThread := len(allRows) / GoThreads
			var wg sync.WaitGroup
			resultChan := make(chan string, GoThreads)

			for i := 0; i < GoThreads; i++ {
				wg.Add(1)
				start := i * rowsPerThread
				end := start + rowsPerThread
				if i == GoThreads-1 {
					end = len(allRows)
				}

				go func(start, end int) {
					defer wg.Done()
					var insertStatements strings.Builder
					insertStatements.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tablename, strings.Join(columns, ", ")))

					for j, rowValues := range allRows[start:end] {
						values := make([]string, len(rowValues))
						for k, v := range rowValues {
							values[k] = formatValue(v)
						}
						insertStatements.WriteString(fmt.Sprintf("(%s)", strings.Join(values, ", ")))

						if (j+1)%batchSize == 0 || j == end-start-1 {
							insertStatements.WriteString(";\n")

							resultChan <- insertStatements.String()
							insertStatements.Reset()
							if j < end-start-1 {

								insertStatements.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tablename, strings.Join(columns, ", ")))
							}
						} else {
							insertStatements.WriteString(", ")
						}
					}
				}(start, end)
			}

			go func() {
				wg.Wait()
				close(resultChan)
			}()

			for result := range resultChan {
				_, err := file.WriteString(result)
				if err != nil {
					fmt.Println("Error writing to file: ", err)
					continue
				}
			}
			fmt.Println("file exported successfully in downloads folder Filename : ",filename)
			file.Close()

		}
		queries = []Query{}
	}
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch v := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case []uint8:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(string(v), "'", "''"))
	default:
		return fmt.Sprintf("%v", v)
	}
}
