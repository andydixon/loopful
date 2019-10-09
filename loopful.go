package main

import "fmt"
import "github.com/docopt/docopt-go"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import "os/user"
import "os"
import "strings"

func main() {
	  usage := `loopful - run one MySQL query in batches

Usage:
	loopful --host=<mysql server> --user=<mysql user> --pass=<mysql password> --db=<database> --query=<SQL query to run> --batchsize=<max rows per call>

Options:
	--host=<hostname>	Hostname or IP of MySQL server
	--user=<username>	MySQL username (defaults to current user)
	--pass=<password>	MySQL Password
	--db=<database>		Database Name
	--query=<query>		MySQL query to run
	--batchsize=<num>	Size of batches (defaults to 1000)`

	opts, _ := docopt.ParseDoc(usage)

	connectString := "";
	hostname,_  := opts.String("--host")
	username,_  := opts.String("--user")
	password,_  := opts.String("--pass")
	database,_  := opts.String("--db")
	query,_	    := opts.String("--query");
	batchSize,_ := opts.String("--batchsize");
	user, err := user.Current()

	if hostname == "" {
		hostname = "localhost"
	}

	if username == "" {
		username = user.Username
	}

	if query == "" {
		fmt.Println("Missing query. Kinda fundamental really.")
		os.Exit(4)
	} else {
		// Strip trailing colon if there is one
		if strings.HasSuffix(query,";") {     // was query[len(query)-1] == byte(";") {
			query = query[:len(query)-1]
		}	
	}

	if batchSize == "" {
		batchSize = "1000"
	}

	connectString = username	

	if password != "" {
		connectString = connectString + ":" + password
	}

	connectString = connectString + "@tcp(" + hostname + ")/" + database
	
	db, err := sql.Open("mysql", connectString)
	fmt.Println("connected")
	if err != nil {
    		fmt.Println(err)
		os.Exit(3)
	}	

	var rowCount int64
	rowCount = 1
	iteration := 0

	for rowCount > 0 {
		iteration = iteration + 1
		newQuery := query+" LIMIT "+batchSize

		
		res,err := db.Exec(newQuery);
        	if err != nil {
                	fmt.Println(err)
                	os.Exit(3)
        	}
		
		rowCount, _ = res.RowsAffected()
		fmt.Println("Iteration ",iteration,": Query=",newQuery," affected = ", rowCount)
	}

}
