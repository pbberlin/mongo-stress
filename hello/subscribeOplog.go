package main

import "fmt"

import (
	"github.com/garyburd/go-mongo/mongo"
	"log"
	"time"
	"math"
	_ "os"
)


var   deadPeriods    int = 0
const deadPeriodsMax int = 310


const changelogDb  string = "learn"
const changelogCol string = "oplog.subscription"
var   changelogPath = fmt.Sprint( changelogDb , "." , changelogCol )



var    lts mongo.Timestamp = mongo.Timestamp(5898548092499667758)	// limit timestamp


func getConn() mongo.Conn {

	conn, err := mongo.Dial("localhost:27003")
	if err != nil {
		log.Fatal(err)
	}
	return conn

}


func main(){

	conn := getConn()
	defer conn.Close()


	// Wrap connection with logger so that we can view the traffic to and from the server.
	// conn = mongo.NewLoggingConn(conn, log.New(os.Stdout, "", 0), "")

	// Clear the log prefix for more readable output.
	log.SetFlags(0)


	//reset(conn)
	log.Println("\n\n==connect_to_oplog==")

	
	db := mongo.Database{conn, "local", mongo.DefaultLastErrorCmd}	// Create a database object.
	oplog := db.C("oplog.rs")  // create collection
	

	// Limit(4).Skip(2) and skip are ignored for tailable cursors
	// cursor, err := oplog.Find(nil                                            ).Tailable(true).AwaitData(true).Cursor()


	lts = mongo.Timestamp(5898548092499667779)
	cursor, err := oplog.Find( mongo.M{"ts": mongo.M{ "$gte":lts}  }            ).Tailable(true).AwaitData(true).Sort( mongo.D{{"$natural", 1}} ).Cursor()


	//cursor, err := oplog.Find( mongo.M{"insertDate": mongo.M{ "$type":-1}  } ).Tailable(true).AwaitData(true).Sort( mongo.D{{"$natural", 1}} ).Limit(4).Skip(2).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	iterateCursorTail(cursor)

}


func iterateCursorTail( c mongo.Cursor ){

	conn1 := getConn()
	defer conn1.Close()
	dbInsert := mongo.Database{conn1, changelogDb, mongo.DefaultLastErrorCmd}	// Create a database object.
	oplogsubscription := dbInsert.C( changelogCol )  // create collection


	//checkCursor(c)
	//for c.HasNext() {
	for {
		
		doBreak, hasNext := checkCursor(c)

		if doBreak {
			break
		}

		if hasNext {

			var m mongo.M
			err := c.Next(&m)
			if err != nil {
				log.Println(err, fmt.Sprint(err) )
			}

			/*
			mo := m["o"]
			fmt.Printf(" %+v  - %T\n",  mo, mo  )
			*/

			if  m["ts"] == nil {
				fmt.Println("no timestamp")
			}

			ns := m["ns"] 
			//fmt.Printf("%+v %T -  vs. %v\n",ns, ns, changelogPath)


			printMap(m,true)
			if  ns != changelogPath {

				lts = m["ts"].(mongo.Timestamp)
				var oid mongo.ObjectId = mongo.ObjectId("51dc1b9d419c")  // 12 chars

				//str, ok := data.(string) - http://stackoverflow.com/questions/14289256/cannot-convert-data-type-interface-to-type-string-need-type-assertion
				moo, ok := m["o"].(map[string]interface {})
				if ok {

					if moo["_id"] != nil {

						var ok1 bool
						oid, ok1  = moo["_id"].(mongo.ObjectId)
						if !ok1 {
							log.Fatal(err)
						}

						/*
						tmp,ok  := moo["insertDate"].(string)
						if ok {
							lastInsert = tmp
						}
						*/
					}

				} else {
					fmt.Printf(" m[\"o\"] No object map (delete op) \n")
				}

				err := oplogsubscription.Insert(mongo.M{"ts": lts , "operation": m["op"], "oid" : oid , "ns": ns})
				if err != nil {
					log.Fatal(err)
				}



			} else {
				fmt.Print("recursion ")
			}


		}


	}

}

func checkCursor( c mongo.Cursor  ) ( bool,bool ){

	dead :=  c.GetId()

	hasNext := 	c.HasNext()

	if dead < 1 || hasNext == false {

		if hasNext == false  {
			log.Println( "has next returned false - going to sleep")
		}
		if dead < 1  {
			log.Println( fmt.Sprintf( "dead cursor id is %v - going to sleep", dead))
		}

		deadPeriods++
		log.Println( fmt.Sprintf( "periods %v of %v - going to sleep ", deadPeriods, deadPeriodsMax) )
		if deadPeriods > deadPeriodsMax {
			return true, false
		}
		time.Sleep( 1200 * time.Millisecond)
	}


	if err := c.Err(); err != nil {
		//log.Println( fmt.Sprint( "cursor.Err() says ", err) )
		log.Fatal(   fmt.Sprint( "cursor.Err() says ", err) )
	}
	return false, hasNext

}




func iterateCursor(c mongo.Cursor ){
	for c.HasNext() {
		var m mongo.M
		err := c.Next(&m)
		if err != nil {
			log.Fatal(err)
		}
		printMap(m, false)
	}
}


func printMap( m mongo.M, short bool ){

	if short {
		ms := fmt.Sprintf("%+v",m)
		ms2:=ms[0: int(math.Min(4120,float64(len(ms))))]
		log.Println( ms2 )
	} else {
		log.Println( m )
		for k,v := range m {
			log.Printf("\tk: %v \t\t: %v \n", k,v )
		}

	}
}




// reset cleans up after previous runs of this applications.
func reset(conn mongo.Conn) {
	log.Print("\n\n== Clear documents and indexes created by previous run. ==\n")
	db := mongo.Database{conn, "learn", mongo.DefaultLastErrorCmd}
	db.Run(mongo.D{{"profile", 0}}, nil)
	db.C("unicorns").Remove(nil)
	db.C("hits").Remove(nil)
	db.Run(mongo.D{{"dropIndexes", "unicorns"}, {"index", "*"}}, nil)
	db.Run(mongo.D{{"dropIndexes", "hits"}, {"index", "*"}}, nil)
}


