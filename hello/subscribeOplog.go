package main

import "fmt"

import (
	"github.com/garyburd/go-mongo/mongo"
	"log"
	"time"
	"math"
	"strings"
//	_ "os"
)

var   askedCursor    int = 0
const askCursorMax   int = 400 


const changelogDb  string = "offer-db"
const changelogCol string = "oplog.subscription"
var   changelogPath = fmt.Sprint( changelogDb , "." , changelogCol )
const offers = "offers.test"
var   sixtyMongoSecondsEarlier mongo.Timestamp = mongo.Timestamp(5898548092499667758)	// limit timestamp


//var cq chan int = make(chan,0)
var cq chan int = make(chan int)


/*
this is a go implementation of a tailable cursor against the oplog
as describe at the bottom of this document: 
	http://docs.mongodb.org/manual/tutorial/create-tailable-cursor/
*/
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


	//clearAll(conn)


	// create a capped collection if it is empty
	dbChangeLog  := mongo.Database{conn, changelogDb, mongo.DefaultLastErrorCmd}	// get a database object
	colChangeLog := dbChangeLog.C(changelogCol)  
	n, _ := colChangeLog.Find(nil).Count()
	if n>0   {
		fmt.Println("capped oplog ",changelogPath,"already exists. Entries: ", n)
	} else {
		err1 := dbChangeLog.Run(
			mongo.D{
				{"create", fmt.Sprint( changelogCol ) },
				{"capped", true},
				{"size", 1024 },
			},
			nil,
		)
		if err1 != nil {
			log.Fatal(err1)
		} else {
			fmt.Println("capped oplog ",changelogPath,"created")
		}
	}


	log.Println("\n\n==connect_to_oplog==")
	dbOplog  := mongo.Database{conn, "local", mongo.DefaultLastErrorCmd}	// get a database object
	oplog    := dbOplog.C("oplog.rs")  // get collection


	// Limit(4).Skip(2) and skip are ignored for tailable cursors
	// most basic query would be
	// cursor, err := oplog.Find(nil).Tailable(true).AwaitData(true).Cursor()


	// if no timestamp is available, we could query for BSON.minKey constant 
	// as described here http://docs.mongodb.org/manual/reference/operator/type/ 
	// but no worki 
	/*
	cursor, err := oplog.Find( mongo.M{"ts": mongo.M{ "$type":-1}  }  ).Tailable(true).AwaitData(true).Sort( mongo.D{{"$natural", 1}} ).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	*/


	// instead, we start at some recent timestamp
	// and demand natural sort (default anyway?)
	// 	this can be time consuming
	sixtySecondsEarlier := int32(time.Now().Unix()) - 10*60
	fmt.Println(sixtySecondsEarlier )
	// make a mongo/bson timestamp from the unix timestamp
	//		according to http://docs.mongodb.org/manual/core/document/
	var sixtyInt64SecondsEarlier int64 = int64(sixtySecondsEarlier) << 32
	fmt.Println(sixtyInt64SecondsEarlier)

	sixtyMongoSecondsEarlier = mongo.Timestamp(5898932101230624778)		// example
	sixtyMongoSecondsEarlier = mongo.Timestamp(sixtyInt64SecondsEarlier)
	fmt.Println(sixtyMongoSecondsEarlier)

	cursor, err := oplog.Find( mongo.M{"ts": mongo.M{ "$gte":sixtyMongoSecondsEarlier}  }  ).Tailable(true).AwaitData(true).Sort( mongo.D{{"$natural", 1}} ).Cursor()
	if err != nil {
		log.Fatal(err)
	}


	go iterateTailCursor(cursor,colChangeLog)
	go fill()
	x := <- cq
	fmt.Println("quit signal received: ", x)

}


func fill(){

	conn := getConn()
	defer conn.Close()
	dbChangeLog  := mongo.Database{conn, changelogDb, mongo.DefaultLastErrorCmd}	// get a database object
	colOffers := dbChangeLog.C(offers)  
	
	for i:=0 ; i < 100; i++ {
		err := colOffers.Insert(mongo.M{"offerId": i,
			 "shopId"     : 20, 
			 "lastSeen"   : int32(time.Now().Unix()) ,
			 "categoryId" : 15 ,
			 "title":       fmt.Sprint("title",i) ,
			 "description": strings.Repeat( fmt.Sprint("description",i), 100),
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	

	
}

func iterateTailCursor( c mongo.Cursor, oplogsubscription mongo.Collection ){

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

			if  m["ts"] == nil {
				fmt.Println("no timestamp")
			}

			var innerMap mongo.M = nil

			ns := m["ns"] 
			//fmt.Printf("%+v %T -  vs. %v\n",ns, ns, changelogPath)

			if  ns != changelogPath {

				sixtyMongoSecondsEarlier = m["ts"].(mongo.Timestamp)
				var oid mongo.ObjectId = mongo.ObjectId("51dc1b9d419c")  // 12 chars

				//str, ok := data.(string) - http://stackoverflow.com/questions/14289256/cannot-convert-data-type-interface-to-type-string-need-type-assertion
				moo, ok := m["o"].(map[string]interface {})
				if ok {

					innerMap = moo

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

				err := oplogsubscription.Insert(mongo.M{"ts": sixtyMongoSecondsEarlier ,
					 "operation": m["op"], 
					 "oid" : oid ,
					  "ns": ns,
					  "del": "\n",
					  "im": innerMap,
				})
				if err != nil {
					log.Fatal(err)
				}

				fmt.Print("  ")



			} else {
				fmt.Print("      recurs. ")
			}
			printMap(m,true)
			if innerMap != nil { 
				fmt.Print("    inner map: ")
				printMap(innerMap,true) 
			}


		}


	}
	fmt.Println(" sending quit signal: ", 1)
	cq <- 1
	//panic("show")
	

}

/*
	tailable cursors either 
		1.) have no more records currently
		2.) become "dead" (cursor.cursorid == 0)
		3.) have a permanent error
	
	sadly - immediately after opening the cursor - 
		it appears as being "dead"

	therefore, currently upon "dead" - we sleep and retry

*/
func checkCursor( c mongo.Cursor  ) ( bool,bool ){

	alive   :=  c.GetId()

	hasNext := 	c.HasNext()

	if alive < 1 || hasNext == false {

		if hasNext == false  {
			log.Println( "has next returned false - going to sleep")
		}

		if alive < 1  {
			log.Println( fmt.Sprintf( "dead cursor id is %v - going to sleep", alive))
		}

		askedCursor++
		log.Println( fmt.Sprintf( "asked cursor %v of %v - going to sleep ", askedCursor, askCursorMax) )
		if askedCursor > askCursorMax {
			return true, false
		}
		time.Sleep( 400 * time.Millisecond)
	}


	if err := c.Err(); err != nil {
		log.Fatal(   fmt.Sprint( "cursor.Err() says ", err) )
	}

	return false, hasNext

}



/*
	non-tailable cursor
*/
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

/*
	boring little helper
*/
func printMap( m mongo.M, short bool ){

	if short {
		ms := fmt.Sprintf("%+v",m)
		ms2:=ms[0: int(math.Min(120,float64(len(ms))))]
		log.Println( ms2 )
	} else {
		log.Println( m )
		for k,v := range m {
			log.Printf("\tk: %v \t\t: %v \n", k,v )
		}

	}
}




// clearAll cleans up after previous runs of this applications.
func clearAll(conn mongo.Conn) {
	log.Print("\n\n== Clear documents and indexes created by previous run. ==\n")
	db := mongo.Database{conn, changelogDb, mongo.DefaultLastErrorCmd}
	db.Run(mongo.D{{"profile", 0}}, nil)
	db.C(offers).Remove(nil)
	db.Run(mongo.D{{"dropIndexes", offers}, {"index", "*"}}, nil)
}


