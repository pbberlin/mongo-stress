package main


import (
	"github.com/garyburd/go-mongo/mongo"
	"log"
	"time"
	"math"
	"strings"
	"fmt"
//	_ "os"
)

var   countNoNextValue  int = 0
const noNextValueMax    int = 4 
const secondsPast = 2

const insertThreads     = 8
const insertsPerThread  = int64(2000)  // "cursor not found"
//const insertsPerThread  = int64(6000)  // "cursor not found"

const outputLevel = 0

const changelogDb  string = "offer-db"
const changelogCol string = "oplog.subscription"
const counterChangeLogCol string = "oplog.subscription.counter"
var   changelogPath = fmt.Sprint( changelogDb , "." , changelogCol )
const offers = "offers.test"
var   sixtyMongoSecondsEarlier mongo.Timestamp = mongo.Timestamp(5898548092499667758)	// limit timestamp


var cq chan int = make(chan int) // channel quit
var chl chan []int = make(chan []int )			// channel load
var cht chan map[string]int = make(chan map[string]int,1)   // channel cursor tail


/*
this is a go implementation of a tailable cursor against the oplog
as describe at the bottom of this document: 
	http://docs.mongodb.org/manual/tutorial/create-tailable-cursor/
*/
func getConn() mongo.Conn {

	conn, err := mongo.Dial("localhost:27001")
	if err != nil {
		log.Println("getConn failed")
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


	startTimerLog()

	oplog  := getOplogCollection(conn, "")
	cursor := getTailableCursor(oplog)
	colChangeLog, colCounterChangeLog := ensureChangeLogExists(conn)


	go iterateTailCursor(cursor,colChangeLog, colCounterChangeLog)

	for i:=int64(0); i< insertThreads; i++ {
		go fill( int64(time.Now().Unix() )<<32  +  i*insertsPerThread, i )
	}
	
	arrayLoad := make( []int, insertThreads )
	chl <- arrayLoad
	
	
	x := <- cq
	fmt.Println("quit signal received: ", x)



	arrayLoad, ok := (<- chl)
	if ok {
		for k,v := range arrayLoad {
			fmt.Printf("thread %v - load ops %v - ", k , v)			
		}
		fmt.Println("")
	} else {
		log.Fatal("error reading from chl 1")
	}

}


func fill(start int64, idxThread int64 ){

	//fctGetRecurseMsg := getRecurseMsg( fmt.Sprint("fill",idxThread," "))

	conn := getConn()
	defer conn.Close()
	dbChangeLog  := mongo.Database{conn, changelogDb, mongo.DefaultLastErrorCmd}	// get a database object
	colOffers := dbChangeLog.C(offers)  
	
	for i:=start ; i < start+insertsPerThread; i++ {
		
		err := colOffers.Insert(mongo.M{"offerId": i,
			 "shopId"     : 20, 
			 "lastSeen"   : int32(time.Now().Unix()) ,
			 "categoryId" : 15 ,
			 "title":       fmt.Sprint("title",i) ,
			 "description": strings.Repeat( fmt.Sprint("description",i), 100),
		})
		if err != nil {
			log.Println(   fmt.Sprint( "mongo fill error: ", err,"\n") )		
			log.Fatal(err)
		}
		//fmt.Print( fctGetRecurseMsg() )

		const chunkSize = 100
		if i % chunkSize == 0 {
			//fmt.Print( "|" )		
			arrayLoad, ok := (<- chl)
			if ok {
				arrayLoad[idxThread] += chunkSize
				chl <- arrayLoad
			} else {
				log.Fatal("error reading from chl 2")
			}
		}
		
	}


	fmt.Println("\nfill",idxThread," finished")
	
}

func iterateTailCursor( c mongo.Cursor, oplogsubscription mongo.Collection , oplogSubscriptionCounter mongo.Collection ){


	fctGetRecurseMsg := getRecurseMsg("recursion ")
	//checkCursor(c)
	//for c.HasNext() {
	for {
		
		doBreak, hasNext := checkCursor(c)


		if doBreak {

			time.Sleep( 400 * time.Millisecond )
			
			if countNoNextValue < noNextValueMax {
				c = recoverTailableCursor()
				doBreak, _ := checkCursor(c)
				if doBreak {
					fmt.Println("second failure")
					break
				}
			} else {
				break		
			}
		}

		if hasNext{

			var m mongo.M
			err := c.Next(&m)
			if err != nil {
				log.Println(err, fmt.Sprint(err) )
			}

			if  m["ts"] == nil {
				fmt.Println("no timestamp")
			}

			var innerMap mongo.M = nil

			ns := m["ns"].(string) 
			//fmt.Printf("%+v %T -  vs. %v\n",ns, ns, changelogPath)

			if   ! strings.HasPrefix( ns ,changelogPath)  {

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

					}

				} else {
					fmt.Printf(" m[\"o\"] No object map (delete op) \n")
				}

				err := oplogsubscription.Insert(mongo.M{"ts": sixtyMongoSecondsEarlier ,
					  "operation": m["op"], 
					  "oid" : oid ,
					  "ns": ns,
					  "im": innerMap,
				})
				if err != nil {
					log.Fatal(err)
				}
				printMap(m,true,"   ")


				errCounter := oplogSubscriptionCounter.Update( mongo.M{"counter": mongo.M{"$exists": true}, } , mongo.M{"$inc"   : mongo.M{"counter": 1} } ,)
				if errCounter != nil {
					log.Fatal("lf12 ",errCounter)
				}



				const chunkSize = 100
				if i % chunkSize == 0 {
					//fmt.Print( "|" )		
					arrayLoad, ok := (<- chl)
					if ok {
						arrayLoad[idxThread] += chunkSize
						for k,v := range arrayLoad {
							
						}
						arrayLoadNew := make( []int, insertThreads )
						chl <- arrayLoadNew

					} else {
						log.Fatal("error reading from chl 3")
					}
				}


				select {
					case monData,ok := (<- cht) :
						if ok {
							//print("received \n")
						} else {
							print("cht is closed\n")
						}						
						ix := monData["cntr"]
						//fmt.Println("producer: read - write", ix)
						ix++
						monData["cntr"] = ix
						cht  <- monData
					default: 
						//fmt.Println("producer: noop")
						//fmt.Println("producer: write a")
						monData := map[string]int{
						    "cntr": 1,
						}
						//fmt.Println("producer: write b")
						cht<-monData
						//fmt.Print("reset cntr")
				}				



			} else {
				//fmt.Print(" recurs.")
				fmt.Print( fctGetRecurseMsg() )
				
			}
			if innerMap != nil { 
				printMap(innerMap,true,"      inner map: ") 
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
			log.Println( fmt.Sprintf( "dead cursor id is %v - going to sleep\n", alive))
		}

		countNoNextValue++
		log.Println( fmt.Sprintf( "await over - no next value no. %v of %v - going to sleep\n", countNoNextValue, noNextValueMax) )
		if countNoNextValue > noNextValueMax {
			return true, false
		}
	}


	if err := c.Err(); err != nil {
		log.Println(   fmt.Sprint( "mongo permanent cursor error: ", err,"\n") )
		return true, hasNext
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
		printMap(m, false,"")
	}
}

/*
	boring little helper
*/
func printMap( m mongo.M, short bool, prefix string ){

	if outputLevel < 2 {
		return	
	}
	
	if short {
		ms := fmt.Sprintf("%+v",m)
		ms2:=ms[0: int(math.Min(120,float64(len(ms))))]
		log.Println( prefix, ms2 )
	} else {
		log.Println( m )
		for k,v := range m {
			log.Printf("%v\tk: %v \t\t: %v \n", prefix ,k,v )
		}

	}
}



func getRecurseMsg(cmsg string) func() string {

    ctr := 0

    var cmsgl int = len(cmsg)
    msg := ""
    csr := 0

    return func() string {

    		ctr++
    		if mod := ctr % 100; mod != 0{
    			return ""	
    		}

		    if  len(msg) >= len(cmsg) {
		    	msg = ""
		    }
    		xlen := len(msg)
    		ylen := len(msg)+1
        msg = fmt.Sprint(msg, cmsg[xlen:ylen])
        
		    if  csr >= cmsgl {
		    	csr = 0
		    }
        msg = fmt.Sprint("", cmsg[csr:csr+1])
		    csr++

        return fmt.Sprint(msg)
        return fmt.Sprint(ctr, msg,"-",csr,"-",csr+1,"-\n")
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


func getOplogCollection(conn mongo.Conn, colName string) mongo.Collection {
	
	if colName == "" {
		colName = 	"oplog.rs"
	}
	
	log.Print(  fmt.Sprint("\n\n==(re-)connect_to_",colName,"== ... ") )
	dbOplog  := mongo.Database{conn, "local", mongo.DefaultLastErrorCmd}	// get a database object
	oplog    := dbOplog.C( colName )  // get collection
	
	return oplog

}


func getTailableCursor( oplog mongo.Collection ) mongo.Cursor  {

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
	sixtySecondsEarlier := int32(time.Now().Unix()) - secondsPast
	//fmt.Println(sixtySecondsEarlier )
	// make a mongo/bson timestamp from the unix timestamp
	//		according to http://docs.mongodb.org/manual/core/document/
	var sixtyInt64SecondsEarlier int64 = int64(sixtySecondsEarlier) << 32
	//fmt.Println(sixtyInt64SecondsEarlier)

	sixtyMongoSecondsEarlier = mongo.Timestamp(5898932101230624778)		// example
	sixtyMongoSecondsEarlier = mongo.Timestamp(sixtyInt64SecondsEarlier)
	//fmt.Println(sixtyMongoSecondsEarlier)

	cursor, err := oplog.Find( mongo.M{"ts": mongo.M{ "$gte":sixtyMongoSecondsEarlier}  }  ).Tailable(true).AwaitData(true).Sort( mongo.D{{"$natural", 1}} ).Cursor()
	if err != nil {
		log.Println(   fmt.Sprint( "mongo oplog find error: ", err,"\n") )		
		log.Fatal(err)
	}

	log.Println( " ... tailable cursor retrieved. Id ", cursor.GetId() )	
	return cursor

}


func ensureChangeLogExists(conn mongo.Conn) (mongo.Collection, mongo.Collection){
	
	// create a capped collection if it is empty
	dbChangeLog  := mongo.Database{conn, changelogDb, mongo.DefaultLastErrorCmd}	// get a database object
	colChangeLog := dbChangeLog.C(changelogCol)  
	n, _ := colChangeLog.Find(nil).Count()
	if n>0   {
		fmt.Println("capped oplog ",changelogPath,"already exists. Entries: ", n)
	} else {
		errCreate := dbChangeLog.Run(
			mongo.D{
				{"create", fmt.Sprint( changelogCol ) },
				{"capped", true},
				{"size", 1024*1024 },
			},
			nil,
		)
		if errCreate != nil {
			log.Fatal(errCreate)
		} else {
			fmt.Println("capped oplog ",changelogPath,"created")
		}
	}
	colChangelogCounter := dbChangeLog.C(counterChangeLogCol)  
	errCounter := colChangelogCounter.Upsert( mongo.M{"counter": mongo.M{"$exists": true}, } , mongo.M{"counter": 1}  ,)
	if errCounter != nil {
		log.Fatal("lf11 ",errCounter)
	}

	
	return colChangeLog, colChangelogCounter
	
	
}

func startTimerLog(){
	
	const millisecs = 500
	
	const layout2 = "15:04 05"
	const layout3 = " 05.0 "
	// http://digital.ni.com/public.nsf/allkb/A98D197224CB83B486256BC100765C4B

	timeStart := time.Now()
	fmt.Print( timeStart.Format( layout3 ) )
	ctick := time.Tick(millisecs * time.Millisecond)


	go func() { 
		for now := range ctick {


			var ix int = 0
			//fmt.Println("consume0")
			select {
				
				case monData,_ := (<- cht) : 
					ix = monData["cntr"]
					//fmt.Println("consume1",ok)
		
				
				default: 
					ix = 0
					//fmt.Println("consume2")
			}				
			//fmt.Println("consume3")


			//fmt.Print( now.Sub(timeStart).Format( layout3 ), ix/ ( 1000/millisecs) )
			strSeconds := fmt.Sprint( now.Sub(timeStart).Seconds() )
			strSeconds2:=strSeconds[0:4]

			strInsertPerSec  := fmt.Sprint( float64(ix)/(1000/millisecs) )
			lenS := len( strInsertPerSec )
			if lenS > 5 { lenS = 5}			
			strInsertPerSec2 := strInsertPerSec[0:lenS]
			strSeconds2 = fmt.Sprint(strSeconds2,"")
			//fmt.Print( strSeconds2, " - ", strInsertPerSec2, "; " )
			if strInsertPerSec2 == "0" {
				fmt.Print( "|" )							
			} else {
				fmt.Print( strInsertPerSec2, " " )			
			}
		}
	}()
	
	
}


func recoverTailableCursor() mongo.Cursor {
	
		//log.Println(   "Trying to recover: " )	
		time.Sleep( 400 * time.Millisecond)
		conn := getConn()
		oplog  := getOplogCollection(conn, "")
		//log.Println(   "Oplog retrieved " )	
		
		c := getTailableCursor(oplog)

		return c

	
}