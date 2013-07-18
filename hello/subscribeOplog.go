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
const noNextValueMax    int = 2 

const secondsDefer = 4							// upon cursor not found error - amount of sleep  - 
const secondsDeferTailCursor = 1		// after sleep - set back additional x seconds
const nofill = 0

const insertThreads     = 8
const insertsPerThread  = int64(28000)  // "cursor not found"
//const insertsPerThread  = int64(6000)  // "cursor not found"

const outputLevel = 0

const changelogDb  string = "offer-db"
const changelogCol string = "oplog.subscription"
const counterChangeLogCol string = "oplog.subscription.counter"
var   changelogPath = fmt.Sprint( changelogDb , "." , changelogCol )
const offers = "offers.test"
var   sixtyMongoSecondsEarlier mongo.Timestamp = mongo.Timestamp(5898548092499667758)	// limit timestamp


var cq  chan int     = make(chan   int   )        // channel quit
var chl chan []int64 = make(chan []int64 ,1)			// channel load
var arrayLoadTotal = make([]int64 ,insertThreads)
var loadTotal = int64(0)

var cht chan int64   = make(chan   int64 ,1)      // channel cursor tail
var tailTotal = int64(0)


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

	colChangeLog, colCounterChangeLog := ensureChangeLogExists(conn)


	go iterateTailCursor(colChangeLog, colCounterChangeLog)


	time.Sleep( 200 * time.Millisecond )
	
	for i:= 0; i< insertThreads; i++ {
		batchStamp := int64(time.Now().Unix() )<<32  +  int64(i)*insertsPerThread
		go loadInsert( i, batchStamp)
		go loadRead(   i, batchStamp)
		
	}



	
	arrayLoad := make( []int64, insertThreads )
	chl <- arrayLoad
	cht <- int64(0)
	
	
	x := <- cq
	log.Println("quit signal received: ", x)



	arrayLoad, ok := (<- chl)
	if ok {
		for k,_ := range arrayLoad {
			v2 := arrayLoadTotal[k]
			loadTotal += v2
			log.Printf("thread %v - load ops %v - ", k , v2)			
		}
	} else {
		log.Fatal("error reading from chl 1")
	}
	var quote float64 = float64(tailTotal)/float64(loadTotal)
	quote = math.Trunc(quote*1000)/10
	log.Print("==================================================")
	log.Printf("Loaded-Tailed: %v - %v - %v percent",loadTotal,tailTotal,quote)

	tsFinish := int32(time.Now().Unix())
	log.Println("tsFinish: ",tsFinish )


}


func loadRead(idxThread int , batchStamp int64){
	
	if nofill > 0 {
		return	
	}


}

func loadInsert(idxThread int , batchStamp int64){
	
	if nofill > 0 {
		return	
	}

	fctGetRecurseMsg := getRecurseMsg( fmt.Sprint("loadInsert",idxThread," "))

	conn := getConn()
	defer conn.Close()
	dbChangeLog  := mongo.Database{conn, changelogDb, mongo.DefaultLastErrorCmd}	// get a database object
	colOffers := dbChangeLog.C(offers)  
	
	for i:=batchStamp ; i < batchStamp+insertsPerThread; i++ {
		
		err := colOffers.Insert(mongo.M{"offerId": i,
			 "shopId"     : 20, 
			 "lastSeen"   : int32(time.Now().Unix()) ,
			 "categoryId" : 15 ,
			 "title":       fmt.Sprint("title",i) ,
			 "description": strings.Repeat( fmt.Sprint("description",i), 35),
		})
		if err != nil {
			log.Println(   fmt.Sprint( "mongo loadInsert error: ", err,"\n") )		
			log.Fatal(err)
		}
		log.Print( fctGetRecurseMsg() )

		const chunkSize = 100
		if (i+1) % chunkSize == 0 {
			//log.Print( "|" )		
			arrayLoad, ok := (<- chl)
			if ok {
				arrayLoad[idxThread] += chunkSize
				chl <- arrayLoad
			} else {
				log.Fatal("error reading from chl 2")
			}
		}
		
	}
	fmt.Print(" -loadInsert",idxThread," finished- ")
	
}

func iterateTailCursor( oplogsubscription mongo.Collection , oplogSubscriptionCounter mongo.Collection ){


	fctGetRecurseMsg := getRecurseMsg("recursion ")

	c := recoverTailableCursor()
	
	for {
		
		doBreak, hasNext := checkCursor(c)


		if doBreak {

			fmt.Println("going to sleep ",secondsDefer, " seconds")
			time.Sleep( secondsDefer * time.Second )
			
			if countNoNextValue < noNextValueMax {
				c = recoverTailableCursor()
				doBreak, _ := checkCursor(c)
				if doBreak {
					log.Println("second failure")
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
				log.Println("no timestamp")
			}

			var innerMap mongo.M = nil

			ns := m["ns"].(string) 
			//log.Printf("%+v %T -  vs. %v\n",ns, ns, changelogPath)

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
					log.Printf(" m[\"o\"] No object map (delete op) \n")
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


				incTailCounter()
				errCounter := oplogSubscriptionCounter.Update( mongo.M{"counter": mongo.M{"$exists": true}, } , mongo.M{"$inc"   : mongo.M{"counter": 1} } ,)
				if errCounter != nil {
					log.Fatal("lf12 ",errCounter)
				}


			} else {
				fmt.Print( fctGetRecurseMsg() )
			}
			if innerMap != nil { 
				printMap(innerMap,true,"      inner map: ") 
			}


		}


	}
	log.Println(" sending quit signal: ", 1)
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
			log.Println( fmt.Sprintf( "dead cursor id is %v", alive) )
		}

		log.Println( fmt.Sprintf( "await is over - no next value no. %v of %v", countNoNextValue, noNextValueMax) )

		countNoNextValue++
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

				return ""
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
	sixtySecondsEarlier := int32(time.Now().Unix()) - secondsDefer - secondsDeferTailCursor

	// make a mongo/bson timestamp from the unix timestamp
	//		according to http://docs.mongodb.org/manual/core/document/
	var sixtyInt64SecondsEarlier int64 = int64(sixtySecondsEarlier) << 32
	//log.Println(sixtyInt64SecondsEarlier)

	sixtyMongoSecondsEarlier = mongo.Timestamp(5898932101230624778)		// example
	sixtyMongoSecondsEarlier = mongo.Timestamp(sixtyInt64SecondsEarlier)

	cursor, err := oplog.Find( mongo.M{"ts": mongo.M{ "$gte":sixtyMongoSecondsEarlier}  }  ).Tailable(true).AwaitData(true).Sort( mongo.D{{"$natural", 1}} ).Cursor()


	//fmt.Println(  " ts1 = Math.round( new Date().getTime()/1000) -300;" )
	fmt.Println(  "ts2 = new Timestamp(",sixtySecondsEarlier,", 0);" )
	fmt.Println(  "db.getSiblingDB('local').oplog.rs.find({'ts': { '$gte': ts2 }  }, {ts:1,op:1}  ).sort( {\"$natural\": 1} ) " )

	// db.getSiblingDB('offer-db').offers.test.find({},{description:0}).max({_id: ObjectId("51e800067e6abf81274b4e35") })

	 	  
	// .addOption(DBQuery.Option.tailable).addOption(DBQuery.Option.awaitData)
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
		log.Println("capped oplog ",changelogPath,"already exists. Entries: ", n)
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
			log.Println("capped oplog ",changelogPath,"created")
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
	log.Print( timeStart.Format( layout3 ) )
	ctick := time.Tick(millisecs * time.Millisecond)


	go func() { 
		for now := range ctick {

			writeLoadInfo()
			writeTailInfo(now,timeStart, float64(millisecs))

		}
	}()
	
	
}


func recoverTailableCursor() mongo.Cursor {
	
		//log.Println(   "Trying to recover: " )	
		conn := getConn()
		oplog  := getOplogCollection(conn, "")
		//log.Println(   "Oplog retrieved " )	
		c := getTailableCursor(oplog)

		return c

	
}


func writeLoadInfo() {
	
	
		arrayLoad, ok := (<- chl)
		if ok {
			sum := int64(0)
			for k,v := range arrayLoad {
				sum += int64(v)
				arrayLoadTotal[k] += int64(v)
			}
			if sum > 0 {
				fmt.Printf("l%v ",sum)			
			}
			arrayLoadNew := make( []int64, insertThreads )
			chl <- arrayLoadNew
	
		} else {
			log.Fatal("error reading from chl 3")
		}
	
	
}


func writeTailInfo(now time.Time, timeStart time.Time, millisecs float64){

		cntTail,_ := (<- cht) 
		cht <- 0
		
		//strSeconds := fmt.Sprint( now.Sub(timeStart).Seconds() )

		tailTotal += cntTail

		perSec := float64(cntTail) * millisecs / 1000
		perSec  = math.Trunc( 10* perSec) / 10
		
		
		if perSec < 1 {
			fmt.Print( "|" )							
		} else {
			//fmt.Printf( " -%v %v- ",perSec,cntTail )			
			fmt.Printf( "t%v ",perSec )			
		}
	
	
}

func incTailCounter(){
	
		cntTail,ok := (<- cht)
		if ok {
			cntTail++
			cht  <- cntTail
			//print("cntTail:",cntTail)
		} else {
			print("cht is closed\n")
		}						
	
}