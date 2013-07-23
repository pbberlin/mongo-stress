package main


/*
This is a go implementation of a tailable cursor against the oplog
as describe at the bottom of this document: 
	http://docs.mongodb.org/manual/tutorial/create-tailable-cursor/

It is also a Mongo Load Tester
	
It Tests Inserts, Reads, 


*/

import (
	"github.com/garyburd/go-mongo/mongo"
	"log"
	"time"
	"math"
	"strings"
	"fmt"
//	_ "os"
  "net/http"
	"sync/atomic"  
)

var   countNoNextValue  int = 0
const noNextValueMax    int = 2 

const secondsDefer = 4							// upon cursor not found error - amount of sleep  - 
const secondsDeferTailCursor = 1		// after sleep - set back additional x seconds
const noInsert = 0

const LOAD_THREADS     = 8
const insertsPerThread  = int64(2000)  // if oplog is not big enough, causes "cursor not found"


const outputLevel = 0

const changelogDb  string = "offer-db"
const changelogCol string = "oplog.subscription"
const counterChangeLogCol string = "oplog.subscription.counter"
var   changelogPath = fmt.Sprint( changelogDb , "." , changelogCol )
const offers = "offers.test"
var   mongoSecsEarlier mongo.Timestamp = mongo.Timestamp(5898548092499667758)	// limit timestamp

const readBatchSize= 100

var chl chan []int64 = make(chan []int64 ,1)			// channel load
var arrayLoadTotal = make([]int64 ,LOAD_THREADS)

var chr chan []int64 = make(chan []int64 ,1)			// channel read
var arrayReadTotal = make([]int64 ,LOAD_THREADS)

var cht chan int64   = make(chan   int64 ,1)      // channel cursor tail
var tailTotal = int64(0)

var cq  chan int     = make(chan   int   )        // channel quit

var	tsStart int64      = time.Now().Unix()
var	tStart  time.Time  = time.Now()


var timeXSaveOp   int64 = time.Now().Unix()
var lastSaveOperation   int64 = time.Now().Unix()


func main(){
	http.HandleFunc("/", handler)
	http.ListenAndServe(":8080", nil)
}

func getFuncLoadRead()  func(idxThread int,batchStamp int64) {

	//newOid := mongo.NewObjectId()
	//minOid := mongo.MinObjectIdForTime( tStart.Add(-200 * time.Millisecond))
	minOid1999 := mongo.MinObjectIdForTime( time.Date(1999, time.November, 10, 15, 25, 0, 222, time.Local))
	mgoCmd := fmt.Sprint( "db.getSiblingDB(\"offer-db\").offers.test.find({},{description:0}).min({_id: ObjectId(\"",minOid1999,"\") })" )
	fmt.Println(mgoCmd)

	mgoCmd  = fmt.Sprint( "db.getSiblingDB(\"offer-db\").offers.test.find({},{description:0}).sort({\"_id\":-1})" )
	fmt.Println(mgoCmd)
	

	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"offer-db\").oplog.subscription.find({},{im:0}).sort({\"_id\":-1})" )
	fmt.Println(mgoCmd)

	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"offer-db\").oplog.subscription.counter.find({},{_id:0,changed3:0})" )
	fmt.Println(mgoCmd)


	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"local\").oplog.rs.find({},{o:0}).sort({\"$natural\":-1})" )
	fmt.Println(mgoCmd)




	return func(idxThread int , batchStamp int64) {

		//fmt.Println( "loadRead: ", idxThread , batchStamp  )	
		fctGetRecurseMsg := getRecurseMsg( fmt.Sprint("loadRead",idxThread," "))


		conn := getConn()
		defer conn.Close()
		colOffers := getCollection(conn,changelogDb,offers)

		getPartitionStart := getFuncPartitionStart()
		
		minOid,initMinOid:= getPartitionStart(idxThread )
		loopMinOid := initMinOid

		i := int64(0)
		for  {

			i++
			if i > (10 * 1000 * 1000) {
				log.Println("more than 10.000 iterations. Break.")		
				break	
			}
			log.Print( fctGetRecurseMsg() )
			incReadCounter(i,idxThread)


			var m mongo.M
		  err := colOffers.Find(mongo.M{"_id": mongo.M{"$gte": loopMinOid,},}).Fields(mongo.M{"description": 0}).Skip(readBatchSize).Limit(1).One(&m)
			if err != nil  && err != mongo.Done {
				log.Println(   fmt.Sprint( "mongo loadRead error: ", err,"\n") )		
				log.Fatal(err)
			}

			tmpMinOid, ok := m["_id"].(mongo.ObjectId)
			if ! ok {
				if err.Error() == "mongo: cursor has no more results" {
					fmt.Print(" rstrt",idxThread)
					loopMinOid = minOid
					continue
				} else {
					log.Fatal("end of read seq. err: ", err)
				}
			} else if loopMinOid == tmpMinOid {
					fmt.Print(" rstrtTWO",idxThread)
				loopMinOid = minOid
				continue
			} else {
				//fmt.Println(idxThread, " new oid" , loopMinOid, tmpMinOid )
				loopMinOid = tmpMinOid
			}

			
		}
		fmt.Print(" -loadRead",idxThread,"_finished- ")



	}


}

func loadInsert(idxThread int , batchStamp int64){
	
	if noInsert > 0 {
		return	
	}

	fctGetRecurseMsg := getRecurseMsg( fmt.Sprint("loadInsert",idxThread," "))

	conn := getConn()
	defer conn.Close()
	colOffers := getCollection( conn, changelogDb, offers  )
	
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

		incLoadCounter(i,idxThread)
		_ =  updLoadTime( time.Now().Unix() ,0)	
		
	}
	fmt.Print(" -loadInsert",idxThread,"_finished- ")
	
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

				var ok bool = true	
				mongoSecsEarlier,ok = m["ts"].(mongo.Timestamp)
				if ! ok {
					log.Fatal("m[ts] not a valid timestamp")	
				}
				timeOfSaveOperation := int64(mongoSecsEarlier) >> 32
				//_ =  updLoadTime( 0, time.Now().Unix() )	
				_ =  updLoadTime( 0, timeOfSaveOperation )	
				
				
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

				err := oplogsubscription.Insert(mongo.M{"ts": mongoSecsEarlier ,
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
				
				//errCounter := oplogSubscriptionCounter.Update( mongo.M{"counter": mongo.M{"$exists": true}, } , mongo.M{"$inc"   : mongo.M{"counter": 1}, "$set" : mongo.M{"changed3":2} },)
				  errCounter := oplogSubscriptionCounter.Update( mongo.M{"counter": mongo.M{"$exists": true}, } , mongo.M{"$inc"   : mongo.M{"counter": 1} },)
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
	someSecsEarlier := time.Now().Unix() - int64(secondsDefer) - int64(secondsDeferTailCursor)

	// make a mongo/bson timestamp from the unix timestamp
	//		according to http://docs.mongodb.org/manual/core/document/
	var pow32 int64 = someSecsEarlier << 32
	mongoSecsEarlier = mongo.Timestamp(pow32)

	cursor, err := oplog.Find( mongo.M{"ts": mongo.M{ "$gte":mongoSecsEarlier}  }  ).Tailable(true).AwaitData(true).Sort( mongo.D{{"$natural", 1}} ).Cursor()


	//fmt.Println(  " ts1 = Math.round( new Date().getTime()/1000) -300;" )
	fmt.Println(  "ts2 = new Timestamp(",someSecsEarlier,", 0);" )
	fmt.Println(  "db.getSiblingDB('local').oplog.rs.find({'ts': { '$gte': ts2 }  }, {ts:1,op:1}  ).sort( {\"$natural\": 1} ) " )


	 	  
	// .addOption(DBQuery.Option.tailable).addOption(DBQuery.Option.awaitData)
	if err != nil {
		log.Println(   fmt.Sprint( "mongo oplog find error: ", err,"\n") )		
		log.Fatal(err)
	}

	log.Println( " ... tailable cursor retrieved. Id ", cursor.GetId() )	
	return cursor

}



func initChangeLog(conn mongo.Conn) (mongo.Collection, mongo.Collection){
	
	// create a capped collection if it is empty
	colChangeLog := getCollection( conn, changelogDb, changelogCol  )
	colChangelogCounter := getCollection( conn, changelogDb, counterChangeLogCol)

	n, _ := colChangeLog.Find(nil).Count()
	if n>0   {
		log.Println("capped oplog ",changelogPath,"already exists. Entries: ", n)
	} else {
		dbChangeLog  := mongo.Database{conn, changelogDb, mongo.DefaultLastErrorCmd}	// get a database object
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
	errCounter := colChangelogCounter.Upsert( mongo.M{"counter": mongo.M{"$exists": true}, } , mongo.M{"counter": 1}  ,)
	if errCounter != nil {
		log.Fatal("lf11 ",errCounter)
	}
	return colChangeLog, colChangelogCounter
	
}


func startTimerLog(){
	
	const intervalTimer = 500
	
	const layout2 = "15:04 05"
	const layout3 = " 05.0 "
	// http://digital.ni.com/public.nsf/allkb/A98D197224CB83B486256BC100765C4B

	timeStart := time.Now()
	log.Print( timeStart.Format( layout3 ) )
	ctick := time.Tick(intervalTimer * time.Millisecond)

	i := int64(0)



	go func() { 
		for now := range ctick {

			if i % 40 == 0 {
				fmt.Printf("\n")				
				fmt.Printf("\n%10s%10s%10s%10s","seq_rd","insert","tail","lag")
				fmt.Printf("\n================================================")
			}

			fmt.Print("\n")
			writeLoadReadInfo()
			writeTailInfo(now,timeStart, float64(intervalTimer))
			
			if i % 5 == 0 {
				// db.getSiblingDB("local").oplog.rs.find({},{o:0}).sort({"$natural":-1})			

/*
				var m3 mongo.M
				var err error

				conn := getConn()
				defer conn.Close()
				oplog := getOplogCollection( conn, "", true)				

				err = oplog.Find( mongo.M{} ).Sort( mongo.D{{"$natural", -1}} ).One(&m3)
				if err != nil  && err != mongo.Done {
					log.Fatal("query last oplog entry error:",err)
				}

				newestOplog,ok := m3["ts"].(mongo.Timestamp)
				if ! ok {
					fmt.Println("oplog did not contain a newest entry")				
				}
				
				secsNewestOplog := int64(newestOplog)
				secsNewestOplog  = secsNewestOplog >> 32
				
				lastTailedOplog := int64(mongoSecsEarlier) >> 32
				fmt.Printf("%10v",secsNewestOplog - lastTailedOplog )			

*/
				lag :=  updLoadTime( 0 ,0 )	
				fmt.Printf("%10v",lag)		
				


			} else {
				fmt.Printf("%10s","-")			
			}
			
			
			i++

		}
	}()
	
	
}


func recoverTailableCursor() mongo.Cursor {
	
	
		//log.Println(   "Trying to recover: " )	
		conn := getConn()
		oplog  := getOplogCollection(conn, "", false)
		//log.Println(   "Oplog retrieved " )	
		c := getTailableCursor(oplog)

		return c

	
}



func writeLoadReadInfo() {
	


		arrayRead, ok := (<- chr)
		if ok {
			sum := int64(0)
			for k,v := range arrayRead {
				sum += int64(v)
				arrayReadTotal[k] += int64(v)
			}
			sum *= readBatchSize
			/*
			if sum > 0 {
				fmt.Printf("r%v ",sum)			
			}
			*/		
			fmt.Printf("%10v",sum)			
			arrayReadNew := make( []int64, LOAD_THREADS )
			chr <- arrayReadNew
	
		} else {
			log.Fatal("error reading from chr 3")
		}



		arrayLoad, ok := (<- chl)
		if ok {
			sum := int64(0)
			for k,v := range arrayLoad {
				sum += int64(v)
				arrayLoadTotal[k] += int64(v)
			}
			/*
			if sum > 0 {
				fmt.Printf("l%v ",sum)			
			}
			*/
			fmt.Printf("%10v",sum)			
			arrayLoadNew := make( []int64, LOAD_THREADS )
			chl <- arrayLoadNew
	
		} else {
			log.Fatal("error reading from chl 3")
		}

	
}


func writeTailInfo(now time.Time, timeStart time.Time, intervalTimer float64){

		cntTail,_ := (<- cht) 
		cht <- 0
		
		//strSeconds := fmt.Sprint( now.Sub(timeStart).Seconds() )

		tailTotal += cntTail

		perSec := float64(cntTail) * intervalTimer / 1000
		perSec  = math.Trunc( 10* perSec) / 10
		
		/*
		if perSec < 1 {
			fmt.Print( "|" )							
		} else {
			//fmt.Printf( " -%v %v- ",perSec,cntTail )			
			fmt.Printf( "t%v ",perSec )			
		}
		*/
		fmt.Printf( "%10v",perSec )			
	
	
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



func incReadCounter(i int64, idxThread int){

		const chunkSize = 100
		if (i+1) % chunkSize == 0 {
			arrayRead, ok := (<- chr)
			if ok {
				arrayRead[idxThread] += chunkSize
				chr <- arrayRead
			} else {
				log.Fatal("error reading from chr 2")
			}
		}
	
}

func incLoadCounter(i int64, idxThread int){
	
		const chunkSize = 100
		if (i+1) % chunkSize == 0 {
			arrayLoad, ok := (<- chl)
			if ok {
				arrayLoad[idxThread] += chunkSize
				chl <- arrayLoad
			} else {
				log.Fatal("error reading from chl 2")
			}
		}
	
}



func updLoadTime(newSaveTime int64,  timeSaveOp int64)  int64{

	if(newSaveTime > 1){
	  atomic.StoreInt64( &lastSaveOperation, newSaveTime, )
	  //fmt.Print(" save:" , atomic.LoadInt64(&lastSaveOperation) )
	}

	if(timeSaveOp > 1){
	  atomic.StoreInt64( &timeXSaveOp, timeSaveOp, )
	  //fmt.Print(" tail:" , atomic.LoadInt64(&timeXSaveOp) )
	}


  timeSaveOp =  atomic.LoadInt64(&lastSaveOperation)
  //lastTail :=  atomic.LoadInt64(&timeXSaveOp)

	
	return ( time.Now().Unix() - timeSaveOp)
	
}



func finalReport() (x int64, y int64){

	var loadTotal = int64(0)
	var readTotal = int64(0)
	

	arrayLoad, ok1 := (<- chl)
	if ok1 {
		for k,_ := range arrayLoad {
			v2 := arrayLoadTotal[k]
			loadTotal += v2
			log.Printf("thread %v - load ops %v - ", k , v2)
		}
	} else {
		log.Fatal("error reading from chl 1")
	}


	arrayRead, ok2 := (<- chr)
	if ok2 {
		for k,_ := range arrayRead {
			v2 := arrayReadTotal[k]
			v2 *= readBatchSize
			readTotal += v2
			log.Printf("thread %v - read ops %v - ", k , v2)
		}
	} else {
		log.Fatal("error reading from chr 1")
	}

	return loadTotal, readTotal 
	 
	
}


func getConn() mongo.Conn {

	conn, err := mongo.Dial("localhost:27001")
	if err != nil {
		log.Println("getConn failed")
		log.Fatal(err)
	}
	return conn

}


func getCollection(conn mongo.Conn, nameDb string, nameCol string  )(col mongo.Collection){
	
		tmpDb  := mongo.Database{conn, nameDb, mongo.DefaultLastErrorCmd}	// get a database object
		col    = tmpDb.C(nameCol)  
		return
	
}

func getOplogCollection(conn mongo.Conn, colName string, silent bool) mongo.Collection {
	
	if colName == "" {
		colName = 	"oplog.rs"
	}
	
	if silent {
	} else {
		log.Print(  fmt.Sprint("\n\n==(re-)connect_to_",colName,"== ... ") )
	}
	dbOplog  := mongo.Database{conn, "local", mongo.DefaultLastErrorCmd}	// get a database object
	oplog    := dbOplog.C( colName )  // get collection
	
	return oplog

}

/*
	partitioning data, so that reads can start at different partitions

*/
func getFuncPartitionStart() func(threadIdx int) (x,y mongo.ObjectId){

		conn := getConn()
		defer conn.Close()
		colOffers := getCollection(conn,changelogDb,offers)
		
		

		// because auto-oids of mongo contain a timestamp, we are lucky 
		// to postulate the minimum and maximum possible oids
		minOidPostulated := mongo.MinObjectIdForTime( time.Date(1999, time.November,  1, 02, 01, 0, 222, time.Local))
		maxOidPostulated := mongo.MinObjectIdForTime( time.Date(2030, time.December, 31, 23, 59, 0, 222, time.Local))
		if minOidPostulated > maxOidPostulated {
			log.Fatal( "minOidPostulated must be smaller than maxOidPostulated", minOidPostulated , maxOidPostulated )
		}

		// with their help, we query the -real- minOid and maxOid of the current data set
		var m1,m2 mongo.M
		var err error
	  err = colOffers.Find(mongo.M{"_id": mongo.M{"$gte": minOidPostulated,},}).Fields(mongo.M{"_id": 1}).
	  	Skip(0).Sort( mongo.M{"_id":  1,}, ).Limit(1).One(&m1)
		if err != nil  && err != mongo.Done {
			log.Fatal("query min error:",err)
		}

	  err = colOffers.Find(mongo.M{"_id": mongo.M{"$lte": maxOidPostulated,},}).Fields(mongo.M{"_id": 1}).
	  	Skip(0).Sort( mongo.M{"_id": -1,}, ).Limit(1).One(&m2)
		if err != nil  && err != mongo.Done {
			log.Fatal("query max error:",err)
		}
	
		oidMin,ok := m1["_id"].(mongo.ObjectId)
		if ! ok {
			
			log.Print("min did not contain an OID - using a default ")				
			oidMin = mongo.MinObjectIdForTime( time.Date(1999, time.November,  1, 02, 01, 0, 222, time.Local))
			
		}
		oidMax,ok := m2["_id"].(mongo.ObjectId)
		if ! ok {
			log.Fatal("max did not contain an OID")				
		}

		/* Now we could either partition by record -count-
				which brings different -seek- times 
				to find the 1 ...10 millonth record
				
			Or we could stupidly partition by seconds passed
			assuming evenly distributed insertion times
			which is imperfect but good enough,
			as we loop-wrap anyway
		*/

		ctMin, ctMax := oidMin.CreationTime(), oidMax.CreationTime()
		diffTime := ctMax.Sub(ctMin)


		return func(threadIdx int)(minOid,minOidThreadPartition mongo.ObjectId) {

			partitionTimeDiff := time.Duration(threadIdx)*diffTime / time.Duration(LOAD_THREADS)
			//fmt.Println("diff",diffTime, partitionTimeDiff)				
			timeMinThread := ctMin.Add( partitionTimeDiff )
			minOidThread  := mongo.MinObjectIdForTime( timeMinThread )

			//const layout2 = "01-02 15:04 05"
			//fmt.Printf("threadIndex: %v, oid-min %v threadStart %v oid-max %v \n",threadIdx,oidMin.CreationTime().Format(layout2),minOidThread.CreationTime().Format(layout2),oidMax.CreationTime().Format(layout2) )
			return oidMin,minOidThread

		}

}


func handler(w http.ResponseWriter, r *http.Request) {
	
  //fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
  
  path1 := r.URL.Path[1:]
  if ! strings.HasPrefix( path1, "start" ){
  	fmt.Fprintf(w, "==================================================\n")
  	fmt.Fprintf(w, "use /start to begin \n")
  	return
  }
  
  fmt.Fprintf(w, "starting ...\n")
  fmt.Fprintf(w, strings.Repeat("     ",9000))
  fmt.Fprintf(w, "\n")




	conn := getConn()
	defer conn.Close()

	// Wrap connection with logger so that we can view the traffic to and from the server.
	// conn = mongo.NewLoggingConn(conn, log.New(os.Stdout, "", 0), "")

	// Clear the log prefix for more readable output.
	log.SetFlags(0)

	//clearAll(conn)


	startTimerLog()

	colChangeLog, colCounterChangeLog := initChangeLog(conn)

	go iterateTailCursor(colChangeLog, colCounterChangeLog)


	time.Sleep( 200 * time.Millisecond )
	
	//loadRead :=  getFuncLoadRead()
	
	for i:= 0; i< LOAD_THREADS; i++ {
		batchStamp := int64(time.Now().Unix() )<<32  +  int64(i)*insertsPerThread
		go loadInsert( i, batchStamp)
		//go loadRead(   i, batchStamp)
		
	}


	
	arrayLoad := make( []int64, LOAD_THREADS )
	chl <- arrayLoad

	arrayRead := make( []int64, LOAD_THREADS )
	chr <- arrayRead

	cht <- int64(0)
	
	
	x := <- cq
	log.Println("quit signal received: ", x)


	tsFinish := time.Now().Unix()
	elapsed  := (tsFinish-tsStart)
	log.Println("tsFinish: ",tsFinish, " Dauer: " , elapsed )


	loadTotal,readTotal :=  finalReport()

	var percentage float64 = float64(tailTotal)/float64(loadTotal)
	percentage = math.Trunc(percentage*1000)/10
	
	readPerSec :=  int64(   math.Trunc(float64(readTotal)/float64(elapsed))   )

	log.Print(     "==================================================")
	log.Printf(    "ReadTotal-Read-Loaded-Tailed: %12v - %8v - %v- %v - %v%%", readTotal,readPerSec,loadTotal,tailTotal,percentage)

	fmt.Fprintf(w, "==================================================\n")
	fmt.Fprintf(w, "ReadTotal-Read-Loaded-Tailed: %12v - %8v - %v- %v - %v%%", readTotal,readPerSec,loadTotal,tailTotal,percentage)



}

