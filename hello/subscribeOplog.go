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

var   askedCursor    int = 0
const askCursorMax   int = 4 
const secondsPast = 2

const insertThreads     = 8
const insertsPerThread  = int64(2000)  // "cursor not found"

const outputLevel = 0

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
				{"size", 1024*1024 },
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
	sixtySecondsEarlier := int32(time.Now().Unix()) - secondsPast
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

	const layout2 = "15:04 05"
	const layout3 = " 05.0 "
	// http://digital.ni.com/public.nsf/allkb/A98D197224CB83B486256BC100765C4B
	fmt.Print(time.Now().Format( layout3 ) )
	ctick := time.Tick(1 * time.Second)
	go func() { 
		for now := range ctick {
			fmt.Print(now.Format( layout3 ) )
		}
	}()

	for i:=int64(0); i< insertThreads; i++ {
		go fill( int64(time.Now().Unix() )<<32  +  i*insertsPerThread )
	}
	x := <- cq
	fmt.Println("quit signal received: ", x)

}


func fill(start int64){

	fctGetRecurseMsg := getRecurseMsg( fmt.Sprint("fill",start," "))

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
			log.Fatal(err)
		}
		fmt.Print( fctGetRecurseMsg() )

	}
	

	
}

func iterateTailCursor( c mongo.Cursor, oplogsubscription mongo.Collection ){


	fctGetRecurseMsg := getRecurseMsg("recursion ")
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
		log.Fatal(   fmt.Sprint( "mongo permanent cursor error: ", err) )
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
    		if mod := ctr % 50; mod != 0{
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


