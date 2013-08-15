package main

/*

cd /home/peter.buchmann/ws_go/src/github.com/pbberlin/g1/mongostress
export GOPATH="/home/peter.buchmann/ws_go/"


This is a go implementation of a tailable cursor against the oplog
as describe at the bottom of this document: 
	http://docs.mongodb.org/manual/tutorial/create-tailable-cursor/

It is also a Mongo Load Tester
	


Todo
	tailCursorLogInc( ... ,newTimeOplog )
		=> make an array of shards for each newTimeOplog-Datum
		=> compute the average newTimeOplog across all shards
		=> update the damn thing only every 100 call




*/

import (
	"github.com/garyburd/go-mongo/mongo"
	"log"
	"time"
	"math"
	"strings"
	"fmt"
  "net/http"
  "html/template"
	"sync/atomic"
	"os/exec"
	"os"
	"bytes" 
	"strconv" 
	"errors"
	"encoding/json"	
	"regexp"
	"code.google.com/p/gcfg"
 	"math/rand"	
 	"flag"
 	"runtime/pprof"
 	"runtime/debug"
	"crypto/md5" 	
	"io"
)





var   countNoNextValue  int = 0
const noNextValueMax	  int = 122		// if the tailing cursor is exhausted this many times, we QUIT the app

const secondsDefer = 4							// upon cursor not found error - amount of sleep  - 
const secondsDeferTailCursor = 1		// after sleep - set back additional x seconds


var LOADER_COUNTER  = int32(0)
var INSERTERS_CONC_MAX=	int32(0)
var ARR_INSERT_TOT = make( []int64 ,INSERTERS_CONC_MAX )
var ARR_INSERT_CUR = make( []int64, INSERTERS_CONC_MAX )
var chl chan []int64 = make(chan []int64 ,1)      // sync channel load



var READER_COUNTER	  = int32(0)
var READERS_CONC_MAX	= int32(1)
var ARR_READ_TOT = make([]int64 ,READERS_CONC_MAX)
var ARR_READ_CUR = make([]int64 ,READERS_CONC_MAX)
var chr chan []int64 = make(chan []int64 ,1)      // sync channel read



var UPDATER_COUNTER	  = int32(0)
var UPDATERS_CONC_MAX	= int32(1)
var ARR_UPDATE_TOT = make([]int64 ,UPDATERS_CONC_MAX)
var ARR_UPDATE_CUR = make([]int64 ,UPDATERS_CONC_MAX)
var chu chan []int64 = make(chan []int64 ,1)      // sync channel UPDATE




const offers = "offers.test"
const changelogCol string = "oplog.subscription"
const counterChangeLogCol string = "oplog.subscription.counter"

var nsToTrack string 
var changelogFullPath string 


const shardErr = "can't use 'local' database through mongos"
const writebackErr = "writeback waitfor for older id"




var 	oplogAccessible bool = true




var   mongoSecsEarlier mongo.Timestamp = mongo.Timestamp(5898548092499667758)	// limit timestamp

var outputLevel int = 0


const readBatchSize  = 100
const updateBatchSize= 100
const insertsPerThread  = int64(400)  // if oplog is not big enough, causes "cursor not found"


var cht chan int64   = make(chan   int64 ,1)	    // channel cursor tail
var tailTotal = int64(0)

var cq  chan int	 = make(chan   int   )		      // channel quit


var	tStart  time.Time  = time.Now()
var	tsStart int64	     = tStart.Unix()


var timeLastOplogOperation  int64 = time.Now().Unix()
var timeLastSaveOperation   int64 = time.Now().Unix()
const sizeLagReport = 4
var lv []int64 = make( []int64, sizeLagReport )




var csvRecord map[string]int64 = make(map[string]int64)
var singleInstanceRunning bool = false


var templates = template.Must( template.ParseFiles("main_header.html", "main_body.html",
 "main_body_chart.html",
 "main_footer.html") )
var httpParamValidator = regexp.MustCompile("^[a-zA-Z0-9/]+$")


type Config struct {
        Main struct {
                //ConnectionString string
                Host string
                Port string
                DatabaseName string
                DbUsername string
                DbPassword string
                DbAdminUsername string
                DbAdminPassword string
        }
}
var CFG Config

var SHARDS map[string]map[string]string  = make( map[string]map[string]string )

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

var updateSecondaryIndize int32 = 0



func main() {

    flag.Parse()
    if *cpuprofile != "" {
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal(err)
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }



	rand.Seed(time.Now().UnixNano()) 
	
	errCfg := loadConfig()
	if errCfg != nil {
		log.Fatal("config load err:", errCfg)
	}
	changelogFullPath = fmt.Sprint( CFG.Main.DatabaseName , "." , changelogCol )
	nsToTrack = fmt.Sprint( CFG.Main.DatabaseName , "." , offers )	
	
	
	

	
	printHelperCommands()
	
	http.HandleFunc("/"	  , elseHandler)
  http.HandleFunc("/data/" , dataHandler)
  http.HandleFunc("/start/", subClassOfHandlerFunc(startHandler) )
  http.HandleFunc("/stop/" , stopHandler)
  http.HandleFunc("/tpl/"  , subClassOfHandlerFunc(tplHandler) )
  http.HandleFunc("/changeLoadThreads/" , subClassOfHandlerFunc(changeLoadThreads))
  http.HandleFunc("/changeReadThreads/" , subClassOfHandlerFunc(changeReadThreads))
  http.HandleFunc("/changeUpdateThreads/" , subClassOfHandlerFunc(changeUpdateThreads))
  http.HandleFunc("/getConfigInfo/" , subClassOfHandlerFunc(getConfigInfo))
  http.HandleFunc("/toggleSecondaryIndize/" , subClassOfHandlerFunc(toggleSecondaryIndize))
  http.HandleFunc("/reloadCfg/" , subClassOfHandlerFunc(reloadCfg))
  
  
  portStart := 8080
  //panic(http.ListenAndServe(":8080", http.FileServer(http.Dir("/home/peter.buchmann/ws_go/src/github.com/pbberlin/g1/mongostress"))))
  for {
  	fmt.Println("  about to start listening at port: ", portStart)  	
	  errL := http.ListenAndServe( fmt.Sprint(":", portStart), nil)
	  if errL != nil {
	  	fmt.Println("       http server error ", errL)  	
	  } else {
	  	break	
	  }
	  portStart++
  }


}

/* challenging construct from http://golang.org/doc/articles/wiki/
		essentially "subclassing" an ordinary http handler function
		- enhancing it with functionality, common to all handlers
		- RETURNING normal http.HandlerFunc
		- but CALLING extended own handler func
		
		TODO: lenPath needs to be made dynamic
*/
func subClassOfHandlerFunc( fn func(http.ResponseWriter, *http.Request, string)  ) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		
		const lenPath = len("/start/")
	  params := r.URL.Path[lenPath:]
	  
		if !httpParamValidator.MatchString( params ){
			http.NotFound(w, r)
			err := errors.New( fmt.Sprint("invalid http param", r.URL.Path)  )
			p2(w,"%v",err)
			return 
		}
		fn(w, r, params)
	}
	
}

func elseHandler(w http.ResponseWriter, r *http.Request) {
	
  path1 := r.URL.Path[1:]

	validCommands := map[string]string{ 
		"start": "start" ,
		"stop":  "stop" ,
		"tpl":   "tpl" ,
		"data":  "data" ,
		"changeLoadThreads": "changeLoadThreads",
		"changeReadThreads": "changeReadThreads",
		"changeUpdateThreads": "changeUpdateThreads",
		"getConfigInfo": "getConfigInfo",
		"toggleSecondaryIndize": "toggleSecondaryIndize",
		"reloadCfg": "reloadCfg",
		
		"command-without-handler":  "bla" ,
		
	}

	msgCommands := ""
  var isCommand bool = false
  
  for k,_ := range validCommands {
	  if strings.HasPrefix( path1, k )  {
	  	isCommand = true
	  }
	  msgCommands = fmt.Sprint( msgCommands, k , " ")
  }
  
  if ! isCommand {
  	fmt.Fprintf(w, "use commands %vto begin \n",msgCommands)	
  	return
  } else {
  	fmt.Fprintf(w, "valid command \"%v\" \n- but no handler available\n",path1)	
  }
	http.Redirect(w, r, "/tpl/", http.StatusFound)
  
}



func stopHandler(w http.ResponseWriter, r *http.Request) {
  	p2( w, "\nreceived quit signal by browser: %v", 1)
  	Flush1(w)

		log.Println(" sending quit signal: ", 1)
		cq <- 1
  	time.Sleep( 50 * time.Millisecond )
  	
  	os.Exit(1)
}


func changeLoadThreads(w http.ResponseWriter, r *http.Request, dummy string) {


	const lenPath = len("/changeLoadThreads/")
  params := r.URL.Path[lenPath:]
	
	newLoadersConcMax,err := strconv.ParseInt( params,10,32 )
	if err != nil {
  	p2( w, "could not parse '%v'- no change<br>\n", params)
	} else {

  	p2( w, "changeLoadThreads switched from %v to %v \n", INSERTERS_CONC_MAX, newLoadersConcMax)

		// loading not started yet
		if !singleInstanceRunning {
	  	p2( w, "<br>changing load threads while not running \n" )
			INSERTERS_CONC_MAX=	int32(newLoadersConcMax)
			ARR_INSERT_TOT = make( []int64 , newLoadersConcMax )
			ARR_INSERT_CUR = make( []int64 , newLoadersConcMax )
			return
		}

		// loading already started - carefully intervene by blocking the channel
		if( int32(newLoadersConcMax) > INSERTERS_CONC_MAX ){
			ARR_INSERT_CUR, ok := (<- chl)
	  	//p2( w, "cur len %v \n", len(ARR_INSERT_CUR) )
			if ok {
				ARR_INSERT_TOT = make( []int64 , newLoadersConcMax )
				tmp := make( []int64 , newLoadersConcMax )
				copy(tmp,ARR_INSERT_CUR)
				ARR_INSERT_CUR = tmp
			} else {
				log.Fatal("error reading from chl 4")
			}
	  	//p2( w, "new len %v \n", len(ARR_INSERT_CUR) )
			INSERTERS_CONC_MAX=	int32(newLoadersConcMax)
			chl <- ARR_INSERT_CUR				
			
		} else {
			ARR_INSERT_CUR, _ := (<- chl)
			INSERTERS_CONC_MAX=	int32(newLoadersConcMax)
			chl <- ARR_INSERT_CUR				
		}

	}
  	
}


func changeReadThreads(w http.ResponseWriter, r *http.Request, dummy string) {


	const lenPath = len("/changeReadThreads/")
  params := r.URL.Path[lenPath:]
	
	newReadersConcMax,err := strconv.ParseInt( params,10,32 )
	if err != nil {
  	p2( w, "could not parse '%v'- no change<br>\n", params)
	} else {

  	p2( w, "changeReadThreads switched from %v to %v \n", READERS_CONC_MAX, newReadersConcMax)

		// reading not started yet
		if !singleInstanceRunning {
	  	p2( w, "<br>changing read threads while not running \n" )
			READERS_CONC_MAX=	int32(newReadersConcMax)
			ARR_READ_TOT = make( []int64 , newReadersConcMax )
			ARR_READ_CUR = make( []int64 , newReadersConcMax )
			return
		}

		// reading already started - carefully intervene by blocking the channel
		if( int32(newReadersConcMax) > READERS_CONC_MAX ){
			ARR_READ_CUR, ok := (<- chr)
	  	//p2( w, "cur len %v \n", len(ARR_READ_CUR) )
			if ok {
				ARR_READ_TOT = make( []int64 , newReadersConcMax )
				tmp := make( []int64 , newReadersConcMax )
				copy(tmp,ARR_READ_CUR)
				ARR_READ_CUR = tmp
			} else {
				log.Fatal("error reading from chr 4")
			}
	  	//p2( w, "new len %v \n", len(ARR_READ_CUR) )
			READERS_CONC_MAX=	int32(newReadersConcMax)
			chr <- ARR_READ_CUR				
			
		} else {
			ARR_READ_CUR, _ := (<- chr)
			READERS_CONC_MAX=	int32(newReadersConcMax)
			chr <- ARR_READ_CUR				
		}

	}
  	
}



func changeUpdateThreads(w http.ResponseWriter, r *http.Request, dummy string) {


	const lenPath = len("/changeUpdateThreads/")
  params := r.URL.Path[lenPath:]
	
	newUpdateersConcMax,err := strconv.ParseInt( params,10,32 )
	if err != nil {
  	p2( w, "could not parse '%v'- no change<br>\n", params)
	} else {

  	p2( w, "changeUpdateThreads switched from %v to %v \n", UPDATERS_CONC_MAX, newUpdateersConcMax)

		// Updateing not started yet
		if !singleInstanceRunning {
	  	p2( w, "<br>changing Update Threads while not running \n" )
			UPDATERS_CONC_MAX=	int32(newUpdateersConcMax)
			ARR_UPDATE_TOT = make( []int64 , newUpdateersConcMax )
			ARR_UPDATE_CUR = make( []int64 , newUpdateersConcMax )
			return
		}

		// Updateing alUpdatey started - carefully intervene by blocking the channel
		if( int32(newUpdateersConcMax) > UPDATERS_CONC_MAX ){
			ARR_UPDATE_CUR, ok := (<- chu)
	  	//p2( w, "cur len %v \n", len(ARR_UPDATE_CUR) )
			if ok {
				ARR_UPDATE_TOT = make( []int64 , newUpdateersConcMax )
				tmp := make( []int64 , newUpdateersConcMax )
				copy(tmp,ARR_UPDATE_CUR)
				ARR_UPDATE_CUR = tmp
			} else {
				log.Fatal("error Updateing from chu 4")
			}
	  	//p2( w, "new len %v \n", len(ARR_UPDATE_CUR) )
			UPDATERS_CONC_MAX=	int32(newUpdateersConcMax)
			chu <- ARR_UPDATE_CUR				
			
		} else {
			ARR_UPDATE_CUR, _ := (<- chu)
			UPDATERS_CONC_MAX=	int32(newUpdateersConcMax)
			chu <- ARR_UPDATE_CUR				
		}

	}
  	
}


/* sending current csv column as JSON to client */
func dataHandler(w http.ResponseWriter, r *http.Request) {


 	arrByte,err := json.Marshal( csvRecord ) 
 	if err != nil {
		p2(w,"Marshal Map to Json - %v",err) 		
 	} else {
  	w.Header().Set("Content-type:", "application/json")
  	w.Write(arrByte)
 	}
 	
}


func getConfigInfo(w http.ResponseWriter, r *http.Request, params string) {

	var mapCounts map[string]int32 = make(map[string]int32)
	mapCounts["inpLoadThreads"] = INSERTERS_CONC_MAX
	mapCounts["inpReadThreads"] = READERS_CONC_MAX
	mapCounts["inpUpdateThreads"] = UPDATERS_CONC_MAX
	mapCounts["inpUpdateSecondaryIndize"] = int32(updateSecondaryIndize)
 	arrByte,err := json.Marshal( mapCounts ) 
 	if err != nil {
		p2(w,"Marshal Map to Json - %v",err) 		
 	} else {
  	w.Header().Set("Content-type:", "application/json")
  	w.Write(arrByte)
 	}

 	
}


func toggleSecondaryIndize(w http.ResponseWriter, r *http.Request, params string) {
	
	if  updateSecondaryIndize > 0 {
		updateSecondaryIndize = 0	
	} else {
		updateSecondaryIndize = 1
	}
 	p2( w, "changing updateSecondaryIndize to %v\n", updateSecondaryIndize )
	
	//getConfigInfo(w , r , params )	
	
}

func reloadCfg(w http.ResponseWriter, r *http.Request, params string) {

	err := loadConfig()
 	if err != nil {
		p2(w,"cfg reload failed %v",err) 		
 	} else {
		p2(w,"cfg loaded") 		
 	}

 	
}


func tplHandler(w http.ResponseWriter, r *http.Request, params string) {
	
	
	c := map[string]string{
		"Title": fmt.Sprint("Mongo Load"," ",CFG.Main.Host,":",CFG.Main.Port, " - ", CFG.Main.DatabaseName ),
	  "Body" :"body msg test",
	}
	renderTemplatePrecompile( w ,"main_header", c )	
	//renderTemplatePrecompile( w ,"main_body", c )	
	renderTemplateNewCompile( w ,"main_body_chart", c )	
	renderTemplatePrecompile( w ,"main_footer", c )	

}


func startHandler(w http.ResponseWriter, r *http.Request, params string) {

	c := map[string]string{
		"Title":"Doing load",
	  "Body" :  fmt.Sprintf("starting ... (%v)\n", params),
	}
	
	if( singleInstanceRunning ){
		c = map[string]string{
		  "Body" : fmt.Sprintf("already running ... (%v)\n", params),
		}
		renderTemplatePrecompile( w ,"main_header", c )		
		renderTemplatePrecompile( w ,"main_body"  , c )	
		renderTemplatePrecompile( w ,"main_footer", c )	
		fmt.Println("already one instance running")
		return
	} else {
		singleInstanceRunning = true
	}


	conn := getConn()
	time.Sleep( 100 * time.Millisecond )
	checkAccessAndShardConfig(conn)
	setupAndInitDestinationCollections(conn)
	conn.Close()


	// Wrap connection with logger so that we can view the traffic to and from the server.
	// conn = mongo.NewLoggingConn(conn, log.New(os.Stdout, "", 0), "")

	// Clear the log prefix for more readable output.
	log.SetFlags(0)
	//cleanUpPreviousData(conn)

	startTimerLog()


	for k,_ := range SHARDS {
		go iterateTailCursor(SHARDS[k])		
	}
	
	
	time.Sleep( 100 * time.Millisecond )


	go spawnInserts()

	go spawnReads()

	go spawnUpdates()




	// no throwing the "syncing" balls onto the field:
	chl <- ARR_INSERT_CUR

	chr <- ARR_READ_CUR

	chu <- ARR_UPDATE_CUR

	cht <- int64(0)


	renderTemplateNewCompile( w ,"main_body_chart", c )	
	Flush1(w)
	
	
	// the tailing cursor and stopHandlers may send a quit signal via cq
	x := <- cq
	log.Println("quit signal received: ", x)


	tsFinish := time.Now().Unix()
	elapsed  := (tsFinish-tsStart)
	log.Println("tsFinish: ",tsFinish, " Dauer: " , elapsed )
	readTotal, insertTotal,updateTotal :=  finalReport()
	var percentage float64 = float64(tailTotal)/float64(insertTotal+updateTotal)
	percentage = math.Trunc(percentage*1000)/10
	readPerSec   :=  int64(   math.Trunc(float64(readTotal)  /float64(elapsed))   )
	insertUpdatePerSec :=  int64(   math.Trunc(float64(insertTotal+updateTotal)/float64(elapsed))   )
	c["Body"] = "==================================================<br>\n"
	c["Body1"] = fmt.Sprintf( "readpersec %8v - insert+updatepersec %8v \n"  , readPerSec,insertUpdatePerSec) 
	c["Body2"] = fmt.Sprintf( "total insertupate %v tailed %v -percent %v\n", insertTotal+updateTotal,tailTotal,percentage) 

	p2( w, c["Body1"] )
	p2( w, c["Body2"] )

	singleInstanceRunning = false

}


func spawnInserts(){

	monotonicInc := int32(0)
	for {
		
		lc := atomic.LoadInt32( &LOADER_COUNTER )
		if lc > INSERTERS_CONC_MAX-1 {
			time.Sleep( 500 * time.Millisecond )
			continue
		}
		

		atomic.AddInt32( &LOADER_COUNTER, 1, )
		batchStamp := int64(time.Now().Unix() )<<32  +  int64(monotonicInc)*insertsPerThread
		go loadInsert( lc, batchStamp)
		monotonicInc++
	}

}


func spawnReads(){

	loadRead :=  funcLoadRead()

	monotonicInc := int32(0)
	for {
		
		lc := atomic.LoadInt32( &READER_COUNTER )
		if lc > READERS_CONC_MAX-1 {
			time.Sleep( 500 * time.Millisecond )
			continue
		}
		

		atomic.AddInt32( &READER_COUNTER, 1, )
		go loadRead( lc )
		monotonicInc++
	}

}



func spawnUpdates(){


	monotonicInc := int32(0)
	for {
		
		lc := atomic.LoadInt32( &UPDATER_COUNTER )
		if lc > UPDATERS_CONC_MAX-1 {
			time.Sleep( 500 * time.Millisecond )
			continue
		}
		

		atomic.AddInt32( &UPDATER_COUNTER, 1, )
		go loadUpdate( lc )
		monotonicInc++
	}

}



func x1___mongo_access________(){}


func getConn() mongo.Conn {

	conn, err := mongo.Dial( fmt.Sprint( CFG.Main.Host, ":", CFG.Main.Port ) )	
	if err != nil {
		log.Fatal("getConn1 failed", err)
	}
	return conn

}


func checkAccessAndShardConfig(conn mongo.Conn) {

	var success bool

	success = checkDbAccess(conn,CFG.Main.DatabaseName,CFG.Main.DbUsername, CFG.Main.DbPassword)
	if ! success {
		log.Fatal("no access to main database.")
	}



	success = checkDbAccess(conn,"admin",CFG.Main.DbAdminUsername, CFG.Main.DbAdminPassword)
	if ! success {
		log.Fatal("no access to admin database.")
	}



	oplogAccessible = checkDbAccess(conn,"local",CFG.Main.DbUsername, CFG.Main.DbPassword)
	if ! oplogAccessible {
		var m mongo.M
		dbAdmin   := mongo.Database{conn, "admin", mongo.DefaultLastErrorCmd}	
	  errAccess := dbAdmin.Run(mongo.D{{"listShards", 1}}, &m)	
		if errAccess != nil {
			log.Println("listing of shards", errAccess )
		} else {
			aShards, isArray :=  m["shards"].(  []interface{} )
			//fmt.Printf("\n%#v\n",m["shards"])
			if isArray {
				for _,v := range aShards {
					//fmt.Printf("key: %v - %#v\n",k,v)
					mShard, isMap :=  v.(  map[string]interface{} )
					if isMap  {
						/*
						for k1,v1 := range mShard {
							fmt.Printf("key: %#v - %#v\n",k1,v1)
						}
						*/
						shardId, ok1 := mShard["_id"].(string)
						hostString, ok2 := mShard["host"].(string)
						rsName := ""
						ipAddress  := ""
						portNumber := ""
						if ok1 && ok2 {
							//fmt.Printf("shardId: %#v - hostString: %#v\n",shardId,hostString)	
							aHost := strings.Split(hostString, "/")
							//fmt.Printf("%#v \n",aHost)
							rsName     = aHost[0]
							
							rsMembers := aHost[1]
							firstMember := strings.Split(rsMembers, ",")

							
							aAddressPlusPort := strings.Split(firstMember[0], ":")
							ipAddress  = aAddressPlusPort[0]
							if len(ipAddress) > 1 {
								portNumber  = aAddressPlusPort[1]	
							}
							
							mainKey := fmt.Sprint( shardId, " :: ", rsName, " :: ",ipAddress," :: ", portNumber)	
							//fmt.Println(mainKey)	
							var tmpMap map[string]string = make( map[string]string )
							tmpMap["shardId"]   = shardId
							tmpMap["rsName"]    = rsName
							tmpMap["ipAddress"]  = ipAddress
							tmpMap["portNumber"] = portNumber
							SHARDS[mainKey] = tmpMap
							
						} else {
							fmt.Println(" mShard[\"_id\"] or mShard[\"host\"] not parseable")							
						}
						
					} else {
						fmt.Println("conversion of shard map failed")	
					}
				}
			} else {
				fmt.Println("conversion of array of shards failed")	
			}
		}

		
		if len(SHARDS) > 0 {
			oplogAccessible = true	
		}
		
	} else {

		mainKey := fmt.Sprint( "no_shards", "::", "rs_nomatterwhat", "::",CFG.Main.Host,"::", CFG.Main.Port)	
		//fmt.Println(mainKey)	
		var tmpMap map[string]string = make( map[string]string )
		tmpMap["shardId"]   = "no_shards"
		tmpMap["rsName"]    = "i_am_primary"
		tmpMap["ipAddress"]  = CFG.Main.Host
		tmpMap["portNumber"] = CFG.Main.Port
		SHARDS[mainKey] = tmpMap
		
	}
	

	fmt.Println("Shard config is")
	for k,_ := range SHARDS {
		fmt.Printf("\t%v\n", k)
	}

	

}

/*
		checking the mere existence AND auth access
		of the required mongo dbs
		
		without auth, we run a db.stats
		with    auth, we simply take the db.Authenticate method as proof of existence and auth access
*/
func checkDbAccess( conn mongo.Conn, dbName string, username string, password string ) bool {
	
	db  := mongo.Database{conn, dbName , mongo.DefaultLastErrorCmd}	
	if len(username) > 0 {
		errAuth := db.Authenticate(username, password) 
		if errAuth != nil {
			if errAuth.Error() == shardErr {
				fmt.Println("db ",dbName, " not available via shard ", errAuth )
				return false
			} else {
				log.Println("auth for db",dbName, "failed for",username,"-", password, ":", errAuth )
				return false
			}
		} 
	}	else {
		var m mongo.M
	  errAccess := db.Run(mongo.D{{"dbStats", 1}}, &m)	
		if errAccess != nil {
			log.Println("access to db ",dbName, " failed: ", errAccess )
			return false
		}
	}

	return true
	
	
}



func getMainDBCollection(conn mongo.Conn, nameDb string, nameCol string  )(col mongo.Collection){

	tmpDb  := mongo.Database{conn, nameDb, mongo.DefaultLastErrorCmd}	// get a database object
	authenticateMainDb(tmpDb)
	col	= tmpDb.C(nameCol)  
	return
	
}


func authenticateMainDb( db mongo.Database){

		if len(CFG.Main.DbUsername) > 0 {
			errAuth := db.Authenticate(CFG.Main.DbUsername, CFG.Main.DbPassword) 
			if errAuth != nil {
				log.Println("2nd auth for workdb ",db, "failed for",CFG.Main.DbUsername, CFG.Main.DbPassword, ":", errAuth )
			} 
		}
	
}



func authenticateAdminDb( db mongo.Database){

		if len(CFG.Main.DbAdminUsername) > 0 {
			errAuth := db.Authenticate(CFG.Main.DbAdminUsername, CFG.Main.DbAdminPassword) 
			if errAuth != nil {
				log.Fatal("2nd auth for admindb ",db, " failed for ",CFG.Main.DbAdminUsername," - ", CFG.Main.DbAdminPassword, ": ", errAuth )
				
			} 
		}
	
}


func setupAndInitDestinationCollections(conn mongo.Conn) (a,b,c,d mongo.Collection){
	
	// create a capped collection if it is empty
	colChangeLog        := getMainDBCollection( conn, CFG.Main.DatabaseName, changelogCol  )
	n, _ := colChangeLog.Find(nil).Count()
	if n>0  {
		log.Println("capped oplog ",changelogFullPath,"already exists. Entries: ", n)
	} else {
		dbChangeLog  := mongo.Database{conn, CFG.Main.DatabaseName, mongo.DefaultLastErrorCmd}	// get a database object
		authenticateMainDb(dbChangeLog)
		
		errCreate := dbChangeLog.Run(
			mongo.D{
				{"create", fmt.Sprint( changelogCol ) },
				{"capped", true},
				{"size", 16 * 1024*1024 },
			},
			nil,
		)
		if errCreate != nil {
			if errCreate.Error() == "collection already exists" {
				fmt.Println("1capped oplog journal ",changelogFullPath,"already exists")			
			} else {
				log.Fatal("2capped oplog journal creation failed. err: ", errCreate)
			}
		} else {
			log.Println("3capped oplog journal ",changelogFullPath,"created")
		}
	}

	colChangelogCounter := getMainDBCollection( conn, CFG.Main.DatabaseName, counterChangeLogCol)
	errCounter := colChangelogCounter.Upsert( mongo.M{"counter": mongo.M{"$exists": true}, } , mongo.M{"counter": 0}  ,)
	if errCounter != nil {
		if errCounter.Error() == "isOk()" {
			fmt.Print("skipping strange upsert error: ",errCounter)
		} else {
			checkWriteBack("lf11 ",errCounter, "mongos")
		}
	}
	
	
	colOffersByShop        := getMainDBCollection( conn, CFG.Main.DatabaseName, "offersByShop")
	colOffersByLastUpdated := getMainDBCollection( conn, CFG.Main.DatabaseName, "offersByLastUpdated")
	// todo: if shards exists - then shard those collections...	
	
	
	return colChangeLog, colChangelogCounter, colOffersByShop, colOffersByLastUpdated
	
}


func sizesAndMemory(printDetails bool)(sizeWorkDb,sizeSumOplogs int64,  resident, virtual int, err error){

	conn := getConn()
	defer conn.Close()

	var db mongo.Database
	db = mongo.Database{conn, CFG.Main.DatabaseName, mongo.DefaultLastErrorCmd}
	authenticateMainDb(db)
	// 	err = db.Run(D{{"drop", collectionName}}, nil)
	var m mongo.M
	
	err = db.Run(mongo.D{{"buildInfo", 1}}, &m)
	if err != nil {
		log.Fatal("runcommand buildInfo failed: ", err)
	}
	//fmt.Println(m)	
	//fmt.Println("version: ", m["version"])	


  //err = db.Run(mongo.D{{"dbStats", 1}}, &m)

	db = mongo.Database{conn, CFG.Main.DatabaseName, mongo.DefaultLastErrorCmd}
	err = db.Run(mongo.D{{"collStats",offers },{"scale",(1024*1024) }}, &m)
	if err != nil {
		fmt.Println(  fmt.Sprint("collStats for ",CFG.Main.DatabaseName,".",offers," failed: "), err)
	} else {
		if printDetails {
			fmt.Println( m["ns"] , " size: ", m["storageSize"], " MB" )			
		}
		tmpSize1,ok := m["storageSize"].(int)
		if ok {
			sizeWorkDb = int64(tmpSize1)
		}
	}




	for _,shardOrSelf := range SHARDS {

		if oplogAccessible {

			strConn := fmt.Sprint( shardOrSelf["ipAddress"], ":", shardOrSelf["portNumber"] )
			lpConn, err := mongo.Dial( strConn )	
			if err != nil {
				log.Fatal("2 mongo.Dial failed for ", strConn, err)
			}


			// revolting trick - authenticate against admin db
			// 	and after that you can access the local db
			db = mongo.Database{lpConn, "admin", mongo.DefaultLastErrorCmd}
			authenticateAdminDb(db)
			db = mongo.Database{lpConn, "local", mongo.DefaultLastErrorCmd}
			err = db.Run(mongo.D{{"collStats","oplog.rs" },{"scale",(1024*1024) }}, &m)
			if err != nil {
				fmt.Println(  fmt.Sprint("collStats for ", "local.oplog.rs" ," failed: "), err)
			} else {
				if printDetails {
					fmt.Println( m["ns"] , " size: ", m["storageSize"], " MB" )			
				}
				tmpSize2,ok := m["storageSize"].(int)
				if ok {
					sizeSumOplogs += int64(tmpSize2)
				}
			}

			
		  //err = db.Run(  mongo.D{{"dbStats"    , 1}}, &m)
			  err = db.Run(  mongo.D{{"serverStatus",1}}, &m)
			mapMem := make( map[string]interface{} )
			var ok bool		
			//db.runCommand({"serverStatus":1,"workingSet":1,"metrics":1,"locks":1,"repl":1,"indexCounters":1}) 			
			if err != nil {
				log.Fatal("runcommand serverStatus failed: ", err)
			} else {
				//printMap(m,false,"")
				mapMem,ok = m["mem"].( map[string]interface{} )
				if ok {
					//printMap(mapMem,false,"")
					resident += mapMem["resident"].(int)
					virtual  += mapMem["virtual"].(int)
					
				} else {
					fmt.Println("Server Status map did not contain a map 'mem' - no memory info")	
				}
			}

			
		}

	}



	return

	
}





/*
		Getting the memory infos of the mongod/mongos.
		
		Only useful if running without sharding on a mongod.
		
		Memory infos for a mongoS are relatively USELESS.
		Instead - use the info of MMS 
		
		interestingly,  serverStatus is executable 
			from app db

*/
func getServerStatsMemory(printDetails bool){

	/*
	resident, virtual, err := getServerStatsMemory(true)
	if err == nil {
		fmt.Printf("phys. Memory is %v - of data stock size of %v (useful only on mongod)\n",resident, virtual)
	} else {
		fmt.Println("no mem info", err )
	}
	*/
	
}



/*
	non-tailable cursor
*/
func iterateCursor(c mongo.Cursor ){
	for c.HasNext() {
		var m mongo.M
		err := c.Next(&m)
		if err != nil {
			log.Fatal("iterateCursor3 err: ",err)
		}
		printMap(m, false,"")
	}
}


// cleanUpPreviousData cleans up after previous runs of this applications.
// unused
func cleanUpPreviousData(conn mongo.Conn) {
	log.Print("\n\n== Clear documents and indexes created by previous run. ==\n")
	db := mongo.Database{conn, CFG.Main.DatabaseName, mongo.DefaultLastErrorCmd}
	db.Run(mongo.D{{"profile", 0}}, nil)
	db.C(offers).Remove(nil)
	db.Run(mongo.D{{"dropIndexes", offers}, {"index", "*"}}, nil)
}





func x2________tailing__________(){}





func getTailCursorMain( shardOrSelf map[string]string ) mongo.Cursor {
	
	strConn := fmt.Sprint( shardOrSelf["ipAddress"], ":", shardOrSelf["portNumber"] )
	conn, err := mongo.Dial( strConn )	
	if err != nil {
		log.Fatal("1 mongo.Dial failed for ", strConn, " -- ", err)
	} else {
		fmt.Println("\t(re-)connected to ", strConn)	
	}
	
	dbLocalWithOplog  := mongo.Database{conn, "local", mongo.DefaultLastErrorCmd}	
	dbAdminForAuth    := mongo.Database{conn, "admin", mongo.DefaultLastErrorCmd}	
	authenticateAdminDb(dbAdminForAuth)
	oplog	:= dbLocalWithOplog.C( "oplog.rs" )  

	c := getTailCursorHelper(oplog)
	return c	

	
}


func getTailCursorHelper( oplog mongo.Collection ) mongo.Cursor  {

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


	 	  
	// .addOption(DBQuery.Option.tailable).addOption(DBQuery.Option.awaitData)
	if err != nil {
		log.Fatal( fmt.Sprint( "mongo oplog find error: ", err,"\n") )
	}

	log.Println( "\t\t ... tailable cursor retrieved. Id ", cursor.GetId() )	
	return cursor

}





func iterateTailCursor( shardOrSelf map[string]string ){
	

	conn := getConn()
	defer conn.Close()


	//oplogSubscription        := getMainDBCollection( conn, CFG.Main.DatabaseName, changelogCol  )
	//oplogSubscriptionCounter := getMainDBCollection( conn, CFG.Main.DatabaseName, counterChangeLogCol)
	colOffersByShop          := getMainDBCollection( conn, CFG.Main.DatabaseName, "offersByShop")
	colOffersByLastUpdated   := getMainDBCollection( conn, CFG.Main.DatabaseName, "offersByLastUpdated")


	fctfuncRecurseMsg   := funcRecurseMsg("recursion ")

	c := getTailCursorMain( shardOrSelf   )

	if !  oplogAccessible  {
		fmt.Println("no tail cursor on shard system")	
		return
	}



	for {
		
		doBreak, hasNext := checkTailCursor(c)

		if doBreak {

			fmt.Print("going to sleep ",secondsDefer, " secs.")
			time.Sleep( secondsDefer * time.Second )
			
			if countNoNextValue < noNextValueMax {
				c = getTailCursorMain( shardOrSelf   )
				doBreak, _ := checkTailCursor(c)
				if doBreak {
					log.Println("second failure of tail cursor")
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

			if   strings.HasPrefix( ns ,nsToTrack)  {
			//	if   ! strings.HasPrefix( ns ,changelogFullPath)  {

				var ok bool = true
				var operationTimeStamp mongo.Timestamp	
				var typeOfOp string
				var mapOfSets map[string]interface{}
				var lastUpdatedofOp int
				
				operationTimeStamp,ok = m["ts"].(mongo.Timestamp)
				if ! ok {
					log.Fatal("m[ts] not a valid timestamp")	
				}
				oplogOpTime := int64(operationTimeStamp) >> 32
				_,_ =  tailCursorLogInc( 0, oplogOpTime )	
				
				
				
				var oid mongo.ObjectId = mongo.ObjectId("51dc1b9d419c")  // 12 chars
				var shopId int

				moo, ok := m["o"].(map[string]interface{})
				
				if updateSecondaryIndize > 0  {
					if ok {
						innerMap = moo
						typeOfOp, ok = m["op"].(string)
						if ok   {
							switch typeOfOp {
							case "u" : 
									//fmt.Print(" u")
									oid = getOidFromMap( m["o2"] )
									mapOfSets, ok = moo["$set"].(map[string]interface{})
									if ok {
										lastUpdatedofOp, ok = mapOfSets["lastUpdated"].(int)
										if ok {
											idCustom :=  fmt.Sprint(lastUpdatedofOp, "::" , oid, "::", shardOrSelf["ipAddress"])
											errUpsert1 := colOffersByLastUpdated.Upsert( 
												mongo.M{"_id": idCustom} , 
												mongo.M{"_id": idCustom ,"fk_id": oid, "lastUpdated": lastUpdatedofOp, "op" : "u" })
											checkWriteBack( "OffersByLastUpdated u upsert", errUpsert1,  shardOrSelf["shardId"])
										} else {
											log.Printf(" mapOfSets[\"lastUpdated\"] - empty\n")																		
										}
									} else {
										log.Printf(" moo[\"$set\"] - no last Update value\n")									
									}
	
								
							case "i" : 
								//fmt.Print(" i")
								oid = getOidFromMap( m["o"] )
								lastUpdatedofOp, ok = moo["lastUpdated"].(int)
								if ok {
									idCustom :=  fmt.Sprint(lastUpdatedofOp, "::" , oid, "::", shardOrSelf["ipAddress"])
									errUpsert2 := colOffersByLastUpdated.Upsert( 
										mongo.M{"_id": idCustom} , 
										mongo.M{"_id": idCustom ,"fk_id": oid, "lastUpdated": lastUpdatedofOp, "op" : "i" })
									checkWriteBack( "OffersByLastUpdated i upsert", errUpsert2,  shardOrSelf["shardId"])
	
								} else {
									log.Printf(" map[o][lastUpdated] - empty\n")																		
								}						
	
								shopId, ok = moo["shopId"].(int)
								if ok {
									idCustom :=  fmt.Sprint(shopId, "::" , oid, "::", shardOrSelf["ipAddress"])
									errUpsert3 := colOffersByShop.Upsert( 
										mongo.M{"_id": idCustom} , 
										mongo.M{"_id": idCustom ,"fk_id": oid, "shopId": shopId, "op" : "i" })
									checkWriteBack( "OffersByShop i upsert", errUpsert3,  shardOrSelf["shardId"])
								} else {
									log.Printf(" map[o][shopId] - empty\n")																		
								}						
	
	
							}
						} else {
							log.Printf(" m[\"op\"] - no operation type \n")
						}
	
	
					} else {
						log.Printf(" m[\"o\"] No object map (delete op) \n")
					}
				}
			
/*				
				errInsert := oplogSubscription.Insert(mongo.M{"ts": operationTimeStamp ,
					  "operation": m["op"], 
					  "oid" : oid ,
					  "ns" :  ns,
					  "im" :  innerMap,
				})
				if errInsert != nil {
					if errInsert.Error() == "mongo: unknown response opcode -268435456"  ||   errInsert.Error() == "mongo: cursor has no more results"{
						fmt.Println("oplog journal entry failed with 'opcode -268435456' or no more results - possible threads use the SAME connection")
					} else {
						log.Fatal(" oplog journal insert error: ",errInsert)						
					}
				} else {
					//fmt.Println(" successful oplog journal entry")
				}


				var errCounter error = nil
				if insertCountIntoCollection {
					errCounter = oplogSubscriptionCounter.Update( mongo.M{"counter": mongo.M{"$exists": true}, } , mongo.M{"$inc"   : mongo.M{"counter": 1} },)	
					if errCounter != nil {
						log.Fatal("lf12 ",errCounter)
					}
				}

*/

				printMap(m       ,true ,"   ")
				printMap(innerMap,false,"   ")


			} else {
				fmt.Print( fctfuncRecurseMsg() )
			}

			incTailCounter()

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
func checkTailCursor( c mongo.Cursor  ) ( doBreak, hasNext  bool ){

	if err := c.Err(); err != nil {
		log.Println(   fmt.Sprint( "mongo permanent cursor error: ", err,"\n") )
		return true, false
	}

	hasNext = 	c.HasNext()

	// AFTER hasNext check !
	alive   := false
	if c.GetId() > 0 {
		alive = true
	}


	if alive == false || hasNext == false {


		if !alive  {
			fmt.Print( fmt.Sprintf( " cursor is dead - id %v.", c.GetId()) )
		}

		if hasNext == false  {
			fmt.Print( " hasNext() is false - await is over.")
		}

		fmt.Print( fmt.Sprintf( " dead or exhausted %v (max %v).", countNoNextValue, noNextValueMax) )

		countNoNextValue++
		if countNoNextValue > noNextValueMax {
			return true , hasNext
		}

	}

	return false, hasNext			


}








func x3_________reporting_______(){}


func startTimerLog(){
	
	const intervalTimer = 500
	
	const layout2 = "15:04 05"
	const layout3 = " 05.0 "
	// http://digital.ni.com/public.nsf/allkb/A98D197224CB83B486256BC100765C4B


	//timeStart := time.Now()
	//log.Print( timeStart.Format( layout3 ) )
	ctick := time.Tick(intervalTimer * time.Millisecond)

	i := int64(0)


	go func() { 
		//for now := range ctick {
		for _ = range ctick {

			// header every x secs
			if i % 40 == 0 {
				fmt.Printf("\n")				
				fmt.Printf("\n%10s%10s%10s%10s%14s%10s","seq_rd","insert","update","tail","lag","sz_col")
				fmt.Printf("\n")
				fmt.Print( strings.Repeat("=",10*6+4) )
			}

			csvRecord = make(map[string]int64)		// make new map
			//csvRecord["time"] = time.Now().Unix()

			fmt.Print("\n")
			
			freqPerSec := 1000 / float64(intervalTimer) 
			writeLoadReadUpdateInfo( freqPerSec )
			writeTailInfo(freqPerSec )


			// collection size and oplog lag every y secs
			if i % 5 == 0 {

				lastLag, lagTrail  :=  tailCursorLagReport()
				fmt.Printf("%14v",lagTrail)		

				sizeWorkDb,sizeSumOplogs, resident, virtual , err := sizesAndMemory(false)
				if err != nil {
					if err.Error() == shardErr {
						//log.Print(" -no local db on shard - no oplog size")				
					} else {
						fmt.Printf( "offers size: %v  oplog %v  - error: %v\n", sizeWorkDb, sizeSumOplogs, err)	
					}				
				}
				fmt.Printf("%10v",sizeWorkDb)			

				csvRecord["Hot Set to SysRAM"] = 100*(sizeWorkDb + sizeSumOplogs) / int64(resident)

				//csvRecord["Collection Size"] = s1
				csvRecord["System RAM"] = int64( 100* resident / virtual)
				
				csvRecord["Lag of Tail Cursor"] = lastLag

				
				

			} else {
				fmt.Printf("%14s%10s","-","-")			
			}
			i++


		}
	}()
	
	
}




func writeLoadReadUpdateInfo( freqPerSec float64 ) {
	
		ARR_READ_CUR, ok := (<- chr)
		if ok {
			sum := int64(0)
			for k,v := range ARR_READ_CUR {
				sum += int64(v)
				ARR_READ_TOT[k] += int64(v)
			}
			sum *= readBatchSize

			perSec := float64(sum) * freqPerSec
			perSec  = math.Trunc( 10* perSec) / 10
			fmt.Printf("%10v",perSec)			
			csvRecord["Reads per Sec * 1000"] = int64(perSec  / 1000)
			arrReadCurNew := make( []int64, len(ARR_READ_CUR) )
			chr <- arrReadCurNew
	
		} else {
			log.Fatal("error reading from chr 3")
		}



		ARR_INSERT_CUR, ok := (<- chl)
		if ok {
			sum := int64(0)
			for k,v := range ARR_INSERT_CUR {
				sum += int64(v)
				ARR_INSERT_TOT[k] += int64(v)
			}
			perSec := float64(sum) * freqPerSec
			perSec  = math.Trunc( 10* perSec) / 10
			fmt.Printf("%10v",perSec)			
			csvRecord["Inserts per Sec * 10"] = int64(perSec / 10) 
			arrLoadCurNew := make( []int64, len(ARR_INSERT_CUR) )
			chl <- arrLoadCurNew
	
		} else {
			log.Fatal("error reading from chl 3")
		}


		ARR_UPDATE_CUR, ok := (<- chu)
		if ok {
			sum := int64(0)
			for k,v := range ARR_UPDATE_CUR {
				sum += int64(v)
				ARR_UPDATE_TOT[k] += int64(v)
			}
			perSec := float64(sum) * freqPerSec
			perSec  = math.Trunc( 10* perSec) / 10
			fmt.Printf("%10v",perSec)			
			csvRecord["Updates per Sec * 10"] = int64(perSec/10) 
			arrUpdateCurNew := make( []int64, len(ARR_UPDATE_CUR) )
			chu <- arrUpdateCurNew
	
		} else {
			log.Fatal("error Updateing from chu 3")
		}
	
}


func writeTailInfo( freqPerSec float64){

		cntTail,ok := (<- cht) 

		if ok {
			tailTotal += cntTail
	
			perSec := float64(cntTail) * freqPerSec
			perSec  = math.Trunc( 10* perSec) / 10
			//fmt.Print(" ", perSec," ", cntTail, " ", freqPerSec)
			fmt.Printf( "%10v",perSec )			
			csvRecord["Tails per Sec * 10"] = int64(perSec/ 10) 
	
		} else {
			log.Fatal("error reading from cht")
		}
		cht <- 0
	
}




func incTailCounter(){
		cntTail,ok := (<- cht)
		if ok {
			cntTail++
			cht  <- cntTail
			//print("cntTail:",cntTail)
		} else {
			print(" 2 cht is closed\n")
		}						
	
}



func incReadCounter(i int64, idxThread int32){

		const chunkSize = 100
		if (i+1) % chunkSize == 0 {
			ARR_READ_CUR, ok := (<- chr)
			if ok {
				ARR_READ_CUR[idxThread] += chunkSize
				chr <- ARR_READ_CUR
			} else {
				log.Fatal("error reading from chr 2")
			}
		}
	
}

func incInsertCounter(i int64, idxThread int32){
	
		const chunkSize = 100
		if (i+1) % chunkSize == 0 {
			ARR_INSERT_CUR, ok := (<- chl)
			if ok {
				ARR_INSERT_CUR[idxThread] += chunkSize
				chl <- ARR_INSERT_CUR
			} else {
				log.Fatal("error reading from chl 2")
			}
		}
	
}


func incUpdateCounter(i int64, idxThread int32){
	
		const chunkSize = 100
		if (i+1) % chunkSize == 0 {
			ARR_UPDATE_CUR, ok := (<- chu)
			if ok {
				ARR_UPDATE_CUR[idxThread] += chunkSize
				chu <- ARR_UPDATE_CUR
			} else {
				log.Fatal("error reading from chu 2")
			}
		}
	
}


func tailCursorLogInc(newInsertUpdateSaveTime,newTimeOplog int64) (x,y int64) {
	
		if  newInsertUpdateSaveTime > 1 {
		  atomic.StoreInt64( &timeLastSaveOperation, newInsertUpdateSaveTime, )
		}

		if  newTimeOplog > 1   {
			atomic.StoreInt64( &timeLastOplogOperation, newTimeOplog, )
		}
	
	  effInsertSaveTime :=  atomic.LoadInt64(&timeLastSaveOperation)
	  effTimeOplog	    :=  atomic.LoadInt64(&timeLastOplogOperation)
	  
	  return effInsertSaveTime,effTimeOplog

}


func tailCursorLagReport()(lastLag int64,lagTrail string){

	effInsertSaveTime, effTimeOplog :=  tailCursorLogInc(0,0)

	if lv[0] != effTimeOplog{
		var lvTmp []int64 = make( []int64, sizeLagReport )
		for k,_ := range lv {
			if k == 0 {
				continue
			}
			lvTmp[k] = lv[k-1]
		}
		lv =lvTmp
		lv[0] = effTimeOplog
	}

	var comparisonBase int64 
	//comparisonBase = tStart.Unix()
	//comparisonBase = time.Now().Unix()
	comparisonBase = effInsertSaveTime

	tmp := int64(0)
	for k,v := range lv {
			if v > 1 {
				tmp = comparisonBase - v
			} else {
				tmp = lv[k]
			}
			lagTrail = fmt.Sprint( lagTrail," ", tmp )

			if k == 0 {
				lastLag = tmp	
			}

	}


	if ! oplogAccessible {
			lastLag = -1
	}
	
	return lastLag, lagTrail
		
}
	
	


func finalReport() (x,y,z int64){

	var readTotal = int64(0)

	var insertTotal = int64(0)
	var updateTotal = int64(0)
	


	ARR_READ_CUR, ok1 := (<- chr)
	if ok1 {
		for k,_ := range ARR_READ_CUR {
			v2 := ARR_READ_TOT[k]
			v2 *= readBatchSize
			readTotal += v2
			log.Printf("thread %v - read ops %v - ", k , v2)
		}
	} else {
		log.Fatal("error reading from chr 1")
	}




	ARR_INSERT_CUR, ok2 := (<- chl)
	if ok2 {
		for k,_ := range ARR_INSERT_CUR {
			v2 := ARR_INSERT_TOT[k]
			insertTotal += v2
			log.Printf("thread %v - load ops %v - ", k , v2)
		}
	} else {
		log.Fatal("error reading from chl 1")
	}


	 

	ARR_UPDATE_CUR, ok3 := (<- chu)
	if ok3 {
		for k,_ := range ARR_UPDATE_CUR {
			v2 := ARR_UPDATE_TOT[k]
			//v2 *= updateBatchSize
			updateTotal += v2
			log.Printf("Thread %v - Update ops %v - ", k , v2)
		}
	} else {
		log.Fatal("error Updateing from chu 1")
	}

	return readTotal, insertTotal, updateTotal 
	 

	
}


func x4_________________________(){}

func loadInsert(idxThread int32 , batchStamp int64){
	
	
	fctfuncRecurseMsg   := funcRecurseMsg( fmt.Sprint("loadInsert",idxThread," "))

	conn := getConn()
	defer conn.Close()
	colOffers := getMainDBCollection( conn, CFG.Main.DatabaseName, offers  )
	h := md5.New()

	
	for i:=batchStamp ; i < batchStamp+insertsPerThread; i++ {
		
		factor := 1000
		shopRnd := rand.Intn(10*factor)   // this gives you an int up to but not including arg
		shopId  := shopRnd

		switch {
			case shopRnd < 4*factor:  shopId = 1
			case shopRnd < 8*factor:  shopId = 2
			default: shopId = shopRnd
		}
		
		//io.WriteString(h, "The fog is getting thicker!")
		io.WriteString( h, fmt.Sprint( i , "The fog is getting thicker!") )
		hashedId := fmt.Sprintf( "%x", h.Sum(nil) )
		//fmt.Print(" ", hashedId)

		
		
		err := colOffers.Insert(mongo.M{"offerId": i,
				"_id" : hashedId,
			 "shopId"	       : shopId, 
			 "categoryId"    : 15,
			 "lastSeen"      : int32(time.Now().Unix()) ,
			 "lastUpdated"   : int32(time.Now().Unix()) ,
			 "countUpdates"  : 1 ,
			 "title"         : fmt.Sprint("title",i) ,
			 "description"   : strings.Repeat( fmt.Sprint("description",i), 31),
			 // server side javascript - even if possible - locks the entire collection
			 //"description": "new Array( 44 ).join( \"description\")",					
		})
		if err != nil {
			log.Fatal(   fmt.Sprint( "mongo loadInsert error: ", err,"\n") )		
		}
		log.Print( fctfuncRecurseMsg() )

		incInsertCounter(i,idxThread)
		_,_ =  tailCursorLogInc( time.Now().Unix() ,0)	
		
	}
	atomic.AddInt32( &LOADER_COUNTER, -1 )
	fmt.Print(" -ld_ins",idxThread,"_fin")
	
}




func funcLoadRead()  func(idxThread int32) {



	return func(idxThread int32 ) {

		//fmt.Println( "loadRead: ", idxThread , batchStamp  )	
		fctfuncRecurseMsg := funcRecurseMsg( fmt.Sprint("loadRead",idxThread," "))


		conn := getConn()
		defer conn.Close()
		colOffers := getMainDBCollection(conn,CFG.Main.DatabaseName,offers)

		getPartitionStart := funcPartitionStart()
		
		minOid,initMinOid:= getPartitionStart(idxThread, false )
		loopMinOid := initMinOid

		i := int64(0)
		for  {

			i++
			imax := int64(10 * 1000 * 1000 * readBatchSize)		// make sure to read our dataset at least two times
			if i > imax {
				//log.Println( fmt.Sprint(" more than ",imax," iterations. Tread over.") )		
				break	
			}
			log.Print( fctfuncRecurseMsg() )
			incReadCounter(i,idxThread)


			var m mongo.M
		  err := colOffers.Find(mongo.M{"_id": mongo.M{"$gte": loopMinOid,},}).Fields(mongo.M{"description": 0}).Skip(readBatchSize).Limit(1).One(&m)
			if err != nil  && err != mongo.Done {
				log.Fatal(   fmt.Sprint( "mongo loadRead error: ", err,"\n") )		
			}

			tmpMinOid, ok := m["_id"].(mongo.ObjectId)
			if ! ok {
				if err.Error() == "mongo: cursor has no more results" {
					fmt.Print(" rd",idxThread, "_rst1")
					loopMinOid = minOid
					continue
				} else {
					log.Fatal("end of read seq. err: ", err)
				}
			} else if loopMinOid == tmpMinOid {
				fmt.Print(" rd",idxThread, "_rst2")
				loopMinOid = minOid
				continue
			} else {
				//fmt.Println(idxThread, " new oid" , loopMinOid, tmpMinOid )
				loopMinOid = tmpMinOid
			}

			lc := atomic.LoadInt32( &READER_COUNTER )
			if lc > READERS_CONC_MAX {
				fmt.Print(" rd_pruned",idxThread)
				break
			}
			

			
		}
		atomic.AddInt32( &READER_COUNTER, -1 )	
		fmt.Print(" -rd",idxThread,"_fin")



	}


}




func loadUpdate(idxThread int32 ) {

	//fmt.Println( "loadUpdate: ", idxThread , batchStamp  )	
	fctfuncRecurseMsg := funcRecurseMsg( fmt.Sprint("loadUpdate",idxThread," "))


	conn := getConn()
	defer conn.Close()
	colOffers := getMainDBCollection(conn,CFG.Main.DatabaseName,offers)

	getPartitionStart := funcPartitionStart()
	
	minOid,initMinOid:= getPartitionStart(idxThread, true )
	minOidNextRead  := initMinOid


	i := int64(0)
	for  {

		i++
		imax := int64( 10 * 1000 * 1000 * updateBatchSize  )			// make sure to loop our dataset at least two times
		if i > imax {
			//log.Println( fmt. (" more than ",imax," iterations. LoopUpdate over.") )		
			break	
		}
		//fmt.Print( fmt.Sprint(" -u",i) )		


	  cursor, errRd4Upd := colOffers.Find(  mongo.M{"_id": mongo.M{"$gte": minOidNextRead,},}).
	  	Fields(mongo.M{"_id": 1}).Limit(updateBatchSize).Cursor()
		if errRd4Upd != nil   {

			if errRd4Upd.Error() == "mongo: forupdate cursor has no more results" {
				fmt.Print(" rd4upd_reset_1",idxThread)				
				minOidNextRead = minOid
				continue
			}
			log.Fatal(  fmt.Sprint( "mongo read4Update get oids error: ", errRd4Upd,"\n") )		
		}
		
		
		previousMinOidNextRead := minOidNextRead

		j := int64(0)
		for cursor.HasNext() {

			j++
			
			var m mongo.M
			errRdNext := cursor.Next(&m)
			if errRdNext != nil {
				log.Fatal(  fmt.Sprint( "mongo read4Update next cursor error: ", errRdNext,"\n") )		
			}
			tmpLoopOid, ok := m["_id"].(mongo.ObjectId)
			if ! ok  {
				log.Fatal("mongo read4Update can not read oid out of document")
			}

			now1 := int32(time.Now().Unix())
//			errUpd := colOffers.Update(  mongo.M{  "_id": mongo.M{"$gte": tmpLoopOid,  "$lte": tmpLoopOid,}  , }  ,
//		 			mongo.M{  "$inc": mongo.M{"lastSeen": -1, "countUpdates": 1} ,
//		 		 	"$set": mongo.M{"lastUpdated": now1 }  }  )

			errUpd := colOffers.Update(  mongo.M{  "_id": tmpLoopOid, }  ,
		 			mongo.M{  "$inc": mongo.M{"lastSeen": -1, "countUpdates": 1} ,
		 		 	"$set": mongo.M{"lastUpdated": now1 }  }  )

			checkWriteBack( "mongo read4Update update", errUpd,  fmt.Sprint( "update thread ", idxThread ) )
			
			
			log.Print( fctfuncRecurseMsg() )
			incUpdateCounter( updateBatchSize*i + j,idxThread )
			_,_ =  tailCursorLogInc( time.Now().Unix() ,0)


			minOidNextRead = tmpLoopOid

			//printMap(m, false,"")
		}

		// we deliberately slow the single thread 
		// so that we may scale in finer granularity
		time.Sleep( 150 * time.Millisecond )

		

		if previousMinOidNextRead == minOidNextRead {
			fmt.Print(" rd4upd_reset_2",idxThread)
			minOidNextRead = minOid
			continue
		}


		lc := atomic.LoadInt32( &UPDATER_COUNTER )
		if lc > UPDATERS_CONC_MAX {
			fmt.Print(" rd4upd_pruned",idxThread)
			break
		}
		

	}

	
	atomic.AddInt32( &UPDATER_COUNTER, -1 )	
	fmt.Print(" -ld_upd",idxThread,"_fin")


}



/*
	partitioning data, so that reads can start at different partitions

*/
func funcPartitionStart() func(threadIdx int32, forReadOrUpdate bool) (x,y mongo.ObjectId){

		conn := getConn()
		defer conn.Close()
		colOffers := getMainDBCollection(conn,CFG.Main.DatabaseName,offers)
		
		

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
			log.Print("max did not contain an OID - using a default ")				
			oidMax = mongo.MinObjectIdForTime( time.Date(2030, time.December, 31, 23, 59, 0, 222, time.Local))
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


		return func(threadIdx int32, forReadOrUpdate bool)(minOid,minOidThreadPartition mongo.ObjectId) {

			divisor := READERS_CONC_MAX
			if forReadOrUpdate {
				divisor = UPDATERS_CONC_MAX
			}

			partitionTimeDiff := time.Duration(threadIdx)*diffTime / time.Duration(divisor)
			//fmt.Println("diff",diffTime, partitionTimeDiff)				
			timeMinThread := ctMin.Add( partitionTimeDiff )
			minOidThread  := mongo.MinObjectIdForTime( timeMinThread )

			//const layout2 = "01-02 15:04 05"
			//fmt.Printf("threadIndex: %v, oid-min %v threadStart %v oid-max %v \n",threadIdx,oidMin.CreationTime().Format(layout2),minOidThread.CreationTime().Format(layout2),oidMax.CreationTime().Format(layout2) )
			return oidMin,minOidThread

		}

}






func x5_______helpers___________(){}


/*
	executes a "free" command
	to retrieve physical memory
	on the app machine
*/
func PhysicalMemoryOfAppMachine()(membytes int64, err error) {
	membytes = 0
	err = nil
	cmd := exec.Command("tr", "a-z", "A-Z")
	cmd = exec.Command("free")
	//cmd.Stdin = strings.NewReader("some input")
	cmd.Stdin = strings.NewReader("uselessArg")
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		log.Fatal("PhysicalMemoryOfAppMachine: ",err)
	}
	//fmt.Printf("\nfree says1: %q\n", out.String())
	
	s1 := out.String()
	s2 := strings.Split(s1,"\n")
	if( len(s2) > 1 ){
		//fmt.Printf("\n free says2: %q\n", s2[1] ) 
		words := strings.Fields( s2[1] )	
		//fmt.Printf("\n free says3: %q\n", words[1] ) 
		membytes,err = strconv.ParseInt( words[1],10,64 )
		membytes = membytes >> 10		// MB
		//membytes = membytes >> 10		// GB
		
	} else {
		err = errors.New("could not parse output of free command - windows no worki")
	}

	return

}




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


/*
		takes a msg string 
		and outputs it upon each call of the inner function

*/
func funcRecurseMsg(cmsg string) func() string {
	

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

func Flush1(w http.ResponseWriter){
	
	fmt.Fprintf(w, strings.Repeat("	 ",9000))	
	
}
func p2(w http.ResponseWriter, f string, args ... interface{} ){
	
		fmt.Printf(f, args...)
  	fmt.Fprintf(w, f, args...)

	
}


func renderTemplatePrecompile( w http.ResponseWriter,tname string , c map[string]string ){

	err := templates.ExecuteTemplate(w, tname + ".html", c)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	Flush1(w)
	
}

func renderTemplateNewCompile( w http.ResponseWriter,tname string , c map[string]string ){

	t, err := template.ParseFiles(tname + ".html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = t.Execute(w, c)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	Flush1(w)

}


// commit it !!
func Reverse(s string) string{
	b := []byte(s)
	for i:= 0; i < len(b)/2; i++ {
		j := len(b)-i-1
		b[i],b[j] = b[j],b[i]
	}
	return string(b)

}


func printHelperCommands(){

	//newOid := mongo.NewObjectId()
	//minOid := mongo.MinObjectIdForTime( tStart.Add(-200 * time.Millisecond))
	minOid1999 := mongo.MinObjectIdForTime( time.Date(1999, time.November, 10, 15, 25, 0, 222, time.Local))
	mgoCmd := fmt.Sprint( "db.getSiblingDB(\"", CFG.Main.DatabaseName , "\").offers.test.find({},{description:0}).min({_id: ObjectId(\"",minOid1999,"\") })" )
	fmt.Println(mgoCmd)


	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"", CFG.Main.DatabaseName , "\").offers.test.find({},{description:0}).sort({\"_id\":-1})" )
	fmt.Println(mgoCmd)
	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"", CFG.Main.DatabaseName , "\").offers.test.find({},{description:0}).sort({\"lastUpdated\":-1})" )
	fmt.Println(mgoCmd)


	sc := 1024*1024
	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"", CFG.Main.DatabaseName , "\").offers.test.stats(",sc," )" )
	fmt.Println(mgoCmd)
	fmt.Println( " db.runCommand( { serverStatus : 1 , scale : ", sc, " } )				")
	fmt.Println( " db.runCommand( { dbStats :      1 , scale : ", sc, " } )				")
	//fmt.Println( " db.runCommand( { shutdown: 1 } )				")
	
	
	fmt.Println()
	
	fmt.Println("")

	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"", CFG.Main.DatabaseName , "\").offers.test.ensureIndex( { \"lastUpdated\": 1 } )" )
	fmt.Println(mgoCmd)
	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"", CFG.Main.DatabaseName , "\").offers.test.getIndexes()" )
	fmt.Println(mgoCmd)
	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"", CFG.Main.DatabaseName , "\").offers.test.dropIndex(\"lastUpdated_1\")" )
	fmt.Println(mgoCmd)

	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"", CFG.Main.DatabaseName , "\").addUser(\"scale_tester\", \"32168\" )   " )
	fmt.Println(mgoCmd)


	fmt.Println("")

	mgoCmd = fmt.Sprint( "sh.enableSharding(\"", CFG.Main.DatabaseName , "\") " )
	fmt.Println(mgoCmd)
	mgoCmd = fmt.Sprint( "sh.shardCollection(\"", CFG.Main.DatabaseName ,".", offers, "\" , {_id: 1 } )  // hashed instead of 1 " ) 
	fmt.Println(mgoCmd)

	mgoCmd = fmt.Sprint( "sh.shardCollection(\"", CFG.Main.DatabaseName ,".", "offersByShop",        "\" , {_id: 1, } ) " )
	fmt.Println(mgoCmd)
	mgoCmd = fmt.Sprint( "sh.shardCollection(\"", CFG.Main.DatabaseName ,".", "offersByLastUpdated", "\" , {_id: 1, } ) " )
	fmt.Println(mgoCmd)


	fmt.Println("")


	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"", CFG.Main.DatabaseName , "\").oplog.subscription.find({},{im:0}).sort({\"_id\":-1})" )
	fmt.Println(mgoCmd)

	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"", CFG.Main.DatabaseName , "\").oplog.subscription.counter.find({},{_id:0,changed3:0})" )
	fmt.Println(mgoCmd)

	mgoCmd = fmt.Sprint( "db.getSiblingDB(\"local\").oplog.rs.find({},{o:0}).sort({\"$natural\":-1})" )
	fmt.Println(mgoCmd)
	//fmt.Println(  " ts1 = Math.round( new Date().getTime()/1000) -300;" )

	tenSecsEarlier := time.Now().Unix() - 10
	fmt.Println(  "ts2 = new Timestamp(",tenSecsEarlier,", 0);" )
	fmt.Println(  "db.getSiblingDB('local').oplog.rs.find({'ts': { '$gte': ts2 }  }, {ts:1,op:1}  ).sort( {\"$natural\": 1} ) " )
	fmt.Println()



	
}


func loadConfig() error {

	err := gcfg.ReadFileInto(&CFG, "config.ini")
	if err != nil {
		return err
	} else {
		fmt.Println("connect to ",CFG.Main.Host,":",CFG.Main.Port)
	}

	return nil	
	
}

func getOidFromMap( mp  interface{}  ) ( mongo.ObjectId ) {


	var ok bool
	var oid mongo.ObjectId = mongo.ObjectId("51dc1b9d419c")  // 12 chars

	map2, ok := mp.(map[string]interface{})
	if ok {
		if 	_, ok  = map2["_id"]; ok {
			oid, ok  = map2["_id"].(mongo.ObjectId)
			if !ok {
				log.Fatal("getOidFromMap cannot convert to ObjectId: ",map2)
			}
		} else {
			log.Fatal("getOidFromMap cannot find key '_id' in ",map2)			
		}
	} else {
			log.Fatal("getOidFromMap cannot convert to map[string]interface{}: ",mp)		
	}



	return oid
	
}

func checkWriteBack( mark string , err error, shardName string){

	if err != nil {
		errMsg := err.Error()
	
		if strings.Contains(errMsg, writebackErr)  {
			//log.Print(" -no local db on shard - no oplog size")
			fmt.Print(" ", mark , " error: ",err, " skipping, shard:", shardName)			
		} else {
			debug.PrintStack()
			log.Fatal(mark,err," shard:", shardName)			
		}				
	}
	
}



