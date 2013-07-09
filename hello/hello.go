package main

import "fmt"

import (
	"github.com/garyburd/go-mongo/mongo"
	"log"
	"time"
	"math"
	_ "os"
)


var   deadPeriods int = 0
const deadPeriodsMax int = 310

func main(){



	const layout1 = "Jan 2, 2006 at 3:04pm (MST)"
	const layout2 = "2006-01-02 15:04 05"

	t := time.Date(2009, time.November, 10, 15, 25, 0, 222, time.Local)
	fmt.Println( t.Format(layout1))
	fmt.Println( t.UTC().Format(layout1) )
	fmt.Println( time.Unix(3600*24*740 +16,333).Format(layout2) )
	fmt.Println( time.Now().Format(layout2) )

	

	// Connect to server.
	    conn, err := mongo.Dial("localhost:27003")
	//  conn, err := mongo.Dial("sx122:27017")

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()


	// Wrap connection with logger so that we can view the traffic to and from
	// the server.
	// conn = mongo.NewLoggingConn(conn, log.New(os.Stdout, "", 0), "")

	// Clear the log prefix for more readable output.
	log.SetFlags(0)

	//reset(conn)
	//chapter1(conn)


	log.Println("\n\n==connect_to_oplog==")
	
	db := mongo.Database{conn, "local", mongo.DefaultLastErrorCmd}	// Create a database object.
	oplog := db.C("oplog.rs")  // create collection

	// Limit(4).Skip(2) and skip are ignored for tailable cursors
	cursor, err := oplog.Find(nil).Tailable(true).Limit(4).Skip(2).Sort( mongo.D{{"$natural", 1}} ).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	iterateCursorTail(cursor)


}


func iterateCursorTail( c mongo.Cursor ){

	//checkCursor(c)
	//for c.HasNext() {
	for {
		var m mongo.M
		err := c.Next(&m)
		if err != nil {
			log.Println(err, fmt.Sprint(err) )
		}
		printMap(m,true)


		if doBreak := checkCursor(c); doBreak {
			break
		}
	}

}

func checkCursor( c mongo.Cursor  )  bool {

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
			doBreak = true
			return true
		}
		time.Sleep( 1200 * time.Millisecond)
	}


	if err := c.Err(); err != nil {
		log.Println( fmt.Sprint( "cursor.Err() says ", err) )
		log.Fatal(   fmt.Sprint( "cursor.Err() says ", err) )
	}
	return false

}


var chapter1SampleData = mongo.A{
	mongo.M{"name": "Horny"      , "dob": time.Now().Format("20060102150405")     , "loves": mongo.A{"carrot", "papaya"}, "weight": 600, "gender": "m", "vampires": 63},
	mongo.M{"name": "Aurora"     , "dob": time.Unix(15,0).Format("20060102150405"), "loves": mongo.A{"carrot", "grape"} , "weight": 450, "gender": "f", "vampires": 43},
	mongo.M{"name": "Unicrom"    , "dob": dateTime(1973, 1, 9, 22, 10), "loves": mongo.A{"energon", "redbull"}, "weight": 984, "gender": "m", "vampires": 182},
	mongo.M{"name": "Roooooodles", "dob": dateTime(1979, 7, 18, 18, 44), "loves": mongo.A{"apple"}, "weight": 575, "gender": "m", "vampires": 99},
	mongo.M{"name": "Solnara"    , "dob": dateTime(1985, 6, 4, 2, 1), "loves": mongo.A{"apple", "carrot", "chocolate"}, "weight": 550, "gender": "f", "vampires": 80},
	mongo.M{"name": "Ayna"       , "dob": dateTime(1998, 2, 7, 8, 30), "loves": mongo.A{"strawberry", "lemon"}, "weight": 733, "gender": "f", "vampires": 40},
	mongo.M{"name": "Kenny"      , "dob": dateTime(1997, 6, 1, 10, 42), "loves": mongo.A{"grape", "lemon"}, "weight": 690, "gender": "m", "vampires": 39},
	mongo.M{"name": "Raleigh"    , "dob": dateTime(2005, 4, 3, 0, 57), "loves": mongo.A{"apple", "sugar"}, "weight": 421, "gender": "m", "vampires": 2},
	mongo.M{"name": "Leia"       , "dob": dateTime(2001, 9, 8, 14, 53), "loves": mongo.A{"apple", "watermelon"}, "weight": 601, "gender": "f", "vampires": 33},
	mongo.M{"name": "Pilot"      , "dob": dateTime(1997, 2, 1, 5, 3), "loves": mongo.A{"apple", "watermelon"}, "weight": 650, "gender": "m", "vampires": 54},
	mongo.M{"name": "Nimue"      , "dob": dateTime(1999, 11, 20, 16, 15), "loves": mongo.A{"grape", "carrot"}, "weight": 540, "gender": "f"},
	mongo.M{"name": "Dunx"       , "dob": dateTime(1976, 6, 18, 18, 18), "loves": mongo.A{"grape", "watermelon"}, "weight": 704, "gender": "m", "vampires": 165},
}

func chapter1(conn mongo.Conn) {

	log.Println("\n\n== CHAPTER 1 ==")
	db := mongo.Database{conn, "learn", mongo.DefaultLastErrorCmd}	// Create a database object.
	unicorns := db.C("unicorns")  // create collection

	log.Print("\n\n== Add first unicorn. ==\n")
	err := unicorns.Insert(mongo.M{"name": "Aurora" , "gender": "f", "weight": 450})
	 err = unicorns.Insert(mongo.M{"name": "Baccara", "gender": "h", "weight": 250})
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n\n== Find all unicorns. ==\n")
	cursor, err := unicorns.Find(nil).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	log.Print("\n\n== Iterate over the documents in the result. ==\n")
	iterateCursor(cursor)


	log.Print("\n\n== Show index created on _id. ==\n")
	si := db.C("system.indexes")
	cursor, err = si.Find(nil).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	iterateCursor(cursor)
	//expectFieldValues(cursor, "name", "_id_")


	err = unicorns.Insert(mongo.M{"name": "Leto", "gender": "m", "home": "Arrakeen", "worm": false})
	if err != nil {
		log.Fatal(err)
	}
	cursor, err = unicorns.Find(nil).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	iterateCursor(cursor)

	log.Print("\n\n== Remove what we added to the database so far - and fill in a LOT ==\n")
	err = unicorns.Remove(nil)
	if err != nil {
		log.Fatal(err)
	}
	err = unicorns.Insert(chapter1SampleData...)
	if err != nil {
		log.Fatal(err)
	}
	cursor, err = unicorns.Find(nil).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	iterateCursor(cursor)



	log.Print("\n\n== Find unicorns without the vampires field. ==\n")
	cursor, err = unicorns.Find(mongo.M{"vampires": mongo.M{"$exists": false}}).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	iterateCursor(cursor)


	log.Print("\n\n== Find female unicorns which either love apples or oranges or weigh less than 500 lbs. ==\n")
	cursor, err = unicorns.Find( mongo.M{
		"gender": "f",
		"$or": mongo.A{ mongo.M{"loves": "apple"}, mongo.M{"loves": "orange"} } 	}).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	iterateCursor(cursor)



	log.Println("\n\n== CHAPTER 1 STOP ==\n")

}



func chapter2(conn mongo.Conn) {

	log.Println("\n== CHAPTER 2 ==")

	db := mongo.Database{conn, "learn", mongo.DefaultLastErrorCmd}
	unicorns := db.C("unicorns")
	hits := db.C("hits")

	log.Print("\n== Change Roooooodles' weight. ==\n\n")

	err := unicorns.Update(mongo.M{"name": "Roooooodles"}, mongo.M{"weight": 590})
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n== Update replaced the document. ==\n\n")

	var m mongo.M
	err = unicorns.Find(mongo.M{"name": "Roooooodles"}).One(&m)
	if err != nil && err != mongo.Done {
		log.Fatal(err)
	}

	log.Print("\n== Reset the lost fields using the set operator. ==\n\n")

	err = unicorns.Update(mongo.M{"weight": 590}, mongo.M{"$set": mongo.M{
		"name":     "Roooooodles",
		"dob":      dateTime(1979, 7, 18, 18, 44),
		"loves":    mongo.A{"apple"},
		"gender":   "m",
		"vampires": 99}})
	if err != nil {
		log.Fatal(err)
	}

	m = nil
	err = unicorns.Find(mongo.M{"name": "Roooooodles"}).One(&m)
	if err != nil {
		log.Fatal(err)
	}

	if m["name"] != "Roooooodles" {
		log.Fatal("Did not find Roooooodles")
	}

	log.Print("\n== Update weight the correct way. ==\n\n")

	err = unicorns.Update(mongo.M{"name": "Roooooodles"}, mongo.M{"$set": mongo.M{"weight": 590}})
	if err != nil {
		log.Fatal(err)
	}

	m = nil
	err = unicorns.Find(mongo.M{"name": "Roooooodles"}).One(&m)
	if err != nil {
		log.Fatal(err)
	}

	if m["weight"] != 590 {
		log.Fatalf("Expected Roooooodles' weight=590, got %d\n", m["weight"])
	}

	log.Print("\n== Correct the kill count for Pilot. ==\n\n")

	err = unicorns.Update(mongo.M{"name": "Pilot"}, mongo.M{"$inc": mongo.M{"vampires": -2}})
	if err != nil {
		log.Fatal(err)
	}

	m = nil
	err = unicorns.Find(mongo.M{"name": "Pilot"}).One(&m)
	if err != nil {
		log.Fatal(err)
	}

	if m["vampires"] != 52 {
		log.Fatalf("Expected Pilot's vampires=52, got %d\n", m["vampires"])
	}

	log.Print("\n== Aurora loves sugar. ==\n\n")

	err = unicorns.Update(mongo.M{"name": "Aurora"}, mongo.M{"$push": mongo.M{"loves": "sugar"}})
	if err != nil {
		log.Fatal(err)
	}

	m = nil
	err = unicorns.Find(mongo.M{"name": "Aurora"}).One(&m)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n== Updating a missing document does nothing. ==\n\n")

	err = hits.Update(mongo.M{"page": "unicorns"}, mongo.M{"$inc": mongo.M{"hits": 1}})
	if err == nil || err != mongo.ErrNotFound {
		log.Fatal(err)
	}

	err = hits.Find(mongo.M{"page": "unicorns"}).One(&m)
	if err != nil && err != mongo.Done {
		log.Fatal(err)
	}

	log.Print("\n== Upsert inserts the document if missing. ==\n\n")

	err = hits.Upsert(mongo.M{"page": "unicorns"}, mongo.M{"$inc": mongo.M{"hits": 1}})
	if err != nil {
		log.Fatal(err)
	}

	err = hits.Find(mongo.M{"page": "unicorns"}).One(&m)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n== Upsert updates the document if already present. ==\n\n")

	err = hits.Upsert(mongo.M{"page": "unicorns"}, mongo.M{"$inc": mongo.M{"hits": 1}})
	if err != nil {
		log.Fatal(err)
	}

	err = hits.Find(mongo.M{"page": "unicorns"}).One(&m)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n== Update updates a single document. ==\n\n")

	err = unicorns.Update(nil, mongo.M{"$set": mongo.M{"vaccinated": true}})
	if err != nil {
		log.Fatal(err)
	}

	cursor, err := unicorns.Find(mongo.M{"vaccinated": true}).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	expectCount(cursor, 1)

	log.Print("\n== UpdateAll updates all documents. ==\n\n")

	err = unicorns.UpdateAll(nil, mongo.M{"$set": mongo.M{"vaccinated": true}})
	if err != nil {
		log.Fatal(err)
	}

	cursor, err = unicorns.Find(mongo.M{"vaccinated": true}).Cursor()
	if err != nil {
		log.Fatal(err)
	}

	expectCount(cursor, 12)
}

func chapter3(conn mongo.Conn) {

	log.Println("\n== CHAPTER 3 ==")

	db := mongo.Database{conn, "learn", mongo.DefaultLastErrorCmd}
	unicorns := db.C("unicorns")

	log.Print("\n== Find names of all unicorns. ==\n\n")

	cursor, err := unicorns.Find(nil).Fields(mongo.M{"name": 1}).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	expectCount(cursor, 12)

	log.Print("\n== Find all unicorns ordered by decreasing weight. ==\n\n")

	cursor, err = unicorns.Find(nil).Sort(mongo.D{{"weight", -1}}).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	expectCount(cursor, 12)

	log.Print("\n== Find all unicorns ordered by name and then vampire kills. ==\n\n")

	cursor, err = unicorns.Find(nil).Sort(mongo.D{{"name", 1}, {"vampires", -1}}).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	expectCount(cursor, 12)

	log.Print("\n== Find the 2nd and 3rd heaviest unicorns. ==\n\n")

	cursor, err = unicorns.Find(nil).Sort(mongo.D{{"weight", -1}}).Limit(2).Skip(1).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	expectCount(cursor, 2)

	log.Print("\n== Count unicorns with more than 50 kills. ==\n\n")

	n, err := unicorns.Find(mongo.M{"vampires": mongo.M{"$gt": 50}}).Count()
	if err != nil {
		log.Fatal(err)
	}

	if n != 6 {
		log.Fatalf("Got count=%d, want 6", n)
	}
}

func chapter7(conn mongo.Conn) {

	log.Println("\n== CHAPTER 7 ==")

	db := mongo.Database{conn, "learn", mongo.DefaultLastErrorCmd}
	unicorns := db.C("unicorns")

	log.Print("\n== Create index on name. ==\n\n")

	err := unicorns.CreateIndex(mongo.D{{"name", 1}}, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n== Drop index on name. ==\n\n")

	err = db.Run(mongo.D{
		{"dropIndexes", unicorns.Name()},
		{"index", mongo.IndexName(mongo.D{{"name", 1}})},
	}, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n== Create unique index on name. ==\n\n")

	err = unicorns.CreateIndex(mongo.D{{"name", 1}}, &mongo.IndexOptions{Unique: true})
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n== Create compound index on name ascending and kills descending. ==\n\n")

	err = unicorns.CreateIndex(mongo.D{{"name", 1}, {"vampires", -1}}, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n== Explain query. ==\n\n")

	var m mongo.M
	err = unicorns.Find(nil).Explain(&m)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n== Explain query on name. ==\n\n")

	m = nil
	err = unicorns.Find(mongo.M{"name": "Pilot"}).Explain(&m)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n== Enable profiling. ==\n\n")

	err = db.Run(mongo.D{{"profile", 2}}, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("\n== Get profile data for query. ==\n\n")

	cursor, err := unicorns.Find(mongo.M{"weight": mongo.M{"$gt": 600}}).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	expectFieldValues(cursor, "name", "Unicrom", "Ayna", "Kenny", "Leia", "Pilot", "Dunx")

	cursor, err = db.C("system.profile").Find(nil).Cursor()
	if err != nil {
		log.Fatal(err)
	}

	for cursor.HasNext() {
		var m mongo.M
		err := cursor.Next(&m)
		if err != nil {
			log.Fatal(err)
		}
	}
	cursor.Close()

	log.Print("\n== Profile queries that take longer than 100 ms. ==\n\n")

	err = db.Run(mongo.D{{"profile", 2}, {"slowms", 100}}, nil)
	if err != nil {
		log.Fatal(err)
	}
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
		ms := fmt.Sprint(m)
		ms2:=ms[0: int(math.Min(120,float64(len(ms))))]
		log.Println( ms2 )
	} else {
		log.Println( m )
		for k,v := range m {
			log.Printf("\tk: %v \t\t: %v \n", k,v )
		}

	}
}


func expectCount(cursor mongo.Cursor, n int) {
	defer cursor.Close()
	i := 0
	for cursor.HasNext() {
		i += 1
		var m mongo.M
		err := cursor.Next(&m)
		if err != nil {
			log.Fatal(err)
		}
	}
	if i != n {
		log.Fatalf("Got result count=%d, want %d", i, n)
	}
}



// dateTime converts year, month, day hour and seconds to a time.Time
func dateTime(year, month, day, hour, minutes int) time.Time {
	return time.Date(year, time.Month(month), day, hour, minutes, 0, 0, time.Local)
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


// expectFieldValues iterates through the cursor and logs a fatal error if
// the a document does not have field in values or if a value in values was not
// found in a document.
func expectFieldValues(cursor mongo.Cursor, field string, values ...interface{}) {
	defer cursor.Close()
	found := map[interface{}]bool{}
	for cursor.HasNext() {
		var m mongo.M
		err := cursor.Next(&m)
		if err != nil {
			log.Fatal(err)
		}
		found[m["name"]] = true
	}
	for _, value := range values {
		if !found[value] {
			log.Fatalf("Expected result %v not found\n", value)
		} else {
			delete(found, value)
		}
	}
	for value, _ := range found {
		log.Fatalf("Unexpected result %v found\n", value)
	}
}