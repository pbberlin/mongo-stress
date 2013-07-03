package main

import "fmt"

import (
	"github.com/garyburd/go-mongo/mongo"
	"log"
	_ "os"
	"time"
)

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

	reset(conn)
	chapter1(conn)

}



var chapter1SampleData = mongo.A{
	mongo.M{"name": "Horny"      , "dob": time.Now().Format("20060102150405")     , "loves": mongo.A{"carrot", "papaya"}, "weight": 600, "gender": "m", "vampires": 63},
	mongo.M{"name": "Aurora"     , "dob": time.Unix(15,0).Format("20060102150405"), "loves": mongo.A{"carrot", "grape"} , "weight": 450, "gender": "f", "vampires": 43},
/*
	mongo.M{"name": "Unicrom"    , "dob": dateTime(1973, 1, 9, 22, 10), "loves": mongo.A{"energon", "redbull"}, "weight": 984, "gender": "m", "vampires": 182},
	mongo.M{"name": "Roooooodles", "dob": dateTime(1979, 7, 18, 18, 44), "loves": mongo.A{"apple"}, "weight": 575, "gender": "m", "vampires": 99},
	mongo.M{"name": "Solnara", "dob": dateTime(1985, 6, 4, 2, 1), "loves": mongo.A{"apple", "carrot", "chocolate"}, "weight": 550, "gender": "f", "vampires": 80},
	mongo.M{"name": "Ayna", "dob": dateTime(1998, 2, 7, 8, 30), "loves": mongo.A{"strawberry", "lemon"}, "weight": 733, "gender": "f", "vampires": 40},
	mongo.M{"name": "Kenny", "dob": dateTime(1997, 6, 1, 10, 42), "loves": mongo.A{"grape", "lemon"}, "weight": 690, "gender": "m", "vampires": 39},
	mongo.M{"name": "Raleigh", "dob": dateTime(2005, 4, 3, 0, 57), "loves": mongo.A{"apple", "sugar"}, "weight": 421, "gender": "m", "vampires": 2},
	mongo.M{"name": "Leia", "dob": dateTime(2001, 9, 8, 14, 53), "loves": mongo.A{"apple", "watermelon"}, "weight": 601, "gender": "f", "vampires": 33},
	mongo.M{"name": "Pilot", "dob": dateTime(1997, 2, 1, 5, 3), "loves": mongo.A{"apple", "watermelon"}, "weight": 650, "gender": "m", "vampires": 54},
	mongo.M{"name": "Nimue", "dob": dateTime(1999, 11, 20, 16, 15), "loves": mongo.A{"grape", "carrot"}, "weight": 540, "gender": "f"},
	mongo.M{"name": "Dunx", "dob": dateTime(1976, 6, 18, 18, 18), "loves": mongo.A{"grape", "watermelon"}, "weight": 704, "gender": "m", "vampires": 165},
*/
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
	iteratePrint(cursor)


	log.Print("\n\n== Show index created on _id. ==\n")
	si := db.C("system.indexes")
	cursor, err = si.Find(nil).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	iteratePrint(cursor)
	//expectFieldValues(cursor, "name", "_id_")


	err = unicorns.Insert(mongo.M{"name": "Leto", "gender": "m", "home": "Arrakeen", "worm": false})
	if err != nil {
		log.Fatal(err)
	}
	cursor, err = unicorns.Find(nil).Cursor()
	if err != nil {
		log.Fatal(err)
	}
	iteratePrint(cursor)

	log.Print("\n\n== Remove what we added to the database so far. ==\n")
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
	iteratePrint(cursor)









	log.Println("\n\n== CHAPTER 1 STOP ==\n")

}



func iteratePrint(c mongo.Cursor ){
	for c.HasNext() {
		var m mongo.M
		err := c.Next(&m)
		if err != nil {
			log.Fatal(err)
		}
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
