package main

import (
	"time"
	"fmt"
	"os"
)

type Ball struct{ hits int }

func main() {
    table := make(chan *Ball)
    go player("ping", table)
    go player("pong", table)

    table <- new(Ball) // game on; toss the ball
    time.Sleep(1 * time.Second)
    <-table // game over; grab the ball

    fmt.Println("main program terminated")

}

func player(name string, table chan *Ball) {
	//defer  func d1(name string){ fmt.Println(name, "terminated") }()
	 defer func() { 
		log1("stop")
		//fmt.Println(name, "child terminated")  
	}() 
	log1(  fmt.Sprint("start ", name) )

    for {
        ball := <-table
        ball.hits++
        fmt.Println(name, ball.hits)
        time.Sleep(100 * time.Millisecond)
        table <- ball
    }
	return

}


func a() {
    i := 0
    defer fmt.Println(i)
    i++
    return
}

func log1(msg string) {
    fo, err := os.Create("/tmp/output.txt")
    if err != nil { panic(err) }
    defer func() {
        if err := fo.Close(); err != nil {
            panic(err)
        }
    }()

    // make a buffer to keep chunks that are read
    //buf := make([]byte, 1024)
    buf := []byte(  fmt.Sprintln(msg) )
        // write a chunk
	if _, err := fo.Write(buf[:len(buf)]); err != nil {
		fmt.Println("error while writing")
		panic(err)
	}


}
