package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"
	"runtime"

	"g/bdb"
)

func init() {
	runtime.GOMAXPROCS(2)
}

var env *bdb.Env
var db *bdb.Db
var mutex sync.RWMutex

func get() {
	mutex.RLock(); defer func() { mutex.RUnlock(); println("runlock") }()
	key := []byte("key")
	values, err := db.GetDupsAll(key)
	if err != nil { panic(err) }
	println("count =", len(values))
	println("got")
}

func closer() {
        for {
                s := <-signal.Incoming
		
		fmt.Println("s =", s)

                if int32(s.(signal.UnixSignal)) == 2 {
			//mutex.Lock()
			err := db.Close()
			if err != nil { fmt.Println("db close err =", err) }
			err = env.Close()
			if err != nil { fmt.Println("env close err =", err) }
			println("closed")
                        // os.Exit(1)
			println("after-exit")
                }
        }
}


func main() {
	go closer()

	var err os.Error
	env, err = bdb.OpenEnv("../env", bdb.Create | bdb.InitMPool | bdb.Thread)
	if err != nil { panic(err) }
	db, err = env.OpenDb("db", bdb.BTree, bdb.RdOnly | bdb.Thread)
	if err != nil { panic(err) }

	for i := 0; i < 1000; i++ {
		get()
		time.Sleep(1e8)
	}
}
