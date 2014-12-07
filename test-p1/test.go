package main

import (
	"exec"
	"fmt"
	// "time"

	"g/bdb"
)

var env *bdb.Env
var db *bdb.Db

func clear() {
	c, _ := exec.Run("/bin/sh", []string{"", "-c", "rm ../env/*"}, nil, "",
		exec.DevNull, exec.DevNull, exec.DevNull)
	c.Wait(0)
}

func put() {
	txn, e := env.BeginTxn()
	if e != nil { panic(e) }

	defer func() {
		e := recover()
		if e != nil {
			fmt.Println("e =", e)
			txn.Abort()
		}
	}()


	tdb := txn.Assoc(db)

	for i := 0; i < 500; i++ {
		key := []byte("key")
		value := []byte(fmt.Sprintf("value%d", i))
		err := tdb.Put(key, value)
		if err != nil { panic(err) }
	}

	txn.Commit()
}

func main() {
	clear()
	env, _ = bdb.OpenEnv("../env", bdb.Create | bdb.InitMPool | bdb.InitLock | bdb.InitTxn); defer env.Close()
	db, _ = env.OpenDbFlags("db", bdb.BTree, bdb.Create | bdb.AutoCommit, bdb.Dup); defer db.Close()

	for i := 0; i < 10000; i++ {
		put()
		println("put")
		// time.Sleep(5 * 1e9)
	}
}
