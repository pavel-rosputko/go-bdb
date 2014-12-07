package bdb

import (
	"exec"
	"fmt"
	"runtime"
	"testing"
	"time"
)

func init() {
	runtime.GOMAXPROCS(4)
}

func clear() {
	c, _ := exec.Run("/bin/sh", []string{"", "-c", "rm db/*"}, nil, "",
		exec.DevNull, exec.DevNull, exec.DevNull)
	c.Wait(0)
}

func info() {
	c, _ := exec.Run("/bin/sh", []string{"", "-c", "db_stat -d db/db | grep 'unique keys'"}, nil, "",
		exec.DevNull, exec.PassThrough, exec.DevNull)
	c.Wait(0)
}
func TestGetAll(t *testing.T) {
	clear()
	e, _ := OpenEnv("db", Create | InitMPool); defer e.Close()
	d, _ := e.OpenDb("db", BTree, Create); defer d.Close()

	key := []byte("key")
	d.Put(key, []byte("value1"))

	value, err := d.GetAll(key)
	if err != nil { t.Fatal(err) }
	if string(value) != "value1" { t.FailNow() }
}

func TestMultiThreadedSet(t *testing.T) {
	clear()
	e, _ := OpenEnv("db", Create | InitMPool | InitCDB | Thread); defer e.Close()
	d, _ := e.OpenDb("db", BTree, Create | Thread); defer d.Close()

	c := make(chan bool)
	n := 100
	f := func() {
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprint("key", i))
			value := []byte(fmt.Sprint("value", i))

			d.Put(key, value)
		}
		c <- true
	}

	go f()
	go f()

	<-c
	<-c
}

func TestDup(t *testing.T) {
	clear()
	e, _ := OpenEnv("db", Create | InitMPool); defer e.Close()
	d, _ := e.OpenDbFlags("db", BTree, Create, Dup); defer d.Close()

	key := []byte("key")
	d.Put(key, []byte("value1"))
	d.Put(key, []byte("value2"))

	value, _ := d.GetSlice(key)
	if string(value) != "value1" { t.FailNow() }
}

func TestTxnBegin(t *testing.T) {
	clear()
	e, _ := OpenEnv("db", Create | InitMPool | InitTxn); defer e.Close()
	d, _ := e.OpenDb("db", BTree, Create | AutoCommit); defer d.Close()

	txn, err := e.BeginTxn()
	if err != nil { t.Fatal(err) }

	td := txn.Assoc(d)
	err = td.Put([]byte("key"), []byte("value"))
	if err != nil { t.Fatal(err) }

	err = txn.Commit()
	if err != nil { t.Fatal(err) }
}

func TestGetDupsAll(t *testing.T) {
	clear()
	e, _ := OpenEnv("db", Create | InitMPool); defer e.Close()
	d, _ := e.OpenDbFlags("db", BTree, Create, Dup); defer d.Close()

	key := []byte("key")
	d.Put(key, []byte("value2"))
	d.Put(key, []byte("value1"))

	values, err := d.GetDupsAll(key)
	if err != nil { t.Fatal(err) }

	if len(values) != 2 ||
			string(values[0]) != "value2" ||
			string(values[1]) != "value1" {
		t.Fatal("")
	}
}

func TestAppend(t *testing.T) {
	clear()
	e, _ := OpenEnv("db", Create | InitMPool); defer e.Close()
	d, _ := e.OpenDb("db", RecNo, Create); defer d.Close()

	key, err := d.Append([]byte("value"))
	if err != nil { t.Fatal(err) }
	if key != 1 { t.FailNow() }

	key, err = d.Append([]byte("value"))
	if err != nil { t.Fatal(err) }
	if key != 2 { t.FailNow() }
}

func TestExists(t *testing.T) {
	clear()
	e, _ := OpenEnv("db", Create | InitMPool); defer e.Close()
	d, _ := e.OpenDb("db", BTree, Create); defer d.Close()

	f, err := d.Exists([]byte("key"))
	if err != nil { t.Fatal("exists failed", err) }
	if f { t.Fatal("") }

	err = d.Put([]byte("key"), []byte("value"))
	if err != nil { t.Fatal(err) }

	f, err = d.Exists([]byte("key"))
	if err != nil { t.Fatal(err) }
	if !f { t.Fatal("") }
}

func BenchmarkPut(bm *testing.B) {
	clear()
	e, _ := OpenEnv("db", Create | InitMPool); defer e.Close()
	d, _ := e.OpenDb("db", BTree, Create); defer d.Close()

	t := time.Nanoseconds()
	for i := 0; i < bm.N; i++ {
		data := []byte(fmt.Sprint(i))
		d.Put(data, data)
	}

	tt := time.Nanoseconds() - t
	fmt.Printf("time = %f, rate = %f\n", float64(tt) / 1e9, float64(bm.N) * 1e9 / float64(tt))

	info()
}

