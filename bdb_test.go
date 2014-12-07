package bdb

import "time"
import "fmt"
import "testing"
import "runtime"

import "io/ioutil"

func init() {
	runtime.GOMAXPROCS(2)
}

var (
	key = []byte("key")
	value = []byte("value")
)

func Test(t *testing.T) {
	e := OpenEnv("db", Create | InitMPool); defer e.Close()
	db := e.OpenDb("db", BTree, Create); defer db.Close()
	fmt.Println(db)

	// var all [][]byte
	n := 1000
	for i := 0; i < n; i++ {
		db.Put(key, value)
		gotValue := db.Get(key)
		// all = append(all, gotValue)
		if string(value) != string(gotValue) { t.Fatal("") }

	}
	println("done")

	time.Sleep(15 * 1000000000)
	return

	// all = nil
	println("niled")
	runtime.GC()


	time.Sleep(15 * 1000000000)
}

func TestManyKeys(t *testing.T) {
	go func() {
		for {
			println("1")
			time.Sleep(10 * 1000000)
		}
	}()

	e := OpenEnv("db", Create | InitMPool); defer e.Close()
	db := e.OpenDb("db", BTree, Create); defer db.Close()

	n := 10000
	var all [][]byte
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprint("key", i))
		value := []byte(fmt.Sprint("value", i))
		println("value =", string(value))
		db.Put(key, value)
		gotValue := db.Get(key)
		println("gotValue =", string(gotValue))

		all = append(all, gotValue)
		if string(value) != string(gotValue) { t.Fatal("") }
	}

	println("done")
	for _, value := range all {
		println(string(value))
	}
}

func Test2(t *testing.T) {
	ch1, ch2 := make(chan bool), make(chan bool)
	go func() {
		for {
			bytes, _ := ioutil.ReadFile("bigfile")
			println("2", len(bytes))
		}
		ch2 <- true
	}()

	i := 0
	go func() {
		for {
			i++
			// time.Sleep(1000000)
			if i == 1000000000 { println("tick"); i = 0 }
		}
		ch1 <- true
	}()

	println("i =", i)
	println("grc =", runtime.Goroutines())

	<-ch1
	<-ch2
}
