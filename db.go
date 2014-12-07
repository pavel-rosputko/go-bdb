package bdb

import (
	"unsafe"
	"os"
)

// #include <stdlib.h>
// #include <db.h>
// #include "db-go.h"
import "C"

const (
	Create = uint32(C.DB_CREATE)
	Thread = uint32(C.DB_THREAD)
	RdOnly = uint32(C.DB_RDONLY)

	InitMPool = uint32(C.DB_INIT_MPOOL)
	InitLock = uint32(C.DB_INIT_LOCK)
	InitTxn = uint32(C.DB_INIT_TXN)
	InitLog = uint32(C.DB_INIT_LOG)

	InitCDB = uint32(C.DB_INIT_CDB)

	BTree = int(C.DB_BTREE)
	Hash = int(C.DB_HASH)
	RecNo = int(C.DB_RECNO)

	Dup = uint32(C.DB_DUP)
	AutoCommit = uint32(C.DB_AUTO_COMMIT)
)

type Error int

func (e Error) String() string {
	return C.GoString(C.db_strerror(C.int(e)))
}

type Env struct { p *C.DB_ENV }

func OpenEnv(path string, flags uint32) (*Env, os.Error) {
	var Cenv *C.DB_ENV
	ret := C.db_env_create(&Cenv, 0)
	if ret != 0 { return nil, Error(ret) }

	Cpath := C.CString(path); defer C.free(unsafe.Pointer(Cpath))
	ret = C.db_env_open(Cenv, Cpath, C.u_int32_t(flags), 0)
	if ret != 0 { return nil, Error(ret) }

	return &Env{Cenv}, nil
}

func (e *Env) Close() os.Error {
	ret := C.db_env_close(e.p, 0)
	if ret != 0 { return Error(ret) }
	return nil
}

type Db struct { p unsafe.Pointer }

func (e *Env) OpenDbFlags(filename string dbType int, flags, dbFlags uint32) (*Db, os.Error) {
	var Cdb *C.DB
	ret := C.db_create(&Cdb, e.p, 0)
	if ret != 0 { return nil, Error(ret) }

	if dbFlags != 0 {
		ret := C.db_set_flags(Cdb, C.u_int32_t(dbFlags))
		if ret != 0 { return nil, Error(ret) }
	}

	Cfilename := C.CString(filename); defer C.free(unsafe.Pointer(Cfilename))
	ret = C.db_open(Cdb, (*C.DB_TXN)(nil), Cfilename,
	  (*C.char)(nil), C.DBTYPE(dbType), C.u_int32_t(flags), 0)
	if ret != 0 { return nil, Error(ret) }

	return &Db{unsafe.Pointer(Cdb)}, nil
}

func (e *Env) OpenDb(filename string, dbType int, flags uint32) (*Db, os.Error) {
	return e.OpenDbFlags(filename, dbType, flags, 0)
}

func (d *Db) Close() (err os.Error) {
	ret := C.db_close((*C.DB)(d.p), 0)
	if ret != 0 { err = Error(ret) }
	return
}

func (d *Db) PutTxn(key, value []byte, txn *Txn) (err os.Error) {
	ret := C.db_put((*C.DB)(d.p), txn.p,
		&C.DBT{data: unsafe.Pointer(&key[0]), size: C.u_int32_t(len(key))},
		&C.DBT{data: unsafe.Pointer(&value[0]), size: C.u_int32_t(len(value))},
		0)
	if ret != 0 { err = Error(ret) }
	return
}

func (d *Db) Put(key, value []byte) os.Error {
	return d.PutTxn(key, value, &Txn{nil})
}

// NOTE data underying the returned slice may be changed after next call
func (d *Db) GetSliceTxn(key []byte, txn *Txn) ([]byte, os.Error) {
	Cvalue := C.DBT{}
	ret := C.db_get((*C.DB)(d.p), txn.p,
		&C.DBT{data: unsafe.Pointer(&key[0]), size: C.u_int32_t(len(key))},
		&Cvalue, 0)

	if ret == C.DB_NOTFOUND { return nil, nil }
	if ret != 0 { return nil, Error(ret) }

	return (*[1<<30]byte)(unsafe.Pointer(Cvalue.data))[:int(Cvalue.size)], nil
}

func (d *Db) GetSlice(key []byte) ([]byte, os.Error) {
	return d.GetSliceTxn(key, &Txn{nil})
}

func (d *Db) Get(key, value []byte) (err os.Error) {
	ret := C.db_get((*C.DB)(d.p),
		(*C.DB_TXN)(nil),
		&C.DBT{data: unsafe.Pointer(&key[0]), size: C.u_int32_t(len(key))},
		&C.DBT{data: unsafe.Pointer(&value[0]), ulen: C.u_int32_t(len(value)), flags: C.DB_DBT_USERMEM},
		0)
	if ret != 0 { err = Error(ret) }
	return
}

// another approach: use DB_DBT_MALLOC, one call, copy to []byte
// value can be changed between calls so loop is needed
func (d *Db) GetAllTxn(key []byte, txn *Txn) ([]byte, os.Error) {
	Cvalue := C.DBT{flags: C.DB_DBT_USERMEM}

	var value []byte
	for {
		ret := C.db_get((*C.DB)(d.p), txn.p,
			&C.DBT{data: unsafe.Pointer(&key[0]), size: C.u_int32_t(len(key))},
			&Cvalue, 0)

		if ret == 0 { break }
		if ret == C.DB_NOTFOUND { return nil, nil }
		if ret == C.DB_BUFFER_SMALL {
			value = make([]byte, Cvalue.size)
			Cvalue.data = unsafe.Pointer(&value[0])
			Cvalue.ulen = C.u_int32_t(len(value))
		} else {
			return nil, Error(ret)
		}
	}

	return value, nil
}

type Txn struct { p *C.DB_TXN }

func (d *Db) GetAll(key []byte) ([]byte, os.Error) {
	return d.GetAllTxn(key, &Txn{nil})
}

func (d *Db) GetPageSize() (uint32, os.Error) {
	var Cpagesize C.u_int32_t
	ret := C.db_get_pagesize((*C.DB)(d.p), &Cpagesize)
	if ret != 0 { return 0, Error(ret) }
	return uint32(Cpagesize), nil
}

func unpackUint32(b []byte) uint32 {
        return uint32(b[3]) << 24 | uint32(b[2]) << 16 | uint32(b[1]) << 8 | uint32(b[0])
}

func (d *Db) GetDupsAllTxn(key []byte, txn *Txn) ([][]byte, os.Error) {
	pageSize, err := d.GetPageSize()
	if err != nil { return nil, err }

	Ckey := C.DBT{data: unsafe.Pointer(&key[0]), size: C.u_int32_t(len(key))}

	value := make([]byte, pageSize)
	Cvalue := C.DBT{data: unsafe.Pointer(&value[0]), ulen: C.u_int32_t(pageSize),
		flags: C.DB_DBT_USERMEM}

	for {
		ret := C.db_get((*C.DB)(d.p), txn.p, &Ckey, &Cvalue, C.DB_MULTIPLE)

		if ret == 0 { break }
		if ret == C.DB_NOTFOUND { return [][]byte{}, nil }
		if ret == C.DB_BUFFER_SMALL {
			size := Cvalue.size / 1024 * 1024
			if Cvalue.size % 1024 != 0 { size += 1024 }

			if size <= Cvalue.ulen { // XXX estimated buffer size problem ?
				size = Cvalue.ulen * 2
			}

			value = make([]byte, size) // realloc ?
			Cvalue.data = unsafe.Pointer(&value[0])
			Cvalue.ulen = size
		} else {
			return nil, Error(ret)
		}
	}

	i := int(Cvalue.ulen) - 4
	count := 0
	for {
		offset := unpackUint32(value[i:])
		if int32(offset) == -1 { break }
		i -= 8
		count++
	}

	values := make([][]byte, count)
	i = int(Cvalue.ulen) - 4
	ii := 0
	for {
		offset := unpackUint32(value[i:])
		if int32(offset) == -1 { break }
		i -= 4
		length := unpackUint32(value[i:])
		i -= 4
		values[ii] = value[offset : offset + length]
		ii++
	}

	return values, nil
}

func (d *Db) GetDupsAll(key []byte) ([][]byte, os.Error) {
	return d.GetDupsAllTxn(key, &Txn{nil})
}

func (d *Db) AppendTxn(value []byte, txn *Txn) (uint32, os.Error) {
	Ckey := C.DBT{}
	Cvalue := C.DBT{data: unsafe.Pointer(&value[0]), size: C.u_int32_t(len(value))}
	ret := C.db_put((*C.DB)(d.p), txn.p, &Ckey, &Cvalue, C.DB_APPEND)
	if ret != 0 { return 0, Error(ret) }

	if Ckey.size != 4 { panic("key size assumption is wrong") }
	// I think Ckey.size is 4

	key := (*[1<<30]byte)(unsafe.Pointer(Ckey.data))[:int(Ckey.size)]
	return unpackUint32(key), nil
}

func (d *Db) Append(value []byte) (uint32, os.Error) {
	return d.AppendTxn(value, &Txn{nil})
}

// NOTE or HasKey
func (d *Db) ExistsTxn(key []byte, txn *Txn) (bool, os.Error) {
	Ckey := C.DBT{data: unsafe.Pointer(&key[0]), size: C.u_int32_t(len(key))}
	ret := C.db_exists((*C.DB)(d.p), txn.p, &Ckey, 0)
	if ret == C.DB_NOTFOUND { return false, nil }
	if ret != 0 { return false, Error(ret) }
	return true, nil
}

func (d *Db) Exists(key []byte) (bool, os.Error) {
	return d.ExistsTxn(key, &Txn{nil})
}

func (e *Env) BeginTxn() (*Txn, os.Error) {
	var Ctxn *C.DB_TXN
	ret := C.db_env_txn_begin(e.p, (*C.DB_TXN)(nil), &Ctxn, 0)
	if ret != 0 { return nil, Error(ret) }
	return &Txn{Ctxn}, nil
}

func (t *Txn) Commit() os.Error {
	res := C.txn_commit(t.p, 0)
	if res != 0 { return Error(res) }
	return nil
}

func (t *Txn) Abort() os.Error {
        res := C.txn_abort(t.p)
	if res != 0 { return Error(res) }
	return nil
}

type TxnDb struct {
	txn	*Txn
	db	*Db
}

func (t *Txn) Assoc(db *Db) *TxnDb {
	return &TxnDb{t, db}
}

func (d *TxnDb) Put(key, value []byte) os.Error {
	return d.db.PutTxn(key, value, d.txn)
}

