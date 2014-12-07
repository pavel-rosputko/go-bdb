package bdb

import "C"

type BulkData struct {
	bytes	[]byte
	offset	int
	index	int
}

func NewBulkWriter(bytes []byte) *BulkData {
	putUint32(bytes[len(bytes) - 4:], 1 << 32 - 1)
	return &BulkData{bytes, 0, len(bytes) - 4}
}

func (d *BulkData) Write(bytes []byte) bool {
	if d.offset + len(bytes) > d.index - 8 { return false }

	putUint32(d.bytes[d.index:], uint32(d.offset))
	d.index -= 4
	putUint32(d.bytes[d.index:], uint32(len(bytes)))
	d.index -= 4
	putUint32(d.bytes[d.index:], 1 << 32 - 1)

	copy(d.bytes[d.offset:], bytes)
	d.offset += len(bytes)

	return true
}

func (d *BulkData) WriteString(s string) bool {
	return d.Write([]byte(s))
}

/* func makeDBT(items [][]byte) []byte {
	slen := 0
	for _, item := range items { slen += len(item) }
	// println("slen =", slen)

	bytes := make([]byte, slen + len(items) * 4 * 2 + 4)

	index := len(bytes)
	offset := 0

	for _, item := range items {
		copy(bytes[offset:], item)
		offset += len(item)
	}

	index -= 4
	putUint32(bytes[index:], (1 << 32 - 1))

	// fmt.Println("bytes =", bytes)

	return bytes
} */


/* func (d DB) PutBulk(keys [][]byte, values [][]byte) {
	keysBytes := makeDBT(keys)
	valuesBytes := makeDBT(values)

	check(C.db_put((*C.DB)(d.p),
		(*C.DB_TXN)(nil),
		&C.DBT{data: unsafe.Pointer(&keysBytes[0]), ulen: C.u_int32_t(len(keysBytes)),
			flags: C.DB_DBT_BULK},
		&C.DBT{data: unsafe.Pointer(&valuesBytes[0]), ulen: C.u_int32_t(len(valuesBytes)),
			flags: C.DB_DBT_BULK},
		C.DB_MULTIPLE))
} */

/* func (d DB) PutBulk(keys []byte, values []byte) {
	check(C.db_put((*C.DB)(d.p),
		(*C.DB_TXN)(nil),
		&C.DBT{data: unsafe.Pointer(&keys[0]), ulen: C.u_int32_t(len(keys)),
			flags: C.DB_DBT_BULK},
		&C.DBT{data: unsafe.Pointer(&values[0]), ulen: C.u_int32_t(len(values)),
			flags: C.DB_DBT_BULK},
		C.DB_MULTIPLE))
} */


