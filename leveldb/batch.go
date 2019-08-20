// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// ErrBatchCorrupted records reason of batch corruption. This error will be
// wrapped with errors.ErrCorrupted.
type ErrBatchCorrupted struct {
	Reason string
}

func (e *ErrBatchCorrupted) Error() string {
	return fmt.Sprintf("leveldb: batch corrupted: %s", e.Reason)
}

func newErrBatchCorrupted(reason string) error {
	return errors.NewErrCorrupted(storage.FileDesc{}, &ErrBatchCorrupted{reason})
}

const (
	batchHeaderLen = 8 + 4
	batchGrowRec   = 3000
	batchBufioSize = 16
)

// BatchReplay wraps basic batch operations.
type BatchReplay interface {
	Put(key, value []byte)
	Delete(key []byte)
}

type batchIndex struct {
	keyType            keyType
	keyPos, keyLen     int
	valuePos, valueLen int
}

// 从data中取出key
func (index batchIndex) k(data []byte) []byte {
	return data[index.keyPos : index.keyPos+index.keyLen]
}

// 从data中取出value
func (index batchIndex) v(data []byte) []byte {
	if index.valueLen != 0 {
		return data[index.valuePos : index.valuePos+index.valueLen]
	}
	return nil
}

func (index batchIndex) kv(data []byte) (key, value []byte) {
	return index.k(data), index.v(data)
}

// Batch is a write batch.
type Batch struct {
	data  []byte
	index []batchIndex

	// internalLen is sums of key/value pair length plus 8-bytes internal key.
	internalLen int
}

func (b *Batch) grow(n int) {
	o := len(b.data)
	if cap(b.data)-o < n {
		div := 1
		// 一个rec最多只能有3000个batchindex？
		if len(b.index) > batchGrowRec {
			div = len(b.index) / batchGrowRec
		}
		// todo: 为什么要将len(b.data)平均分成batchGrowRec份
		ndata := make([]byte, o, o+n+o/div)
		copy(ndata, b.data)
		b.data = ndata
	}
}

// 结构: keyType|len(key)|key|len(value)|value
// 往Batch结构添加key-value
func (b *Batch) appendRec(kt keyType, key, value []byte) {
	// 1代表keyType

	// type + key length + key
	n := 1 + binary.MaxVarintLen32 + len(key)
	if kt == keyTypeVal {
		// value length + value
		n += binary.MaxVarintLen32 + len(value)
	}
	b.grow(n)
	index := batchIndex{keyType: kt}
	o := len(b.data)
	data := b.data[:o+n]
	data[o] = byte(kt)
	o++
	o += binary.PutUvarint(data[o:], uint64(len(key)))
	index.keyPos = o
	index.keyLen = len(key)
	o += copy(data[o:], key)
	// kt == keyTypeVal，代表存在value
	if kt == keyTypeVal {
		o += binary.PutUvarint(data[o:], uint64(len(value)))
		index.valuePos = o
		index.valueLen = len(value)
		o += copy(data[o:], value)
	}
	b.data = data[:o]
	// 一组key,value创建一个index
	b.index = append(b.index, index)
	b.internalLen += index.keyLen + index.valueLen + 8
}

// Put appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns but not
// before.
// 加入元素:往Batch里面追加一个keyTypeVal的元素
func (b *Batch) Put(key, value []byte) {
	b.appendRec(keyTypeVal, key, value)
}

// Delete appends 'delete operation' of the given key to the batch.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
// 删除元素:往Batch里面追加一个keyTypeDel的元素
func (b *Batch) Delete(key []byte) {
	b.appendRec(keyTypeDel, key, nil)
}

// Dump dumps batch contents. The returned slice can be loaded into the
// batch using Load method.
// The returned slice is not its own copy, so the contents should not be
// modified.
// 打印元数据
func (b *Batch) Dump() []byte {
	return b.data
}

// Load loads given slice into the batch. Previous contents of the batch
// will be discarded.
// The given slice will not be copied and will be used as batch buffer, so
// it is not safe to modify the contents of the slice.
func (b *Batch) Load(data []byte) error {
	return b.decode(data, -1)
}

// Replay replays batch contents.
func (b *Batch) Replay(r BatchReplay) error {
	// 遍历所有的下标
	// 定期更新元素
	for _, index := range b.index {
		switch index.keyType {
		case keyTypeVal:
			// 调用Put方法
			r.Put(index.k(b.data), index.v(b.data))
		case keyTypeDel:
			// 调用Delete方法
			r.Delete(index.k(b.data))
		}
	}
	return nil
}

// Len returns number of records in the batch.
func (b *Batch) Len() int {
	return len(b.index)
}

// Reset resets the batch.
func (b *Batch) Reset() {
	b.data = b.data[:0]
	b.index = b.index[:0]
	b.internalLen = 0
}

// 逐个元素回调
func (b *Batch) replayInternal(fn func(i int, kt keyType, k, v []byte) error) error {
	for i, index := range b.index {
		if err := fn(i, index.keyType, index.k(b.data), index.v(b.data)); err != nil {
			return err
		}
	}
	return nil
}

// 两个Batch合并
func (b *Batch) append(p *Batch) {
	ob := len(b.data)
	oi := len(b.index)
	b.data = append(b.data, p.data...)
	b.index = append(b.index, p.index...)
	b.internalLen += p.internalLen

	// Updating index offset.
	if ob != 0 {
		for ; oi < len(b.index); oi++ {
			index := &b.index[oi]
			index.keyPos += ob
			if index.valueLen != 0 {
				index.valuePos += ob
			}
		}
	}
}


// 对[]byte进行解码操作
func (b *Batch) decode(data []byte, expectedLen int) error {
	b.data = data
	b.index = b.index[:0]
	b.internalLen = 0
	err := decodeBatch(data, func(i int, index batchIndex) error {
		b.index = append(b.index, index)
		b.internalLen += index.keyLen + index.valueLen + 8
		return nil
	})
	if err != nil {
		return err
	}
	// 与期望的长度不符
	if expectedLen >= 0 && len(b.index) != expectedLen {
		return newErrBatchCorrupted(fmt.Sprintf("invalid records length: %d vs %d", expectedLen, len(b.index)))
	}
	return nil
}

func (b *Batch) putMem(seq uint64, mdb *memdb.DB) error {
	var ik []byte
	// 遍历Batch的所有index
	for i, index := range b.index {
		// 生成一个内部key
		ik = makeInternalKey(ik, index.k(b.data), seq+uint64(i), index.keyType)
		// 放入跳表
		// 详见memdb写入逻辑
		if err := mdb.Put(ik, index.v(b.data)); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) revertMem(seq uint64, mdb *memdb.DB) error {
	var ik []byte
	for i, index := range b.index {
		ik = makeInternalKey(ik, index.k(b.data), seq+uint64(i), index.keyType)
		if err := mdb.Delete(ik); err != nil {  // 从跳表中删除
			return err
		}
	}
	return nil
}

func newBatch() interface{} {
	return &Batch{}
}

// 读取data，逐段取出batchIndex，然后执行fn(i, index)
func decodeBatch(data []byte, fn func(i int, index batchIndex) error) error {
	var index batchIndex
	for i, o := 0, 0; o < len(data); i++ {
		// Key type.
		index.keyType = keyType(data[o])
		if index.keyType > keyTypeVal {
			return newErrBatchCorrupted(fmt.Sprintf("bad record: invalid type %#x", uint(index.keyType)))
		}
		o++

		// Key.
		x, n := binary.Uvarint(data[o:])
		o += n
		if n <= 0 || o+int(x) > len(data) {
			return newErrBatchCorrupted("bad record: invalid key length")
		}
		index.keyPos = o
		index.keyLen = int(x)
		o += index.keyLen

		// Value.
		if index.keyType == keyTypeVal {
			x, n = binary.Uvarint(data[o:])
			o += n
			if n <= 0 || o+int(x) > len(data) {
				return newErrBatchCorrupted("bad record: invalid value length")
			}
			index.valuePos = o
			index.valueLen = int(x)
			o += index.valueLen
		} else {
			index.valuePos = 0
			index.valueLen = 0
		}

		// 从data结构里面逐段读出，取出index
		if err := fn(i, index); err != nil {
			return err
		}
	}
	return nil
}

func decodeBatchToMem(data []byte, expectSeq uint64, mdb *memdb.DB) (seq uint64, batchLen int, err error) {
	// 从data中解析出seq和batchLen
	seq, batchLen, err = decodeBatchHeader(data)
	if err != nil {
		return 0, 0, err
	}
	if seq < expectSeq {
		return 0, 0, newErrBatchCorrupted("invalid sequence number")
	}
	data = data[batchHeaderLen:]
	var ik []byte
	var decodedLen int
	err = decodeBatch(data, func(i int, index batchIndex) error {
		if i >= batchLen {
			return newErrBatchCorrupted("invalid records length")
		}
		ik = makeInternalKey(ik, index.k(data), seq+uint64(i), index.keyType)
		// 逐个插入到memdb里面
		if err := mdb.Put(ik, index.v(data)); err != nil {
			return err
		}
		decodedLen++
		return nil
	})
	if err == nil && decodedLen != batchLen {
		err = newErrBatchCorrupted(fmt.Sprintf("invalid records length: %d vs %d", batchLen, decodedLen))
	}
	return
}

// 将seq和batchLen写入到dst
func encodeBatchHeader(dst []byte, seq uint64, batchLen int) []byte {
	dst = ensureBuffer(dst, batchHeaderLen)  // 分配一个足够的buffer
	binary.LittleEndian.PutUint64(dst, seq)  // 将seq写进去
	binary.LittleEndian.PutUint32(dst[8:], uint32(batchLen))  // 把index个数也写进去
	return dst
}

// 从dst中获取seq和batchLen
func decodeBatchHeader(data []byte) (seq uint64, batchLen int, err error) {
	if len(data) < batchHeaderLen {
		return 0, 0, newErrBatchCorrupted("too short")
	}

	seq = binary.LittleEndian.Uint64(data)
	batchLen = int(binary.LittleEndian.Uint32(data[8:]))
	if batchLen < 0 {
		return 0, 0, newErrBatchCorrupted("invalid records length")
	}
	return
}

// batches有多少个index
func batchesLen(batches []*Batch) int {
	batchLen := 0
	for _, batch := range batches {
		batchLen += batch.Len()
	}
	return batchLen
}

// 把encodeBatchHeader写入到wr里面
func writeBatchesWithHeader(wr io.Writer, batches []*Batch, seq uint64) error {
	// wr多数是journal writer
	// 先写seq no，再写*Batch的个数
	if _, err := wr.Write(encodeBatchHeader(nil, seq, batchesLen(batches))); err != nil {
		return err
	}
	for _, batch := range batches {
		// 这个Batch.data写入
		// key-value结构详见batch.go
		if _, err := wr.Write(batch.data); err != nil {
			return err
		}
	}
	return nil
}
