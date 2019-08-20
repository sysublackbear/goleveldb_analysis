// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package memdb provides in-memory key/value database implementation.
package memdb

import (
	"math/rand"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Common errors.
var (
	ErrNotFound     = errors.ErrNotFound
	ErrIterReleased = errors.New("leveldb/memdb: iterator released")
)

const tMaxHeight = 12

type dbIter struct {
	util.BasicReleaser
	p          *DB
	slice      *util.Range  // Start, Limit []byte
	node       int
	forward    bool
	key, value []byte
	err        error
}
// Node data:
// [0]         : KV offset
// [1]         : Key length
// [2]         : Value length
// [3]         : Height
// [3..height] : Next nodes

// nodeData只记录了key和value的下标，真正的数据在kvData
// 通过node参数，在kvData中解析出key和value
func (i *dbIter) fill(checkStart, checkLimit bool) bool {
	if i.node != 0 {
		n := i.p.nodeData[i.node]  // nodeData  []int
		m := n + i.p.nodeData[i.node+nKey]  // nKey: 1
		i.key = i.p.kvData[n:m]
		if i.slice != nil {
			switch {
			// 必须满足的条件: start <= key < limit
			// 是否检查Limit
			case checkLimit && i.slice.Limit != nil && i.p.cmp.Compare(i.key, i.slice.Limit) >= 0:
				fallthrough
			case checkStart && i.slice.Start != nil && i.p.cmp.Compare(i.key, i.slice.Start) < 0:
				i.node = 0
				goto bail
			}
		}
		// kvData []byte
		i.value = i.p.kvData[m : m+i.p.nodeData[i.node+nVal]]
		return true
	}
bail:
	i.key = nil
	i.value = nil
	return false
}

func (i *dbIter) Valid() bool {
	return i.node != 0
}

func (i *dbIter) First() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil {
		// 在规定的slice里面找出大于Start的元素
		i.node, _ = i.p.findGE(i.slice.Start, false)
	} else {
		// 假定起始点start = 0
		i.node = i.p.nodeData[nNext]
	}
	return i.fill(false, true)
}

func (i *dbIter) Last() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Limit != nil {
		i.node = i.p.findLT(i.slice.Limit)  // 在跳表里面找出少于i.slice.Limit的值
	} else {
		i.node = i.p.findLast()
	}
	return i.fill(true, false)
}

func (i *dbIter) Seek(key []byte) bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil && i.p.cmp.Compare(key, i.slice.Start) < 0 {
		key = i.slice.Start
	}
	i.node, _ = i.p.findGE(key, false)
	return i.fill(false, true)
}

func (i *dbIter) Next() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if !i.forward {
			return i.First()
		}
		return false
	}
	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	i.node = i.p.nodeData[i.node+nNext]
	return i.fill(false, true)
}

func (i *dbIter) Prev() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if i.forward {
			return i.Last()
		}
		return false
	}
	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	i.node = i.p.findLT(i.key)
	return i.fill(true, false)
}

func (i *dbIter) Key() []byte {
	return i.key
}

func (i *dbIter) Value() []byte {
	return i.value
}

func (i *dbIter) Error() error { return i.err }

func (i *dbIter) Release() {
	if !i.Released() {
		i.p = nil
		i.node = 0
		i.key = nil
		i.value = nil
		i.BasicReleaser.Release()
	}
}

const (
	nKV = iota
	nKey
	nVal
	nHeight
	nNext
)

// DB is an in-memory key/value database.
type DB struct {
	cmp comparer.BasicComparer
	rnd *rand.Rand

	mu     sync.RWMutex
	kvData []byte
	// Node data:
	// [0]         : KV offset
	// [1]         : Key length
	// [2]         : Value length
	// [3]         : Height
	// [3..height] : Next nodes
	nodeData  []int
	prevNode  [tMaxHeight]int
	maxHeight int
	n         int
	kvSize    int
}
// DB更像是一个跳表+压缩列表

func (p *DB) randHeight() (h int) {
	const branching = 4
	h = 1
	// 不能超过12层，不能是4的倍数?
	for h < tMaxHeight && p.rnd.Int()%branching == 0 {
		h++
	}
	return
}

// Must hold RW-lock if prev == true, as it use shared prevNode slice.
// 在跳表里面寻找出比key要大的原始
// nodeData里面的值相当于在kvData上面的索引
func (p *DB) findGE(key []byte, prev bool) (int, bool) {
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData[node+nNext+h]  // h代表间距跳表的层数越高，间距越大
		cmp := 1
		if next != 0 {
			o := p.nodeData[next]
			// 取出key值
			cmp = p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key)
		}
		if cmp < 0 {
			// Keep searching in this list
			node = next  // 搜寻下一个节点
		} else {
			if prev {  // 是否需要记录前一个节点
				p.prevNode[h] = node
			} else if cmp == 0 {  // 相等
				return next, true  // 题目要求是大于或等于，因此可以返回
			}
			if h == 0 {
				return next, cmp == 0
			}
			h--  // 树高减1
		}
	}
}

func (p *DB) findLT(key []byte) int {
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData[node+nNext+h]
		o := p.nodeData[next]
		if next == 0 || p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key) >= 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

func (p *DB) findLast() int {
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData[node+nNext+h]
		if next == 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
//
// It is safe to modify the contents of the arguments after Put returns.
func (p *DB) Put(key []byte, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if node, exact := p.findGE(key, true); exact {
		// 往kvData写入数据
		kvOffset := len(p.kvData)
		p.kvData = append(p.kvData, key...)
		p.kvData = append(p.kvData, value...)
		p.nodeData[node] = kvOffset  // 在node记住kv的偏移
		m := p.nodeData[node+nVal]  // m := p.nodeData[node+2]
		p.nodeData[node+nVal] = len(value)
		p.kvSize += len(value) - m  // 覆盖
		return nil
	}

	h := p.randHeight()
	if h > p.maxHeight {
		for i := p.maxHeight; i < h; i++ {
			p.prevNode[i] = 0
		}
		p.maxHeight = h
	}

	kvOffset := len(p.kvData)
	p.kvData = append(p.kvData, key...)
	p.kvData = append(p.kvData, value...)
	// Node
	node := len(p.nodeData)  // nodeData的长度
	// 依次放入: kvOffset, len(key), len(value), h
	p.nodeData = append(p.nodeData, kvOffset, len(key), len(value), h)
	for i, n := range p.prevNode[:h] {
		m := n + nNext + i
		p.nodeData = append(p.nodeData, p.nodeData[m])
		p.nodeData[m] = node
	}

	p.kvSize += len(key) + len(value)
	p.n++  // 新写入节点的情况,n++
	return nil
}
// 结构：
// nodeData:kvOffset1|len(key1)|len(value1)|h|kvOffset2|len(key2)|len(value2)|h
// kvData: key1|value1|key2|value2|...|keyN|valueN

// Delete deletes the value for the given key. It returns ErrNotFound if
// the DB does not contain the key.
//
// It is safe to modify the contents of the arguments after Delete returns.
// 跳表删除
func (p *DB) Delete(key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 找出大于或等于key的node节点
	node, exact := p.findGE(key, true)
	if !exact {
		return ErrNotFound
	}

	h := p.nodeData[node+nHeight]  // 找出下一个节点,记为h
	for i, n := range p.prevNode[:h] {
		// 调整对应的每一层节点
		m := n + nNext + i
		p.nodeData[m] = p.nodeData[p.nodeData[m]+nNext+i]
	}

	p.kvSize -= p.nodeData[node+nKey] + p.nodeData[node+nVal]
	p.n--
	return nil
}

// Contains returns true if the given key are in the DB.
//
// It is safe to modify the contents of the arguments after Contains returns.
func (p *DB) Contains(key []byte) bool {
	p.mu.RLock()
	_, exact := p.findGE(key, false)
	p.mu.RUnlock()
	return exact
}

// Get gets the value for the given key. It returns error.ErrNotFound if the
// DB does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (p *DB) Get(key []byte) (value []byte, err error) {
	p.mu.RLock()
	if node, exact := p.findGE(key, false); exact {
		o := p.nodeData[node] + p.nodeData[node+nKey]
		value = p.kvData[o : o+p.nodeData[node+nVal]]  // p.kvData[o:o+p.nodeData[next+nKey]]记录的是len(value)
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// Find finds key/value pair whose key is greater than or equal to the
// given key. It returns ErrNotFound if the table doesn't contain
// such pair.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Find returns.
// 跟Get(key []byte)操作类似,只是返回的是(key, value, err)
func (p *DB) Find(key []byte) (rkey, value []byte, err error) {
	p.mu.RLock()
	if node, _ := p.findGE(key, false); node != 0 {
		n := p.nodeData[node]
		m := n + p.nodeData[node+nKey]
		rkey = p.kvData[n:m]
		value = p.kvData[m : m+p.nodeData[node+nVal]]
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// NewIterator returns an iterator of the DB.
// The returned iterator is not safe for concurrent use, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently with modifying its
// underlying DB. However, the resultant key/value pairs are not guaranteed
// to be a consistent snapshot of the DB at a particular point in time.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// DB. And a nil Range.Limit is treated as a key after all keys in
// the DB.
//
// WARNING: Any slice returned by interator (e.g. slice returned by calling
// Iterator.Key() or Iterator.Key() methods), its content should not be modified
// unless noted otherwise.
//
// The iterator must be released after use, by calling Release method.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (p *DB) NewIterator(slice *util.Range) iterator.Iterator {
	return &dbIter{p: p, slice: slice}
}

// Capacity returns keys/values buffer capacity.
func (p *DB) Capacity() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cap(p.kvData)
}

// Size returns sum of keys and values length. Note that deleted
// key/value will not be accounted for, but it will still consume
// the buffer, since the buffer is append only.
func (p *DB) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.kvSize
}

// Free returns keys/values free buffer before need to grow.
// 空余空间
func (p *DB) Free() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cap(p.kvData) - len(p.kvData)
}

// Len returns the number of entries in the DB.
// 节点个数
func (p *DB) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.n
}

// Reset resets the DB to initial empty state. Allows reuse the buffer.
func (p *DB) Reset() {
	p.mu.Lock()
	p.rnd = rand.New(rand.NewSource(0xdeadbeef))
	p.maxHeight = 1
	p.n = 0
	p.kvSize = 0
	p.kvData = p.kvData[:0]
	p.nodeData = p.nodeData[:nNext+tMaxHeight]
	p.nodeData[nKV] = 0
	p.nodeData[nKey] = 0
	p.nodeData[nVal] = 0
	p.nodeData[nHeight] = tMaxHeight
	for n := 0; n < tMaxHeight; n++ {
		p.nodeData[nNext+n] = 0
		p.prevNode[n] = 0
	}
	p.mu.Unlock()
}

// New creates a new initialized in-memory key/value DB. The capacity
// is the initial key/value buffer capacity. The capacity is advisory,
// not enforced.
//
// This DB is append-only, deleting an entry would remove entry node but not
// reclaim KV buffer.
//
// The returned DB instance is safe for concurrent use.
func New(cmp comparer.BasicComparer, capacity int) *DB {
	p := &DB{
		cmp:       cmp,
		rnd:       rand.New(rand.NewSource(0xdeadbeef)),
		maxHeight: 1,
		kvData:    make([]byte, 0, capacity),
		nodeData:  make([]int, 4+tMaxHeight),
	}
	p.nodeData[nHeight] = tMaxHeight
	return p
}
