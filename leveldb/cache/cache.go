// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package cache provides interface and implementation of a cache algorithms.
package cache

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/util"
)

// Cacher provides interface to implements a caching functionality.
// An implementation must be safe for concurrent use.
type Cacher interface {
	// Capacity returns cache capacity.
	Capacity() int

	// SetCapacity sets cache capacity.
	SetCapacity(capacity int)

	// Promote promotes the 'cache node'.
	Promote(n *Node)

	// Ban evicts the 'cache node' and prevent subsequent 'promote'.
	Ban(n *Node)

	// Evict evicts the 'cache node'.
	Evict(n *Node)

	// EvictNS evicts 'cache node' with the given namespace.
	EvictNS(ns uint64)

	// EvictAll evicts all 'cache node'.
	EvictAll()

	// Close closes the 'cache tree'
	Close() error
}

// Value is a 'cacheable object'. It may implements util.Releaser, if
// so the the Release method will be called once object is released.
type Value interface{}

// NamespaceGetter provides convenient wrapper for namespace.
type NamespaceGetter struct {
	Cache *Cache
	NS    uint64
}

// Get simply calls Cache.Get() method.
func (g *NamespaceGetter) Get(key uint64, setFunc func() (size int, value Value)) *Handle {
	return g.Cache.Get(g.NS, key, setFunc)
}

// The hash tables implementation is based on:
// "Dynamic-Sized Nonblocking Hash Tables", by Yujie Liu,
// Kunlong Zhang, and Michael Spear.
// ACM Symposium on Principles of Distributed Computing, Jul 2014.

const (
	mInitialSize           = 1 << 4
	mOverflowThreshold     = 1 << 5
	mOverflowGrowThreshold = 1 << 7
)

type mBucket struct {
	mu     sync.Mutex
	node   []*Node
	frozen bool
}

func (b *mBucket) freeze() []*Node {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.frozen {
		b.frozen = true
	}
	return b.node
}

func (b *mBucket) get(r *Cache, h *mNode, hash uint32, ns, key uint64, noset bool) (done, added bool, n *Node) {
	b.mu.Lock()

	if b.frozen {
		b.mu.Unlock()
		return
	}

	// Scan the node.
	for _, n := range b.node {
		// 比较hash,ns,key
		if n.hash == hash && n.ns == ns && n.key == key {
			atomic.AddInt32(&n.ref, 1)  // 引用计数加1
			b.mu.Unlock()
			return true, false, n
		}
	}

	// Get only.
	// 只获取，不设置
	if noset {
		b.mu.Unlock()
		return true, false, nil
	}

	// Create node.
	n = &Node{
		r:    r,
		hash: hash,
		ns:   ns,
		key:  key,
		ref:  1,
	}
	// Add node to bucket.
	b.node = append(b.node, n)
	bLen := len(b.node)
	b.mu.Unlock()

	// Update counter.
	// grow代表需要扩容?
	grow := atomic.AddInt32(&r.nodes, 1) >= h.growThreshold
	if bLen > mOverflowThreshold {  // bLen > 1 << 5
		grow = grow || atomic.AddInt32(&h.overflow, 1) >= mOverflowGrowThreshold
	}

	// Grow.
	// resizeInProgess from 0 to 1: 扩容中
	// Cache的mHead是一个mNode链表，链表前节点的容量是后节点的2倍
	// 层级关系：Cache->mNode->mBucket->Node
	if grow && atomic.CompareAndSwapInt32(&h.resizeInProgess, 0, 1) {
		nhLen := len(h.buckets) << 1  // len(h.buckets) * 2
		nh := &mNode{
			buckets:         make([]unsafe.Pointer, nhLen),
			mask:            uint32(nhLen) - 1,
			pred:            unsafe.Pointer(h),
			growThreshold:   int32(nhLen * mOverflowThreshold),
			shrinkThreshold: int32(nhLen >> 1),
		}
		ok := atomic.CompareAndSwapPointer(&r.mHead, unsafe.Pointer(h), unsafe.Pointer(nh))
		if !ok {
			panic("BUG: failed swapping head")
		}
		// 上面只是建立足够数量的指针数组，但是指针数组指向的对象还未初始化
		go nh.initBuckets()  // 初始化新建的mNode的buckets成员
	}

	return true, true, n
}

func (b *mBucket) delete(r *Cache, h *mNode, hash uint32, ns, key uint64) (done, deleted bool) {
	b.mu.Lock()

	if b.frozen {
		b.mu.Unlock()
		return
	}

	// Scan the node.
	var (
		n    *Node
		bLen int
	)
	// 遍历node列表，找出ns和key相等的node节点
	for i := range b.node {
		n = b.node[i]
		// 核对ns和key
		if n.ns == ns && n.key == key {
			// 在外层已经递减
			if atomic.LoadInt32(&n.ref) == 0 {  // 引用计数将为0
				deleted = true

				// Call releaser.
				if n.value != nil {
					if r, ok := n.value.(util.Releaser); ok {
						r.Release()
					}
					n.value = nil
				}

				// Remove node from bucket.
				b.node = append(b.node[:i], b.node[i+1:]...)  // 从bucket移出
				bLen = len(b.node)
			}
			break
		}
	}
	b.mu.Unlock()

	if deleted {
		// Call OnDel.
		// onDel:删除成功后的回调函数
		for _, f := range n.onDel {
			f()
		}

		// Update counter.
		atomic.AddInt32(&r.size, int32(n.size)*-1)   // cache.size减去node的size
		shrink := atomic.AddInt32(&r.nodes, -1) < h.shrinkThreshold  // 是否需要缩容
		if bLen >= mOverflowThreshold {
			atomic.AddInt32(&h.overflow, -1)
		}

		// Shrink.
		if shrink && len(h.buckets) > mInitialSize && atomic.CompareAndSwapInt32(&h.resizeInProgess, 0, 1) {
			// 正在resize
			nhLen := len(h.buckets) >> 1 // len(h.buckets) / 2
			nh := &mNode{
				buckets:         make([]unsafe.Pointer, nhLen),
				mask:            uint32(nhLen) - 1,
				pred:            unsafe.Pointer(h),
				growThreshold:   int32(nhLen * mOverflowThreshold),
				shrinkThreshold: int32(nhLen >> 1),
			}
			ok := atomic.CompareAndSwapPointer(&r.mHead, unsafe.Pointer(h), unsafe.Pointer(nh))
			if !ok {
				panic("BUG: failed swapping head")
			}
			// 注意:上面buckets:         make([]unsafe.Pointer, nhLen),
			// 只是分配了数量大小足够的指针数组，但是并未初始化真正的mBucket对象
			go nh.initBuckets()
		}
	}

	return true, deleted
}

type mNode struct {
	buckets         []unsafe.Pointer // []*mBucket
	mask            uint32
	pred            unsafe.Pointer // *mNode
	resizeInProgess int32

	overflow        int32
	growThreshold   int32
	shrinkThreshold int32
}

func (n *mNode) initBucket(i uint32) *mBucket {
	// 已经初始化成功
	// 已经初始化成功不需要进行下沉
	if b := (*mBucket)(atomic.LoadPointer(&n.buckets[i])); b != nil {
		return b
	}

	p := (*mNode)(atomic.LoadPointer(&n.pred))
	if p != nil {
		var node []*Node
		if n.mask > p.mask {
			// Grow.
			// 缩容操作,例如将n的0~7的mBuckets聚集到p的0~3的mBuckets里面
			// i&p.mask的目的就是为了取模运算
			pb := (*mBucket)(atomic.LoadPointer(&p.buckets[i&p.mask]))
			if pb == nil {
				pb = p.initBucket(i & p.mask)  // 递归逐渐下沉
			}
			m := pb.freeze()
			// freeze之后，不允许get和delete
			// Split nodes.
			for _, x := range m {
				if x.hash&n.mask == i {
					node = append(node, x)
				}
			}
		} else {
			// n.mask <= p.mask
			// Shrink.
			pb0 := (*mBucket)(atomic.LoadPointer(&p.buckets[i]))
			if pb0 == nil {
				pb0 = p.initBucket(i)
			}
			pb1 := (*mBucket)(atomic.LoadPointer(&p.buckets[i+uint32(len(n.buckets))]))
			if pb1 == nil {
				pb1 = p.initBucket(i + uint32(len(n.buckets)))
			}
			m0 := pb0.freeze()
			m1 := pb1.freeze()
			// Merge nodes.
			node = make([]*Node, 0, len(m0)+len(m1))
			node = append(node, m0...)
			node = append(node, m1...)
		}
		b := &mBucket{node: node}  // 重建bucket对应的node指针列表
		if atomic.CompareAndSwapPointer(&n.buckets[i], nil, unsafe.Pointer(b)) {
			if len(node) > mOverflowThreshold {
				atomic.AddInt32(&n.overflow, int32(len(node)-mOverflowThreshold))  // 溢出了len(node)-mOverflowThreshold
			}
			return b
		}
	}

	return (*mBucket)(atomic.LoadPointer(&n.buckets[i]))
}

// 初始化bucket列表
func (n *mNode) initBuckets() {
	for i := range n.buckets {
		n.initBucket(uint32(i))
	}
	// 将mNode的pred成员置空
	atomic.StorePointer(&n.pred, nil)
}

// Cache is a 'cache map'.
type Cache struct {
	mu     sync.RWMutex
	mHead  unsafe.Pointer // *mNode
	nodes  int32
	size   int32
	cacher Cacher
	closed bool
}

// NewCache creates a new 'cache map'. The cacher is optional and
// may be nil.
func NewCache(cacher Cacher) *Cache {
	h := &mNode{
		buckets:         make([]unsafe.Pointer, mInitialSize),  // 最少为1 << 4(16)
		mask:            mInitialSize - 1,
		growThreshold:   int32(mInitialSize * mOverflowThreshold),   // 16 * 32
		shrinkThreshold: 0,
	}
	for i := range h.buckets {
		h.buckets[i] = unsafe.Pointer(&mBucket{})
	}
	r := &Cache{
		mHead:  unsafe.Pointer(h),
		cacher: cacher,
	}
	return r
}

// Cache->mNode->mBucket
func (r *Cache) getBucket(hash uint32) (*mNode, *mBucket) {
	h := (*mNode)(atomic.LoadPointer(&r.mHead))
	i := hash & h.mask  // 计算出真正下标
	b := (*mBucket)(atomic.LoadPointer(&h.buckets[i]))
	if b == nil {
		b = h.initBucket(i)  // 初始化该bucket
	}
	return h, b
}

func (r *Cache) delete(n *Node) bool {
	for {
		h, b := r.getBucket(n.hash)
		done, deleted := b.delete(r, h, n.hash, n.ns, n.key)
		if done {
			return deleted
		}
	}
}

// Nodes returns number of 'cache node' in the map.
func (r *Cache) Nodes() int {
	return int(atomic.LoadInt32(&r.nodes))
}

// Size returns sums of 'cache node' size in the map.
func (r *Cache) Size() int {
	return int(atomic.LoadInt32(&r.size))
}

// Capacity returns cache capacity.
func (r *Cache) Capacity() int {
	if r.cacher == nil {
		return 0
	}
	return r.cacher.Capacity()
}

// SetCapacity sets cache capacity.
func (r *Cache) SetCapacity(capacity int) {
	if r.cacher != nil {
		r.cacher.SetCapacity(capacity)
	}
}

// Get gets 'cache node' with the given namespace and key.
// If cache node is not found and setFunc is not nil, Get will atomically creates
// the 'cache node' by calling setFunc. Otherwise Get will returns nil.
//
// The returned 'cache handle' should be released after use by calling Release
// method.
func (r *Cache) Get(ns, key uint64, setFunc func() (size int, value Value)) *Handle {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil
	}

	// 计算hash值
	hash := murmur32(ns, key, 0xf00)
	for {
		h, b := r.getBucket(hash)  // 找到对应的mNode和mBucket
		done, _, n := b.get(r, h, hash, ns, key, setFunc == nil)  // setFunc不为nil，证明具有设置逻辑
		if done {
			if n != nil {
				n.mu.Lock()
				if n.value == nil {
					if setFunc == nil {
						n.mu.Unlock()
						n.unref()  // 引用计数减1
						return nil
					}

					n.size, n.value = setFunc()
					// 该节点无效
					if n.value == nil {
						n.size = 0
						n.mu.Unlock()
						n.unref()  // 引用计数减1，从Bucket中移除该Node节点
						return nil
					}
					atomic.AddInt32(&r.size, int32(n.size))
				}
				n.mu.Unlock()
				if r.cacher != nil {
					// 调用Cacher，往Cacher的结构新增加节点
					// 比如通知lru cache，往lru链表里面插入节点。
					// 由这里进行触发。
					r.cacher.Promote(n)
				}
				return &Handle{unsafe.Pointer(n)}  // 将查询到的n用Handle包装起来
			}

			break
		}
	}
	return nil
}

// Delete removes and ban 'cache node' with the given namespace and key.
// A banned 'cache node' will never inserted into the 'cache tree'. Ban
// only attributed to the particular 'cache node', so when a 'cache node'
// is recreated it will not be banned.
//
// If onDel is not nil, then it will be executed if such 'cache node'
// doesn't exist or once the 'cache node' is released.
//
// Delete return true is such 'cache node' exist.
func (r *Cache) Delete(ns, key uint64, onDel func()) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return false
	}

	hash := murmur32(ns, key, 0xf00)
	for {
		h, b := r.getBucket(hash)
		done, _, n := b.get(r, h, hash, ns, key, true)
		if done {
			if n != nil {
				if onDel != nil {
					n.mu.Lock()
					n.onDel = append(n.onDel, onDel)
					n.mu.Unlock()
				}
				if r.cacher != nil {
					r.cacher.Ban(n)
				}
				n.unref()  // 引用计数减1
				return true
			}

			break
		}
	}

	// 节点为nil，早已删除成功，直接回调onDel
	if onDel != nil {
		onDel()
	}

	return false
}

// Evict evicts 'cache node' with the given namespace and key. This will
// simply call Cacher.Evict.
//
// Evict return true is such 'cache node' exist.
func (r *Cache) Evict(ns, key uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return false
	}

	hash := murmur32(ns, key, 0xf00)
	for {
		h, b := r.getBucket(hash)
		done, _, n := b.get(r, h, hash, ns, key, true)
		if done {
			if n != nil {
				if r.cacher != nil {
					// 调用cacher的删除方法，比如调用lru的Evict方法
					r.cacher.Evict(n)
				}
				n.unref()
				return true
			}

			break
		}
	}

	return false
}

// EvictNS evicts 'cache node' with the given namespace. This will
// simply call Cacher.EvictNS.
func (r *Cache) EvictNS(ns uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}

	if r.cacher != nil {
		r.cacher.EvictNS(ns)  // NS=namespace
	}
}

// EvictAll evicts all 'cache node'. This will simply call Cacher.EvictAll.
func (r *Cache) EvictAll() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}

	if r.cacher != nil {
		r.cacher.EvictAll()
	}
}

// Close closes the 'cache map' and forcefully releases all 'cache node'.
func (r *Cache) Close() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true  // 不允许进行Get,Delete,Evict,EvictNS等操作

		h := (*mNode)(r.mHead)
		// 必须要将资源释放完毕
		h.initBuckets()

		for i := range h.buckets {
			b := (*mBucket)(h.buckets[i])
			for _, n := range b.node {
				// Call releaser.
				if n.value != nil {
					if r, ok := n.value.(util.Releaser); ok {
						r.Release()  // 逐个节点进行释放
					}
					n.value = nil
				}

				// Call OnDel.
				for _, f := range n.onDel {
					f()
				}
				n.onDel = nil
			}
		}
	}
	r.mu.Unlock()

	// Avoid deadlock.
	if r.cacher != nil {
		if err := r.cacher.Close(); err != nil {
			return err
		}
	}
	return nil
}

// CloseWeak closes the 'cache map' and evict all 'cache node' from cacher, but
// unlike Close it doesn't forcefully releases 'cache node'.
func (r *Cache) CloseWeak() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
	}
	r.mu.Unlock()

	// 不强制释放每个Node节点

	// Avoid deadlock.
	if r.cacher != nil {
		r.cacher.EvictAll()
		if err := r.cacher.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Node is a 'cache node'.
type Node struct {
	r *Cache

	hash    uint32
	ns, key uint64

	mu    sync.Mutex
	size  int
	value Value

	ref   int32
	onDel []func()  // 删除注册的回调函数

	CacheData unsafe.Pointer
}

// NS returns this 'cache node' namespace.
func (n *Node) NS() uint64 {
	return n.ns
}

// Key returns this 'cache node' key.
func (n *Node) Key() uint64 {
	return n.key
}

// Size returns this 'cache node' size.
func (n *Node) Size() int {
	return n.size
}

// Value returns this 'cache node' value.
func (n *Node) Value() Value {
	return n.value
}

// Ref returns this 'cache node' ref counter.
func (n *Node) Ref() int32 {
	return atomic.LoadInt32(&n.ref)
}

// GetHandle returns an handle for this 'cache node'.
func (n *Node) GetHandle() *Handle {
	// 引用计数不可能为0
	if atomic.AddInt32(&n.ref, 1) <= 1 {
		panic("BUG: Node.GetHandle on zero ref")
	}
	return &Handle{unsafe.Pointer(n)}
}

func (n *Node) unref() {
	if atomic.AddInt32(&n.ref, -1) == 0 {
		n.r.delete(n)  // 这里面会触发引用计数等于0的删除操作
	}
}

func (n *Node) unrefLocked() {
	if atomic.AddInt32(&n.ref, -1) == 0 {
		n.r.mu.RLock()
		if !n.r.closed {
			n.r.delete(n)
		}
		n.r.mu.RUnlock()
	}
}

// Handle is a 'cache handle' of a 'cache node'.
type Handle struct {
	n unsafe.Pointer // *Node
}

// Value returns the value of the 'cache node'.
func (h *Handle) Value() Value {
	n := (*Node)(atomic.LoadPointer(&h.n))
	if n != nil {
		return n.value
	}
	return nil
}

// Release releases this 'cache handle'.
// It is safe to call release multiple times.
func (h *Handle) Release() {
	nPtr := atomic.LoadPointer(&h.n)
	if nPtr != nil && atomic.CompareAndSwapPointer(&h.n, nPtr, nil) {
		n := (*Node)(nPtr)
		n.unrefLocked()
	}
}

func murmur32(ns, key uint64, seed uint32) uint32 {
	const (
		m = uint32(0x5bd1e995)
		r = 24
	)

	k1 := uint32(ns >> 32)
	k2 := uint32(ns)
	k3 := uint32(key >> 32)
	k4 := uint32(key)

	k1 *= m
	k1 ^= k1 >> r
	k1 *= m

	k2 *= m
	k2 ^= k2 >> r
	k2 *= m

	k3 *= m
	k3 ^= k3 >> r
	k3 *= m

	k4 *= m
	k4 ^= k4 >> r
	k4 *= m

	h := seed

	h *= m
	h ^= k1
	h *= m
	h ^= k2
	h *= m
	h ^= k3
	h *= m
	h ^= k4

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}
