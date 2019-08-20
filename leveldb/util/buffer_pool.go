// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package util

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type buffer struct {
	b    []byte
	miss int
}

// BufferPool is a 'buffer pool'.
// 内存池
type BufferPool struct {
	pool      [6]chan []byte
	size      [5]uint32
	sizeMiss  [5]uint32
	sizeHalf  [5]uint32  // 用于修正pool的大小
	baseline  [4]int
	baseline0 int

	mu     sync.RWMutex
	closed bool
	closeC chan struct{}

	get     uint32
	put     uint32
	half    uint32
	less    uint32
	equal   uint32
	greater uint32
	miss    uint32
}

// 根据需要的buffer大小n，分配大于或等于n的内存池
func (p *BufferPool) poolNum(n int) int {
	if n <= p.baseline0 && n > p.baseline0/2 {
		return 0
	}
	for i, x := range p.baseline {
		if n <= x {
			return i + 1
		}
	}
	return len(p.baseline) + 1
}

// Get returns buffer with length of n.
func (p *BufferPool) Get(n int) []byte {
	if p == nil {
		// 直接分配
		return make([]byte, n)
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		// 内存池被关闭，直接分配
		return make([]byte, n)
	}

	atomic.AddUint32(&p.get, 1)  // 累计get的次数?

	// 从内存池中分配
	poolNum := p.poolNum(n)  // 内存池段的大小
	pool := p.pool[poolNum]  // 获取对应的channel
	if poolNum == 0 {  // if n <= p.baseline0 && n > p.baseline0/2
		// Fast path.
		select {
		case b := <-pool:
			switch {
			case cap(b) > n:
				if cap(b)-n >= n {  // 使用率过低，不足50%，直接从堆上分配
					atomic.AddUint32(&p.half, 1)  // 不足一半
					select {  // 放回内存池
					case pool <- b:
					default:
					}
					return make([]byte, n)
				} else {
					atomic.AddUint32(&p.less, 1)  // 大于50%，直接从内存池分配
					return b[:n]
				}
			case cap(b) == n:
				atomic.AddUint32(&p.equal, 1)  // 刚好相等
				return b[:n]
			default:  // cap(b) <= n
				atomic.AddUint32(&p.greater, 1)
			}
		default:
			atomic.AddUint32(&p.miss, 1)
		}

		return make([]byte, n, p.baseline0)  // 容量为p.baseline0
	} else {  // poolNum != 0
		sizePtr := &p.size[poolNum-1]  // size [5]uint32

		select {
		case b := <-pool:
			switch {
			case cap(b) > n:
				if cap(b)-n >= n {
					atomic.AddUint32(&p.half, 1)
					sizeHalfPtr := &p.sizeHalf[poolNum-1]  // sizeHalfPtr相当于一个计数器
					if atomic.AddUint32(sizeHalfPtr, 1) == 20 {  // 达到20,将sizePtr设置为容量的一半（修正sizePtr)
						atomic.StoreUint32(sizePtr, uint32(cap(b)/2))
						atomic.StoreUint32(sizeHalfPtr, 0)
					} else {  // 原子性加1,直接回收内存
						select {
						case pool <- b:
						default:
						}
					}
					return make([]byte, n)
				} else {  // cap(b)-n < n 使用率大于50%
					atomic.AddUint32(&p.less, 1)
					return b[:n]
				}
			case cap(b) == n:
				atomic.AddUint32(&p.equal, 1)
				return b[:n]
			default:  // cap(b) < n
				atomic.AddUint32(&p.greater, 1)
				if uint32(cap(b)) >= atomic.LoadUint32(sizePtr) {  // 回收该块内存
					select {
					case pool <- b:
					default:
					}
				}
			}
		default:
			atomic.AddUint32(&p.miss, 1)
		}

		// 本次需要的内存大小n大于原来的sizePtr
		if size := atomic.LoadUint32(sizePtr); uint32(n) > size {
			if size == 0 {
				atomic.CompareAndSwapUint32(sizePtr, 0, uint32(n))
			} else {
				sizeMissPtr := &p.sizeMiss[poolNum-1]  // sizeMiss也是一个计数器,累计到一定次数之后进行置换
				if atomic.AddUint32(sizeMissPtr, 1) == 20 {
					// todo：那么内存的分配在哪里做
					atomic.StoreUint32(sizePtr, uint32(n))
					atomic.StoreUint32(sizeMissPtr, 0)
				}
			}
			return make([]byte, n)
		} else {
			return make([]byte, n, size)
		}
	}
}

// Put adds given buffer to the pool.
// 将buffer放入
func (p *BufferPool) Put(b []byte) {
	if p == nil {
		return
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return
	}

	atomic.AddUint32(&p.put, 1)

	pool := p.pool[p.poolNum(cap(b))]  // 找到大于cap(b)的channel,然后放进去
	select {
	case pool <- b:
	default:
	}

}

func (p *BufferPool) Close() {
	if p == nil {
		return
	}

	p.mu.Lock()
	if !p.closed {
		p.closed = true
		p.closeC <- struct{}{}
	}
	p.mu.Unlock()
}

func (p *BufferPool) String() string {
	if p == nil {
		return "<nil>"
	}

	return fmt.Sprintf("BufferPool{B·%d Z·%v Zm·%v Zh·%v G·%d P·%d H·%d <·%d =·%d >·%d M·%d}",
		p.baseline0, p.size, p.sizeMiss, p.sizeHalf, p.get, p.put, p.half, p.less, p.equal, p.greater, p.miss)
}

// 排水
func (p *BufferPool) drain() {
	ticker := time.NewTicker(2 * time.Second)  // 每2秒执行一次
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, ch := range p.pool {
				select {
				case <-ch:  // 将所有的poll清空?
				default:
				}
			}
		case <-p.closeC:  // 如果总开关p.closeC关闭了,那么关闭所有的channel(pool)
			close(p.closeC)
			for _, ch := range p.pool {
				close(ch)
			}
			return
		}
	}
}

// NewBufferPool creates a new initialized 'buffer pool'.
func NewBufferPool(baseline int) *BufferPool {
	if baseline <= 0 {
		panic("baseline can't be <= 0")
	}
	p := &BufferPool{
		baseline0: baseline,
		baseline:  [...]int{baseline / 4, baseline / 2, baseline * 2, baseline * 4},
		closeC:    make(chan struct{}, 1),
	}
	for i, cap := range []int{2, 2, 4, 4, 2, 1} {
		// 0: 2个buffer
		// 1: 2个buffer
		// 2: 4个buffer
		// 3: 4个buffer
		// 4: 2个buffer
		// 5: 1个buffer
		p.pool[i] = make(chan []byte, cap)
	}
	go p.drain()
	return p
}
