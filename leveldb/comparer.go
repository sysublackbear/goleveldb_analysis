// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/comparer"
)

type iComparer struct {
	ucmp comparer.Comparer
}

func (icmp *iComparer) uName() string {
	return icmp.ucmp.Name()
}

func (icmp *iComparer) uCompare(a, b []byte) int {
	return icmp.ucmp.Compare(a, b)
}

func (icmp *iComparer) uSeparator(dst, a, b []byte) []byte {
	return icmp.ucmp.Separator(dst, a, b)
}

func (icmp *iComparer) uSuccessor(dst, b []byte) []byte {
	return icmp.ucmp.Successor(dst, b)
}

func (icmp *iComparer) Name() string {
	return icmp.uName()
}

// 默认的比较方法：
//1.首先按照字典序比较用户定义的key（ukey），若用户定义key值大，整个internalKey就大；
//2.若用户定义的key相同，则序列号大的internalKey值就小；
func (icmp *iComparer) Compare(a, b []byte) int {
	x := icmp.uCompare(internalKey(a).ukey(), internalKey(b).ukey())
	if x == 0 {
		// icmp.uCompare返回相等
		// 比较两个num
		if m, n := internalKey(a).num(), internalKey(b).num(); m > n {
			return -1
		} else if m < n {
			return 1
		}
	}
	return x
}

func (icmp *iComparer) Separator(dst, a, b []byte) []byte {
	ua, ub := internalKey(a).ukey(), internalKey(b).ukey()
	dst = icmp.uSeparator(dst, ua, ub)
	if dst != nil && len(dst) < len(ua) && icmp.uCompare(ua, dst) < 0 {
		// Append earliest possible number.
		return append(dst, keyMaxNumBytes...)
	}
	return nil
}

func (icmp *iComparer) Successor(dst, b []byte) []byte {
	ub := internalKey(b).ukey()
	dst = icmp.uSuccessor(dst, ub)
	if dst != nil && len(dst) < len(ub) && icmp.uCompare(ub, dst) < 0 {
		// Append earliest possible number.
		return append(dst, keyMaxNumBytes...)
	}
	return nil
}
