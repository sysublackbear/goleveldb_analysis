// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/filter"
)

type iFilter struct {
	filter.Filter
}

// 直接调用filter.Filter接口的Contains方法
func (f iFilter) Contains(filter, key []byte) bool {
	return f.Filter.Contains(filter, internalKey(key).ukey())
}

// 直接调用filter.Filter接口的NewGenerator方法
func (f iFilter) NewGenerator() filter.FilterGenerator {
	return iFilterGenerator{f.Filter.NewGenerator()}
}

type iFilterGenerator struct {
	filter.FilterGenerator
}

// 直接调用filter.FilterGenerator接口的Add方法
func (g iFilterGenerator) Add(key []byte) {
	g.FilterGenerator.Add(internalKey(key).ukey())
}
