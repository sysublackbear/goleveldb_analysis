// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/table"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// tFile holds basic information about a table.
type tFile struct {
	fd         storage.FileDesc
	seekLeft   int32
	size       int64
	imin, imax internalKey
}

// Returns true if given key is after largest key of this table.
// 判断给定的key是否比当前表里面最大的key还要大
func (t *tFile) after(icmp *iComparer, ukey []byte) bool {
	return ukey != nil && icmp.uCompare(ukey, t.imax.ukey()) > 0
}

// Returns true if given key is before smallest key of this table.
// 判断给定的key是否比当前表里面最小的key还要小
func (t *tFile) before(icmp *iComparer, ukey []byte) bool {
	return ukey != nil && icmp.uCompare(ukey, t.imin.ukey()) < 0
}

// Returns true if given key range overlaps with this table key range.
// 判断给定的tFile的[imin, imax]是否和[umin, umax]重叠
func (t *tFile) overlaps(icmp *iComparer, umin, umax []byte) bool {
	// icmp.uCompare(umin, t.imax.ukey()) <= 0 && icmp.uCompare(umax, t.imin.ukey()) >= 0
	return !t.after(icmp, umin) && !t.before(icmp, umax)
}

// Cosumes one seek and return current seeks left.
// seekLeft减去1
func (t *tFile) consumeSeek() int32 {
	return atomic.AddInt32(&t.seekLeft, -1)
}

// Creates new tFile.
func newTableFile(fd storage.FileDesc, size int64, imin, imax internalKey) *tFile {
	f := &tFile{
		fd:   fd,
		size: size,
		imin: imin,
		imax: imax,
	}

	// We arrange to automatically compact this file after
	// a certain number of seeks.  Let's assume:
	//   (1) One seek costs 10ms
	//   (2) Writing or reading 1MB costs 10ms (100MB/s)
	//   (3) A compaction of 1MB does 25MB of IO:
	//         1MB read from this level
	//         10-12MB read from next level (boundaries may be misaligned)
	//         10-12MB written to next level
	// This implies that 25 seeks cost the same as the compaction
	// of 1MB of data.  I.e., one seek costs approximately the
	// same as the compaction of 40KB of data.  We are a little
	// conservative and allow approximately one seek for every 16KB
	// of data before triggering a compaction.
	f.seekLeft = int32(size / 16384)
	if f.seekLeft < 100 {
		f.seekLeft = 100
	}

	return f
}

/*
type atRecord struct {
	level int
	num   int64
	size  int64
	imin  internalKey
	imax  internalKey
}
 */
// atRecord转tFile
func tableFileFromRecord(r atRecord) *tFile {
	return newTableFile(storage.FileDesc{Type: storage.TypeTable, Num: r.num}, r.size, r.imin, r.imax)
}

// tFiles hold multiple tFile.
type tFiles []*tFile

func (tf tFiles) Len() int      { return len(tf) }
func (tf tFiles) Swap(i, j int) { tf[i], tf[j] = tf[j], tf[i] }

// [tf[0].fd.Num, tf[1].fd.Num, tf[2].fd.Num ... ]
func (tf tFiles) nums() string {
	x := "[ "
	for i, f := range tf {
		if i != 0 {
			x += ", "
		}
		x += fmt.Sprint(f.fd.Num)
	}
	x += " ]"
	return x
}

// Returns true if i smallest key is less than j.
// This used for sort by key in ascending order.
// 判断tf[i], tf[j]两个tFile的imin谁更小，相等的话则判断fd.Num
func (tf tFiles) lessByKey(icmp *iComparer, i, j int) bool {
	a, b := tf[i], tf[j]
	n := icmp.Compare(a.imin, b.imin)
	if n == 0 {
		return a.fd.Num < b.fd.Num
	}
	return n < 0
}

// Returns true if i file number is greater than j.
// This used for sort by file number in descending order.
func (tf tFiles) lessByNum(i, j int) bool {
	return tf[i].fd.Num > tf[j].fd.Num
}

// Sorts tables by key in ascending order.
func (tf tFiles) sortByKey(icmp *iComparer) {
	// 按key进行排序
	sort.Sort(&tFilesSortByKey{tFiles: tf, icmp: icmp})
}

// Sorts tables by file number in descending order.
func (tf tFiles) sortByNum() {
	// 按num进行排序
	sort.Sort(&tFilesSortByNum{tFiles: tf})
}

// Returns sum of all tables size.
// sum += tf[0...n].size
func (tf tFiles) size() (sum int64) {
	for _, t := range tf {
		sum += t.size
	}
	return sum
}

// Searches smallest index of tables whose its smallest
// key is after or equal with given key.
// 找出tf[i].imin>=ikey的最小下标i
func (tf tFiles) searchMin(icmp *iComparer, ikey internalKey) int {
	return sort.Search(len(tf), func(i int) bool {
		return icmp.Compare(tf[i].imin, ikey) >= 0
	})
}

// Searches smallest index of tables whose its largest
// key is after or equal with given key.
// 找出imax>=ikey的最小值
func (tf tFiles) searchMax(icmp *iComparer, ikey internalKey) int {
	return sort.Search(len(tf), func(i int) bool {
		return icmp.Compare(tf[i].imax, ikey) >= 0
	})
}

// Searches smallest index of tables whose its file number
// is smaller than the given number.
// 找出tf[i].fd小于num的下标
func (tf tFiles) searchNumLess(num int64) int {
	return sort.Search(len(tf), func(i int) bool {
		return tf[i].fd.Num < num
	})
}

// Searches smallest index of tables whose its smallest
// key is after the given key.
func (tf tFiles) searchMinUkey(icmp *iComparer, umin []byte) int {
	return sort.Search(len(tf), func(i int) bool {
		// 与searchMin的区别在于，这里只比较两个key
		return icmp.ucmp.Compare(tf[i].imin.ukey(), umin) > 0
	})
}

// Searches smallest index of tables whose its largest
// key is after the given key.
func (tf tFiles) searchMaxUkey(icmp *iComparer, umax []byte) int {
	return sort.Search(len(tf), func(i int) bool {
		// 与searchMax的区别在于,这里只比较两个key
		return icmp.ucmp.Compare(tf[i].imax.ukey(), umax) > 0
	})
}

// Returns true if given key range overlaps with one or more
// tables key range. If unsorted is true then binary search will not be used.
func (tf tFiles) overlaps(icmp *iComparer, umin, umax []byte, unsorted bool) bool {
	if unsorted {
		// 无序
		// Check against all files.
		// 逐个检查，判断给定的tFile的[imin, imax]是否和[umin, umax]重叠
		for _, t := range tf {
			// t.imin, umin, t.imax, umax
			// icmp.uCompare(umin, t.imax.ukey()) <= 0 && icmp.uCompare(umax, t.imin.ukey()) >= 0
			if t.overlaps(icmp, umin, umax) {
				return true
			}
		}
		return false
	}

	// 如果tFiles本身有序，那么不需要逐个遍历，主需要判断umin，umax是否在tf的有序区间极客。
	i := 0
	if len(umin) > 0 {
		/*
		return sort.Search(len(tf), func(i int) bool {
			return icmp.Compare(tf[i].imax, makeInternalKey(nil, umin, keyMaxSeq, keyTypeSeek)) >= 0
		})
		 */
		// Find the earliest possible internal key for min.
		// tf的imax大于umin，说明一定重叠
		i = tf.searchMax(icmp, makeInternalKey(nil, umin, keyMaxSeq, keyTypeSeek))
	}
	if i >= len(tf) {
		// Beginning of range is after all files, so no overlap.
		return false
	}
	return !tf[i].before(icmp, umax)
}

// Returns tables whose its key range overlaps with given key range.
// Range will be expanded if ukey found hop across tables.
// If overlapped is true then the search will be restarted if umax
// expanded.
// The dst content will be overwritten.
// 获取重叠部分
// 获取[umin, umax]与tf重叠的部分
func (tf tFiles) getOverlaps(dst tFiles, icmp *iComparer, umin, umax []byte, overlapped bool) tFiles {
	// Short circuit if tf is empty
	if len(tf) == 0 {
		return nil
	}
	// For non-zero levels, there is no ukey hop across at all.
	// And what's more, the files in these levels are strictly sorted,
	// so use binary search instead of heavy traverse.
	if !overlapped {
		var begin, end int
		// Determine the begin index of the overlapped file
		if umin != nil {
			// 找出比tf.umin大于umin的tFile下标
			/*
			return sort.Search(len(tf), func(i int) bool {
				// 与searchMin的区别在于，这里只比较两个key
				return icmp.ucmp.Compare(tf[i].imin.ukey(), umin) > 0
			})
			 */
			index := tf.searchMinUkey(icmp, umin)
			if index == 0 {
				// tf[index] > umin
				begin = 0
			} else if bytes.Compare(tf[index-1].imax.ukey(), umin) >= 0 {
				// 前一个tfFile的imax已经大于umin
				// The min ukey overlaps with the index-1 file, expand it.
				begin = index - 1
			} else {
				// bytes.Compare(tf[index-1].imax.ukey(), umin) < 0
				begin = index
			}
		}
		// Determine the end index of the overlapped file
		if umax != nil {
			index := tf.searchMaxUkey(icmp, umax)
			/*
			return sort.Search(len(tf), func(i int) bool {
				// 与searchMax的区别在于,这里只比较两个key
				return icmp.ucmp.Compare(tf[i].imax.ukey(), umax) > 0
			})
			*/
			if index == len(tf) {
				end = len(tf)
			} else if bytes.Compare(tf[index].imin.ukey(), umax) <= 0 {
				// The max ukey overlaps with the index file, expand it.
				// max >= index的最小值
				end = index + 1
			} else {
				end = index
			}
		} else {
			end = len(tf)
		}
		// Ensure the overlapped file indexes are valid.
		if begin >= end {
			return nil
		}
		dst = make([]*tFile, end-begin)
		// 截取完全重叠的区间
		copy(dst, tf[begin:end])  // 截取[umin, umax]的区间
		return dst
	}

	// if overlapped
	dst = dst[:0]
	for i := 0; i < len(tf); {
		t := tf[i]
		if t.overlaps(icmp, umin, umax) {
			// tf[i]与[umin, umax]重叠
			// tf[i].imin.ukey() < umin
			if umin != nil && icmp.uCompare(t.imin.ukey(), umin) < 0 {
				umin = t.imin.ukey()
				dst = dst[:0]  // 重置dst
				i = 0
				continue
			} else if umax != nil && icmp.uCompare(t.imax.ukey(), umax) > 0 {
				umax = t.imax.ukey()
				// Restart search if it is overlapped.
				// 如果发现重叠，dst重新计算
				dst = dst[:0]
				i = 0
				continue
			}

			dst = append(dst, t)   // 将重叠的部分添加进来。
		}
		i++
	}

	return dst
}

// Returns tables key range.
// 求出整段tFiles的imin和imax
func (tf tFiles) getRange(icmp *iComparer) (imin, imax internalKey) {
	for i, t := range tf {
		if i == 0 {
			imin, imax = t.imin, t.imax
			continue
		}
		if icmp.Compare(t.imin, imin) < 0 {
			imin = t.imin
		}
		if icmp.Compare(t.imax, imax) > 0 {
			imax = t.imax
		}
	}

	return
}

// Creates iterator index from tables.
func (tf tFiles) newIndexIterator(tops *tOps, icmp *iComparer, slice *util.Range, ro *opt.ReadOptions) iterator.IteratorIndexer {
	if slice != nil {
		var start, limit int
		if slice.Start != nil {
			/*
			func (tf tFiles) searchMax(icmp *iComparer, ikey internalKey) int {
				return sort.Search(len(tf), func(i int) bool {
					return icmp.Compare(tf[i].imax, ikey) >= 0
				})
			}
			 */
			start = tf.searchMax(icmp, internalKey(slice.Start))
		}
		if slice.Limit != nil {
			/*
			func (tf tFiles) searchMin(icmp *iComparer, ikey internalKey) int {
				return sort.Search(len(tf), func(i int) bool {
					return icmp.Compare(tf[i].imin, ikey) >= 0
				})
			}
			 */
			limit = tf.searchMin(icmp, internalKey(slice.Limit))
		} else {
			limit = tf.Len()
		}
		tf = tf[start:limit]  // 找出对应的start
	}
	return iterator.NewArrayIndexer(&tFilesArrayIndexer{
		tFiles: tf,
		tops:   tops,
		icmp:   icmp,
		slice:  slice,
		ro:     ro,
	})
}

// Tables iterator index.
type tFilesArrayIndexer struct {
	tFiles
	tops  *tOps
	icmp  *iComparer
	slice *util.Range
	ro    *opt.ReadOptions
}

func (a *tFilesArrayIndexer) Search(key []byte) int {
	return a.searchMax(a.icmp, internalKey(key))
}

func (a *tFilesArrayIndexer) Get(i int) iterator.Iterator {
	if i == 0 || i == a.Len()-1 {
		return a.tops.newIterator(a.tFiles[i], a.slice, a.ro)
	}
	return a.tops.newIterator(a.tFiles[i], nil, a.ro)
}

// Helper type for sortByKey.
type tFilesSortByKey struct {
	tFiles
	icmp *iComparer
}

func (x *tFilesSortByKey) Less(i, j int) bool {
	return x.lessByKey(x.icmp, i, j)
}

// Helper type for sortByNum.
type tFilesSortByNum struct {
	tFiles
}

func (x *tFilesSortByNum) Less(i, j int) bool {
	return x.lessByNum(i, j)
}

// Table operations.
type tOps struct {
	s            *session  // todo: 这个是什么含义?
	noSync       bool
	evictRemoved bool
	cache        *cache.Cache
	bcache       *cache.Cache
	bpool        *util.BufferPool
}

// Creates an empty table and returns table writer.
func (t *tOps) create() (*tWriter, error) {
	fd := storage.FileDesc{Type: storage.TypeTable, Num: t.s.allocFileNum()}
	// 通过session去创建文件
	fw, err := t.s.stor.Create(fd)
	if err != nil {
		return nil, err
	}
	return &tWriter{
		t:  t,
		fd: fd,
		w:  fw,
		tw: table.NewWriter(fw, t.s.o.Options),
	}, nil
}

// Builds table from src iterator.
// 由源迭代器复制元素到新的tFile中
func (t *tOps) createFrom(src iterator.Iterator) (f *tFile, n int, err error) {
	w, err := t.create()  // 获得*tWriter
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			w.drop()
		}
	}()

	for src.Next() {
		err = w.append(src.Key(), src.Value())
		if err != nil {
			return
		}
	}
	err = src.Error()
	if err != nil {
		return
	}

	n = w.tw.EntriesLen()
	f, err = w.finish()
	return
}

// Opens table. It returns a cache handle, which should
// be released after use.
func (t *tOps) open(f *tFile) (ch *cache.Handle, err error) {
	// 打开一张表

	// 通过cache查询已经被打开的sstable文件句柄以及元数据
	// namespace = 0
	// key: f.fd.Num
	ch = t.cache.Get(0, uint64(f.fd.Num), func() (size int, value cache.Value) {
		// 不存在则设置的函数逻辑
		// 打开一个fd，创建一个reader

		// 打开一个文件
		var r storage.Reader
		r, err = t.s.stor.Open(f.fd)
		if err != nil {
			return 0, nil
		}

		var bcache *cache.NamespaceGetter
		if t.bcache != nil {
			bcache = &cache.NamespaceGetter{Cache: t.bcache, NS: uint64(f.fd.Num)}
		}

		// 初始化table.Reader
		var tr *table.Reader
		//
		tr, err = table.NewReader(r, f.size, f.fd, bcache, t.bpool, t.s.o.Options)
		if err != nil {
			r.Close()
			return 0, nil
		}
		return 1, tr

	})
	if ch == nil && err == nil {
		err = ErrClosed
	}
	return
}

// Finds key/value pair whose key is greater than or equal to the
// given key.
// 找出(rkey, rvalue)大于或等于给定的key
func (t *tOps) find(f *tFile, key []byte, ro *opt.ReadOptions) (rkey, rvalue []byte, err error) {
	ch, err := t.open(f)
	if err != nil {
		return nil, nil, err
	}
	// &Handle{unsafe.Pointer(n)}
	defer ch.Release()
	// 各种block的读取
	return ch.Value().(*table.Reader).Find(key, true, ro)
}

// Finds key that is greater than or equal to the given key.
func (t *tOps) findKey(f *tFile, key []byte, ro *opt.ReadOptions) (rkey []byte, err error) {
	ch, err := t.open(f)
	if err != nil {
		return nil, err
	}
	defer ch.Release()
	return ch.Value().(*table.Reader).FindKey(key, true, ro)
}

// Returns approximate offset of the given key.
func (t *tOps) offsetOf(f *tFile, key []byte) (offset int64, err error) {
	ch, err := t.open(f)
	if err != nil {
		return
	}
	defer ch.Release()
	return ch.Value().(*table.Reader).OffsetOf(key)
}

// Creates an iterator from the given table.
func (t *tOps) newIterator(f *tFile, slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	ch, err := t.open(f)
	if err != nil {
		return iterator.NewEmptyIterator(err)
	}
	iter := ch.Value().(*table.Reader).NewIterator(slice, ro)
	iter.SetReleaser(ch)
	return iter
}

// Removes table from persistent storage. It waits until
// no one use the the table.
func (t *tOps) remove(fd storage.FileDesc) {
	// 从缓存中删除数据（减少引用计数）
	t.cache.Delete(0, uint64(fd.Num), func() {
		if err := t.s.stor.Remove(fd); err != nil {
			t.s.logf("table@remove removing @%d %q", fd.Num, err)
		} else {
			t.s.logf("table@remove removed @%d", fd.Num)
		}
		if t.evictRemoved && t.bcache != nil {
			t.bcache.EvictNS(uint64(fd.Num))
		}
	})
}

// Closes the table ops instance. It will close all tables,
// regadless still used or not.
func (t *tOps) close() {
	t.bpool.Close()
	t.cache.Close()
	if t.bcache != nil {
		t.bcache.CloseWeak()
	}
}

// Creates new initialized table ops instance.
func newTableOps(s *session) *tOps {
	var (
		cacher cache.Cacher
		bcache *cache.Cache
		bpool  *util.BufferPool
	)
	if s.o.GetOpenFilesCacheCapacity() > 0 {
		cacher = cache.NewLRU(s.o.GetOpenFilesCacheCapacity())
	}
	if !s.o.GetDisableBlockCache() {
		var bcacher cache.Cacher
		if s.o.GetBlockCacheCapacity() > 0 {
			bcacher = s.o.GetBlockCacher().New(s.o.GetBlockCacheCapacity())
		}
		bcache = cache.NewCache(bcacher)
	}
	if !s.o.GetDisableBufferPool() {
		bpool = util.NewBufferPool(s.o.GetBlockSize() + 5)
	}
	return &tOps{
		s:            s,
		noSync:       s.o.GetNoSync(),
		evictRemoved: s.o.GetBlockCacheEvictRemoved(),
		cache:        cache.NewCache(cacher),
		bcache:       bcache,
		bpool:        bpool,
	}
}

// tWriter wraps the table writer. It keep track of file descriptor
// and added key range.

// tWriter底层调用了table.Writer
type tWriter struct {
	t *tOps

	fd storage.FileDesc
	w  storage.Writer
	tw *table.Writer

	first, last []byte
}

// Append key/value pair to the table.
func (w *tWriter) append(key, value []byte) error {
	if w.first == nil {
		w.first = append([]byte{}, key...)
	}
	// 这里last不一定是最大的key，只能说是最新的key
	w.last = append(w.last[:0], key...)
	// 写进block里面
	return w.tw.Append(key, value)
}

// Returns true if the table is empty.
func (w *tWriter) empty() bool {
	return w.first == nil
}

// Closes the storage.Writer.
func (w *tWriter) close() {
	if w.w != nil {
		w.w.Close()
		w.w = nil
	}
}

// Finalizes the table and returns table file.
func (w *tWriter) finish() (f *tFile, err error) {
	defer w.close()
	err = w.tw.Close()
	if err != nil {
		return
	}
	if !w.t.noSync {
		err = w.w.Sync()
		if err != nil {
			return
		}
	}
	f = newTableFile(w.fd, int64(w.tw.BytesLen()), internalKey(w.first), internalKey(w.last))
	return
}

// Drops the table.
func (w *tWriter) drop() {
	w.close()
	w.t.s.stor.Remove(w.fd)
	w.t.s.reuseFileNum(w.fd.Num)
	w.tw = nil
	w.first = nil
	w.last = nil
}
