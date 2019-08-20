// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type tSet struct {
	level int
	table *tFile
}

type version struct {
	// 独一无二的version id
	id int64 // unique monotonous increasing version id
	s  *session

	levels []tFiles

	// Level that should be compacted next and its compaction score.
	// Score < 1 means compaction is not strictly needed. These fields
	// are initialized by computeCompaction()
	cLevel int
	cScore float64

	cSeek unsafe.Pointer

	closing  bool
	ref      int
	released bool
}

// newVersion creates a new version with an unique monotonous increasing id.
func newVersion(s *session) *version {
	// 自增1
	id := atomic.AddInt64(&s.ntVersionId, 1)
	nv := &version{s: s, id: id - 1}   // 记录session和version的关系
	return nv
}

func (v *version) incref() {
	if v.released {
		panic("already released")
	}

	v.ref++  // 引用个数+1
	if v.ref == 1 {
		select {
		// v.s.refCh: chan *vTask
		/*
		type vTask struct {
			vid     int64
			files   []tFiles
			created time.Time
		}
		 */
		case v.s.refCh <- &vTask{vid: v.id, files: v.levels, created: time.Now()}:
			// We can use v.levels directly here since it is immutable.
			// v.s.closeC: chan struct{}
		case <-v.s.closeC:
			v.s.log("reference loop already exist")
		}
	}
}

func (v *version) releaseNB() {
	v.ref--
	if v.ref > 0 {
		// 引用计数还大于0,无法释放
		return
	} else if v.ref < 0 {
		panic("negative version ref")
	}
	select {
	case v.s.relCh <- &vTask{vid: v.id, files: v.levels, created: time.Now()}:
		// We can use v.levels directly here since it is immutable.
	case <-v.s.closeC:
		v.s.log("reference loop already exist")
	}

	v.released = true
}

func (v *version) release() {
	v.s.vmu.Lock()
	v.releaseNB()
	v.s.vmu.Unlock()
}

// aux-tFile
func (v *version) walkOverlapping(aux tFiles, ikey internalKey, f func(level int, t *tFile) bool, lf func(level int) bool) {
	ukey := ikey.ukey()

	// Aux level.
	if aux != nil {
		// 在指定范围内查找
		for _, t := range aux {
			// t是否与ukey相等
			// ukey是否在t(tFile)中
			if t.overlaps(v.s.icmp, ukey, ukey) {
				if !f(-1, t) {
					// 查询过程出错，不需要继续了
					return
				}
			}
		}

		// 遍历完
		if lf != nil && !lf(-1) {
			return
		}
	}

	// Walk tables level-by-level.
	// 一层一层地遍历表
	// levels属于version
	for level, tables := range v.levels {
		// 该level没有tFiles
		if len(tables) == 0 {
			continue
		}

		if level == 0 {
			// Level-0 files may overlap each other. Find all files that
			// overlap ukey.
			// level-0的文件会相互重叠的，找出所有的文件
			// 0层的文件比较特殊。由于0层的文件中可能存在key重合的情况，因此在0层中，文件编号大的sstable优先查找。
			// 理由是文件编号较大的sstable中存储的总是最新的数据。
			for _, t := range tables {
				if t.overlaps(v.s.icmp, ukey, ukey) {
					if !f(level, t) {
						return
					}
				}
			}
		} else {
			// 找出imax >= ikey的最小下标
			// level大于0,不相互重叠，自然找出imax>ikey

			// todo:有个疑问:imax,imin的值是什么时候写进去的
			if i := tables.searchMax(v.s.icmp, ikey); i < len(tables) {
				t := tables[i]
				// t.imax.ukey() >= ukey >= t.imin.ukey()
				if v.s.icmp.uCompare(ukey, t.imin.ukey()) >= 0 {
					if !f(level, t) {
						return
					}
				}
			}
		}

		if lf != nil && !lf(level) {
			return
		}
	}
}

func (v *version) get(aux tFiles, ikey internalKey, ro *opt.ReadOptions, noValue bool) (value []byte, tcomp bool, err error) {
	if v.closing {
		return nil, false, ErrClosed
	}

	ukey := ikey.ukey()  // 获取真正的key

	var (
		tset  *tSet
		tseek bool

		// Level-0.
		zfound bool
		zseq   uint64
		zkt    keyType
		zval   []byte
	)

	err = ErrNotFound

	// Since entries never hop across level, finding key/value
	// in smaller level make later levels irrelevant.

	// 遍历各个level，查找对应的值
	v.walkOverlapping(aux, ikey, func(level int, t *tFile) bool {
		if level >= 0 && !tseek {
			if tset == nil {
				tset = &tSet{level, t}
			} else {
				tseek = true
			}
		}

		var (
			fikey, fval []byte
			ferr        error
		)
		if noValue {
			// 只有key，没有value，调用findKey
			// 在sstble查找
			fikey, ferr = v.s.tops.findKey(t, ikey, ro)
		} else {
			fikey, fval, ferr = v.s.tops.find(t, ikey, ro)
		}

		switch ferr {
		case nil:  // 找到了
		case ErrNotFound:  // 没找到
			return true
		default:
			err = ferr
			return false
		}

		// func parseInternalKey(ik []byte) (ukey []byte, seq uint64, kt keyType, err error)
		// 反解查到的key
		if fukey, fseq, fkt, fkerr := parseInternalKey(fikey); fkerr == nil {
			// 核对
			if v.s.icmp.uCompare(ukey, fukey) == 0 {
				// Level <= 0 may overlaps each-other.
				// level会相互重叠
				if level <= 0 {
					if fseq >= zseq {
						// 找出更大的序列号，证明数据比较新
						zfound = true
						zseq = fseq
						zkt = fkt
						zval = fval
					}
				} else {
					// 返回false表示walkOverlapping内部会退出
					switch fkt {
					case keyTypeVal:
						value = fval
						err = nil
					case keyTypeDel:  // 找不到
					default:
						panic("leveldb: invalid internalKey type")
					}
					return false
				}
			}  // 正常情况下,不会存在不等于吧?
		} else {
			err = fkerr
			return false
		}

		return true
	}, func(level int) bool {
		if zfound {
			switch zkt {
			case keyTypeVal:
				value = zval
				err = nil
			case keyTypeDel:
			default:
				panic("leveldb: invalid internalKey type")
			}
			return false
		}
		// 没找到，继续执行?
		return true
	})

	if tseek && tset.table.consumeSeek() <= 0 {
		tcomp = atomic.CompareAndSwapPointer(&v.cSeek, nil, unsafe.Pointer(tset))
	}

	return
}

func (v *version) sampleSeek(ikey internalKey) (tcomp bool) {
	var tset *tSet

	v.walkOverlapping(nil, ikey, func(level int, t *tFile) bool {
		if tset == nil {
			tset = &tSet{level, t}
			return true
		}
		// seekLeft减去1
		if tset.table.consumeSeek() <= 0 {
			// 将v.cSeek置空,然后退出
			tcomp = atomic.CompareAndSwapPointer(&v.cSeek, nil, unsafe.Pointer(tset))
		}
		return false
	}, nil)

	return
}

func (v *version) getIterators(slice *util.Range, ro *opt.ReadOptions) (its []iterator.Iterator) {
	strict := opt.GetStrict(v.s.o.Options, ro, opt.StrictReader)
	// levels []tFiles
	for level, tables := range v.levels {
		if level == 0 {
			// Merge all level zero files together since they may overlap.
			for _, t := range tables {
				// 把所有的Reader收集起来
				its = append(its, v.s.tops.newIterator(t, slice, ro))
			}
		} else if len(tables) != 0 {
			// todo:这两个iterator有什么区别
			its = append(its, iterator.NewIndexedIterator(tables.newIndexIterator(v.s.tops, v.s.icmp, slice, ro), strict))
		}
	}
	return
}

/*
type versionStaging struct {
	base   *version
	levels []tablesScratch
}
 */
func (v *version) newStaging() *versionStaging {
	return &versionStaging{base: v}
}

// Spawn a new version based on this version.
func (v *version) spawn(r *sessionRecord, trivial bool) *version {
	staging := v.newStaging()  // &versionStaging{base: v}
	staging.commit(r)
	return staging.finish(trivial)
}

func (v *version) fillRecord(r *sessionRecord) {
	// 遍历所有的level和所有的table(tFiles)
	for level, tables := range v.levels {
		for _, t := range tables {
			r.addTableFile(level, t)
		}
	}
}

func (v *version) tLen(level int) int {
	if level < len(v.levels) {
		return len(v.levels[level])  // 计算tFiles的个数
	}
	return 0
}

func (v *version) offsetOf(ikey internalKey) (n int64, err error) {
	for level, tables := range v.levels {
		for _, t := range tables {
			if v.s.icmp.Compare(t.imax, ikey) <= 0 {
				// 整个文件小于ikey，偏移需要加上整个文件
				// Entire file is before "ikey", so just add the file size
				n += t.size
			} else if v.s.icmp.Compare(t.imin, ikey) > 0 {
				// Entire file is after "ikey", so ignore
				// 整个文件大于ikey,直接跳过
				if level > 0 {
					// Files other than level 0 are sorted by meta->min, so
					// no further files in this level will contain data for
					// "ikey".
					// 后面的key肯定比当前元素要大，因此可以跳过
					break
				}
			} else {
				// v.s.imp.Compare(t.imax, ikey) > 0 && v.s.icmp.Compare(t.imin, ikey) <= 0
				// "ikey" falls in the range for this table. Add the
				// approximate offset of "ikey" within the table.
				// todo:确认下这种情况是否从中间获取
				if m, err := v.s.tops.offsetOf(t, ikey); err == nil {
					n += m
				} else {
					return 0, err
				}
			}
		}
	}

	return
}

func (v *version) pickMemdbLevel(umin, umax []byte, maxLevel int) (level int) {
	if maxLevel > 0 {
		if len(v.levels) == 0 {
			// todo:version为空，直接往最大level来塞数据?
			return maxLevel
		}
		// 第一层是否与[umin, umax]不重叠
		if !v.levels[0].overlaps(v.s.icmp, umin, umax, true) {
			var overlaps tFiles
			for ; level < maxLevel; level++ {
				if pLevel := level + 1; pLevel >= len(v.levels) {
					return maxLevel  // 是否到最大层次了，说明只能放到最后一层
				} else if v.levels[pLevel].overlaps(v.s.icmp, umin, umax, false) { // 满足条件，就是要盖层
					break
				}
				if gpLevel := level + 2; gpLevel < len(v.levels) {
					overlaps = v.levels[gpLevel].getOverlaps(overlaps, v.s.icmp, umin, umax, false)
					if overlaps.size() > int64(v.s.o.GetCompactionGPOverlaps(level)) {  // todo:这个什么含义
						break
					}
				}
			}
		}
		// 与第一层重叠，直接返回第一层(0)
	}
	return
}

// 积分计算
func (v *version) computeCompaction() {
	// Precomputed best level for next compaction
	bestLevel := int(-1)
	bestScore := float64(-1)

	statFiles := make([]int, len(v.levels))
	statSizes := make([]string, len(v.levels))
	statScore := make([]string, len(v.levels))
	statTotSize := int64(0)

	for level, tables := range v.levels {
		var score float64
		size := tables.size()
		if level == 0 {
			// We treat level-0 specially by bounding the number of files
			// instead of number of bytes for two reasons:
			//
			// (1) With larger write-buffer sizes, it is nice not to do too
			// many level-0 compaction.
			//
			// (2) The files in level-0 are merged on every read and
			// therefore we wish to avoid too many files when the individual
			// file size is small (perhaps because of a small write-buffer
			// setting, or very high compression ratios, or lots of
			// overwrites/deletions).
			// 对于0层文件，该层的分数为文件总数／v.s.o.GetCompactionL0Trigger()（默认DefaultCompactionL0Trigger = 4）
			score = float64(len(tables)) / float64(v.s.o.GetCompactionL0Trigger())
		} else {
			// 对于非0层文件，该层的分数为文件数据总量／数据总量上限；
			score = float64(size) / float64(v.s.o.GetCompactionTotalSize(level))
		}

		// 记录最高分数
		if score > bestScore {
			bestLevel = level
			bestScore = score
		}

		statFiles[level] = len(tables)  // 记录这一层的sst文件个数
		statSizes[level] = shortenb(int(size))  // 记录这一层的文件数据总量
		statScore[level] = fmt.Sprintf("%.2f", score)
		statTotSize += size
	}

	v.cLevel = bestLevel
	v.cScore = bestScore

	v.s.logf("version@stat F·%v S·%s%v Sc·%v", statFiles, shortenb(int(statTotSize)), statSizes, statScore)
}

// 是否需要压缩
func (v *version) needCompaction() bool {
	// 最高得分记录超过1 || 文件读取次数过多
	return v.cScore >= 1 || atomic.LoadPointer(&v.cSeek) != nil
}

type tablesScratch struct {
	added   map[int64]atRecord
	deleted map[int64]struct{}
}

type versionStaging struct {
	base   *version
	levels []tablesScratch
}

func (p *versionStaging) getScratch(level int) *tablesScratch {
	if level >= len(p.levels) {
		// 数据长度不够，扩展数据
		newLevels := make([]tablesScratch, level+1)
		copy(newLevels, p.levels)
		p.levels = newLevels
	}
	return &(p.levels[level])
}

// 这里的目的是修正versionStaging里面的成员
func (p *versionStaging) commit(r *sessionRecord) {
	// Deleted tables.
	for _, r := range r.deletedTables {
		// 删除了哪些sstable文件（由于compaction导致）
		scratch := p.getScratch(r.level)  // 分配足够的TableScratch
		if r.level < len(p.base.levels) && len(p.base.levels[r.level]) > 0 {
			if scratch.deleted == nil {
				scratch.deleted = make(map[int64]struct{})
			}
			// 在scratch里面标记已经删除
			scratch.deleted[r.num] = struct{}{}   // 在level对应的tableScratch里面的deleted成员做记号
		}
		if scratch.added != nil {
			delete(scratch.added, r.num)  // 从added里面删除该r.num
		}
	}

	// New tables.
	for _, r := range r.addedTables {
		scratch := p.getScratch(r.level)
		if scratch.added == nil {
			scratch.added = make(map[int64]atRecord)
		}
		scratch.added[r.num] = r
		if scratch.deleted != nil {
			delete(scratch.deleted, r.num)
		}
	}
}


func (p *versionStaging) finish(trivial bool) *version {
	// Build new version.
	/*
	func newVersion(s *session) *version {
		// 自增1
		id := atomic.AddInt64(&s.ntVersionId, 1)
		nv := &version{s: s, id: id - 1}
		return nv
	}
	 */
	// 新建一个版本
	nv := newVersion(p.base.s)
	/*
	type versionStaging struct {
		base   *version
		levels []tablesScratch
	}
	type version struct {
		// 独一无二的version id
		id int64 // unique monotonous increasing version id
		s  *session

		levels []tFiles

		// Level that should be compacted next and its compaction score.
		// Score < 1 means compaction is not strictly needed. These fields
		// are initialized by computeCompaction()
		cLevel int
		cScore float64

		cSeek unsafe.Pointer

		closing  bool
		ref      int
		released bool
	}
	 */
	numLevel := len(p.levels)
	if len(p.base.levels) > numLevel {
		numLevel = len(p.base.levels)
	}
	nv.levels = make([]tFiles, numLevel)
	for level := 0; level < numLevel; level++ {
		var baseTabels tFiles
		// 遍历每一层
		if level < len(p.base.levels) {
			baseTabels = p.base.levels[level]
		}

		if level < len(p.levels) {
			scratch := p.levels[level]

			// Short circuit if there is no change at all.
			if len(scratch.added) == 0 && len(scratch.deleted) == 0 {
				nv.levels[level] = baseTabels
				continue
			}

			var nt tFiles
			// Prealloc list if possible.
			if n := len(baseTabels) + len(scratch.added) - len(scratch.deleted); n > 0 {
				nt = make(tFiles, 0, n)  // 分配合理的空间
			}

			// Base tables.
			for _, t := range baseTabels {
				if _, ok := scratch.deleted[t.fd.Num]; ok {
					continue
				}
				if _, ok := scratch.added[t.fd.Num]; ok {
					continue
				}
				// nt = new table
				nt = append(nt, t)
			}

			// Avoid resort if only files in this level are deleted
			if len(scratch.added) == 0 {
				nv.levels[level] = nt
				continue
			}

			// For normal table compaction, one compaction will only involve two levels
			// of files. And the new files generated after merging the source level and
			// source+1 level related files can be inserted as a whole into source+1 level
			// without any overlap with the other source+1 files.
			//
			// When the amount of data maintained by leveldb is large, the number of files
			// per level will be very large. While qsort is very inefficient for sorting
			// already ordered arrays. Therefore, for the normal table compaction, we use
			// binary search here to find the insert index to insert a batch of new added
			// files directly instead of using qsort.
			if trivial && len(scratch.added) > 0 {
				added := make(tFiles, 0, len(scratch.added))
				for _, r := range scratch.added {
					added = append(added, tableFileFromRecord(r))
				}
				if level == 0 {
					added.sortByNum()  // level-0,按照num排序
					index := nt.searchNumLess(added[len(added)-1].fd.Num)  // 二分查找
					nt = append(nt[:index], append(added, nt[index:]...)...)
				} else {
					added.sortByKey(p.base.s.icmp)  // level>0层，按照key来排序
					_, amax := added.getRange(p.base.s.icmp)  // 找出added的imax
					index := nt.searchMin(p.base.s.icmp, amax)  // 找出index的imin都比imax都要大
					nt = append(nt[:index], append(added, nt[index:]...)...)
				}
				nv.levels[level] = nt
				continue
			}

			// New tables.
			for _, r := range scratch.added {
				// 逐个atRecord转tFiles
				nt = append(nt, tableFileFromRecord(r))
			}

			// 对新结果进行重新排序，不在上面做排序操作
			if len(nt) != 0 {
				// Sort tables.
				if level == 0 {
					nt.sortByNum()
				} else {
					nt.sortByKey(p.base.s.icmp)
				}

				nv.levels[level] = nt  // newVersion.levels[level] = new Table
			}
		} else {
			nv.levels[level] = baseTabels  // 该层并未改动
		}
	}

	// Trim levels.
	n := len(nv.levels)
	for ; n > 0 && nv.levels[n-1] == nil; n-- {
	}
	nv.levels = nv.levels[:n] // 取出空的level层

	// Compute compaction score for new version.
	// 根据新的版本计算积分
	nv.computeCompaction()

	return nv
}

type versionReleaser struct {
	v    *version
	once bool
}

func (vr *versionReleaser) Release() {
	v := vr.v
	v.s.vmu.Lock()
	if !vr.once {
		v.releaseNB()
		vr.once = true
	}
	v.s.vmu.Unlock()
}
