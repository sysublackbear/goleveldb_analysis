// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// ErrManifestCorrupted records manifest corruption. This error will be
// wrapped with errors.ErrCorrupted.
type ErrManifestCorrupted struct {
	Field  string
	Reason string
}

func (e *ErrManifestCorrupted) Error() string {
	return fmt.Sprintf("leveldb: manifest corrupted (field '%s'): %s", e.Field, e.Reason)
}

func newErrManifestCorrupted(fd storage.FileDesc, field, reason string) error {
	return errors.NewErrCorrupted(fd, &ErrManifestCorrupted{field, reason})
}

// session represent a persistent database session.
type session struct {
	// Need 64-bit alignment.
	// 当前未被使用的文件fd num
	stNextFileNum    int64 // current unused file number
	// 当前未被使用的日志文件fd num
	stJournalNum     int64 // current journal file number; need external synchronization
	// 上一个日志文件
	stPrevJournalNum int64 // prev journal file number; no longer used; for compatibility with older version of leveldb
	stTempFileNum    int64  // 临时文件
	// 序列号
	stSeqNum         uint64 // last mem compacted seq; need external synchronization

	stor     *iStorage
	storLock storage.Locker
	o        *cachedOptions
	icmp     *iComparer
	tops     *tOps

	manifest       *journal.Writer
	manifestWriter storage.Writer
	manifestFd     storage.FileDesc

	stCompPtrs  []internalKey // compaction pointers; need external synchronization
	stVersion   *version      // current version
	ntVersionId int64         // next version id to assign
	refCh       chan *vTask
	relCh       chan *vTask
	deltaCh     chan *vDelta
	abandon     chan int64
	closeC      chan struct{}
	closeW      sync.WaitGroup
	vmu         sync.Mutex

	// Testing fields
	fileRefCh chan chan map[int64]int // channel used to pass current reference stat
}

// Creates new initialized session instance.
// storage.Storage接口由调用方传入去实现
func newSession(stor storage.Storage, o *opt.Options) (s *session, err error) {
	if stor == nil {
		return nil, os.ErrInvalid
	}
	storLock, err := stor.Lock()  // iStorage的Lock()方法，返回storage.Locker
	if err != nil {
		return
	}
	s = &session{
		stor:      newIStorage(stor),
		storLock:  storLock,
		// 初始化channel
		refCh:     make(chan *vTask),
		relCh:     make(chan *vTask),
		deltaCh:   make(chan *vDelta),
		abandon:   make(chan int64),
		fileRefCh: make(chan chan map[int64]int),
		closeC:    make(chan struct{}),
	}
	s.setOptions(o)
	s.tops = newTableOps(s)

	s.closeW.Add(1)
	// todo:refLoop这个逻辑注意要看
	go s.refLoop()
	s.setVersion(nil, newVersion(s))
	s.log("log@legend F·NumFile S·FileSize N·Entry C·BadEntry B·BadBlock Ke·KeyError D·DroppedEntry L·Level Q·SeqNum T·TimeElapsed")
	return
}

// Close session.
func (s *session) close() {
	// 关闭tOps.bpool/cache/bcache
	s.tops.close()
	if s.manifest != nil {
		s.manifest.Close()
	}
	if s.manifestWriter != nil {
		s.manifestWriter.Close()
	}
	// 重置成员变量
	s.manifest = nil
	s.manifestWriter = nil
	s.setVersion(nil, &version{s: s, closing: true, id: s.ntVersionId})

	// Close all background goroutines
	close(s.closeC)
	s.closeW.Wait()
}

// Release session lock.
// 释放storage.Lock
func (s *session) release() {
	s.storLock.Unlock()
}

// Create a new database session; need external synchronization.
// 创建一个数据库session
func (s *session) create() error {
	// create manifest
	return s.newManifest(nil, nil)
}

// Recover a database session; need external synchronization.
func (s *session) recover() (err error) {
	defer func() {
		if os.IsNotExist(err) {  // 如果错误是文件不存在
			// Don't return os.ErrNotExist if the underlying storage contains
			// other files that belong to LevelDB. So the DB won't get trashed.
			if fds, _ := s.stor.List(storage.TypeAll); len(fds) > 0 {
				err = &errors.ErrCorrupted{Fd: storage.FileDesc{Type: storage.TypeManifest}, Err: &errors.ErrMissingFiles{}}
			}
		}
	}()

	// 获取meta属性
	// 读取mainfest文件
	// 获取每一条SessionRecord记录
	fd, err := s.stor.GetMeta()   // 会在这里面做将非current文件指向的其他过期的manifest文件删除
	if err != nil {
		return
	}

	reader, err := s.stor.Open(fd)
	if err != nil {
		return
	}
	defer reader.Close()

	var (
		// Options.
		// 设置Manifest属性
		strict = s.o.GetStrict(opt.StrictManifest)

		// 初始化journal.Reader
		// 读取journal进行恢复
		jr      = journal.NewReader(reader, dropper{s, fd}, strict, true)
		rec     = &sessionRecord{}
		// return &versionStaging{base: s.stVersion}
		staging = s.stVersion.newStaging()
	)
	for {
		var r io.Reader
		// jr.Next():取出一个chunk
		r, err = jr.Next()
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return errors.SetFd(err, fd)
		}

		// r: io.Reader
		// rec: sessionRecord
		// 通过rec.decode(r),将r的内容decode成
		err = rec.decode(r)
		if err == nil {
			// save compact pointers
			for _, r := range rec.compPtrs {
				s.setCompPtr(r.level, internalKey(r.ikey))
			}
			// commit record to version staging
			// versionStaging commit
			staging.commit(rec)
		} else {
			err = errors.SetFd(err, fd)
			if strict || !errors.IsCorrupted(err) {
				return
			}
			s.logf("manifest error: %v (skipped)", errors.SetFd(err, fd))
		}
		// 重置SessionRecord
		rec.resetCompPtrs()
		rec.resetAddedTables()
		rec.resetDeletedTables()
	}

	switch {
	case !rec.has(recComparer):
		return newErrManifestCorrupted(fd, "comparer", "missing")
	case rec.comparer != s.icmp.uName():
		return newErrManifestCorrupted(fd, "comparer", fmt.Sprintf("mismatch: want '%s', got '%s'", s.icmp.uName(), rec.comparer))
	case !rec.has(recNextFileNum):
		return newErrManifestCorrupted(fd, "next-file-num", "missing")
	case !rec.has(recJournalNum):
		return newErrManifestCorrupted(fd, "journal-file-num", "missing")
	case !rec.has(recSeqNum):
		return newErrManifestCorrupted(fd, "seq-num", "missing")
	}

	s.manifestFd = fd
	s.setVersion(rec, staging.finish(false))
	s.setNextFileNum(rec.nextFileNum)
	s.recordCommited(rec)
	return nil
}

// Commit session; need external synchronization.
func (s *session) commit(r *sessionRecord, trivial bool) (err error) {
	v := s.version()
	defer v.release()

	// spawn new version based on current version
	// 基于当前版本创建出新的版本
	nv := v.spawn(r, trivial)

	// abandon useless version id to prevent blocking version processing loop.
	defer func() {
		if err != nil {
			s.abandon <- nv.id
			s.logf("commit@abandon useless vid D%d", nv.id)
		}
	}()

	if s.manifest == nil {
		// manifest journal writer not yet created, create one
		err = s.newManifest(r, nv)
	} else {
		err = s.flushManifest(r)  // 持久化manifest到journal中
	}

	// finally, apply new version if no error rise
	if err == nil {
		s.setVersion(r, nv)
	}

	return
}
