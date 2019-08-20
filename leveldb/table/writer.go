// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// 求出a,b两个[]byte的共同前缀长度
func sharedPrefixLen(a, b []byte) int {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// 写一个data block
type blockWriter struct {
	restartInterval int
	buf             util.Buffer
	nEntries        int  // entry的个数
	prevKey         []byte  // 当前位置的前一个key
	restarts        []uint32
	scratch         []byte
}

func (w *blockWriter) append(key, value []byte) {
	nShared := 0
	// 每隔w.restartInterval写一个完整的key
	if w.nEntries%w.restartInterval == 0 {
		// restart
		w.restarts = append(w.restarts, uint32(w.buf.Len()))
	} else {
		// 求出key和prevKey的共同前缀长度
		nShared = sharedPrefixLen(w.prevKey, key)
	}
	// Shared key length
	n := binary.PutUvarint(w.scratch[0:], uint64(nShared))
	// Unshared key length
	n += binary.PutUvarint(w.scratch[n:], uint64(len(key)-nShared))
	// Value length
	n += binary.PutUvarint(w.scratch[n:], uint64(len(value)))
	// 写入到w.buf里面
	w.buf.Write(w.scratch[:n])
	// Unshared key的内容
	w.buf.Write(key[nShared:])
	// Value的内容
	w.buf.Write(value)
	w.prevKey = append(w.prevKey[:0], key...)  // 更新prevKey
	w.nEntries++  // 写入一个entry,nEntries累计加1
}

// 为blockWriter写入trailer
func (w *blockWriter) finish() {
	// Write restarts entry.
	if w.nEntries == 0 {
		// Must have at least one restart entry.
		w.restarts = append(w.restarts, 0)
	}
	w.restarts = append(w.restarts, uint32(len(w.restarts)))
	for _, x := range w.restarts {
		// 记录每个restartPoint
		buf4 := w.buf.Alloc(4)
		binary.LittleEndian.PutUint32(buf4, x)
	}
}

func (w *blockWriter) reset() {
	w.buf.Reset()
	w.nEntries = 0
	w.restarts = w.restarts[:0]  // []
}

// 整个block的长度:entry[] + trailer
func (w *blockWriter) bytesLen() int {
	restartsLen := len(w.restarts)
	if restartsLen == 0 {
		restartsLen = 1
	}
	// buf长度+restartPoint占空间（每个4字节）+restartPoint总数（4个字节）
	return w.buf.Len() + 4*restartsLen + 4
}

// 写一个filter block
type filterWriter struct {
	generator filter.FilterGenerator
	buf       util.Buffer
	nKeys     int
	offsets   []uint32
}

func (w *filterWriter) add(key []byte) {
	if w.generator == nil {
		return
	}
	w.generator.Add(key)  // 布隆过滤加上一个样本key
	w.nKeys++  // 过滤的key个数加1
}

func (w *filterWriter) flush(offset uint64) {
	if w.generator == nil {
		return
	}
	// 这个循环看不太明白
	// 每个filterBase添加一个offset点
	for x := int(offset / filterBase); x > len(w.offsets); {  // 2048
		w.generate()
	}
}

// 写成一个filter block
func (w *filterWriter) finish() {
	if w.generator == nil {
		return
	}
	// Generate last keys.

	if w.nKeys > 0 {
		w.generate()
	}
	// 每个filter block的offset
	w.offsets = append(w.offsets, uint32(w.buf.Len()))
	for _, x := range w.offsets {
		buf4 := w.buf.Alloc(4)
		binary.LittleEndian.PutUint32(buf4, x)
	}
	w.buf.WriteByte(filterBaseLg)
}

func (w *filterWriter) generate() {
	// Record offset.
	w.offsets = append(w.offsets, uint32(w.buf.Len()))
	// Generate filters.
	if w.nKeys > 0 {
		// 将filter的结构写到Buffer里面
		w.generator.Generate(&w.buf)
		// nKey置0，重新计算
		w.nKeys = 0
	}
}

// Writer is a table writer.
// 将block写入到文件
type Writer struct {
	writer io.Writer
	err    error
	// Options
	cmp         comparer.Comparer
	filter      filter.Filter
	compression opt.Compression
	blockSize   int

	dataBlock   blockWriter
	indexBlock  blockWriter
	filterBlock filterWriter

	// pendingBH记录了上一个dataBlock的索引信息，当下一个dataBlock的数据写入时，将该索引信息写入indexBlock中。
	pendingBH   blockHandle
	offset      uint64
	nEntries    int
	// Scratch allocated enough for 5 uvarint. Block writer should not use
	// first 20-bytes since it will be used to encode block handle, which
	// then passed to the block writer itself.
	scratch            [50]byte
	comparerScratch    []byte
	compressionScratch []byte
}

// 返回下标blockHandle
func (w *Writer) writeBlock(buf *util.Buffer, compression opt.Compression) (bh blockHandle, err error) {
	// Compress the buffer if necessary.
	var b []byte
	// compression：压缩算法
	if compression == opt.SnappyCompression {
		// Allocate scratch enough for compression and block trailer.
		if n := snappy.MaxEncodedLen(buf.Len()) + blockTrailerLen; len(w.compressionScratch) < n {
			w.compressionScratch = make([]byte, n)
		}
		compressed := snappy.Encode(w.compressionScratch, buf.Bytes())
		n := len(compressed)
		b = compressed[:n+blockTrailerLen]
		b[n] = blockTypeSnappyCompression
	} else {
		tmp := buf.Alloc(blockTrailerLen)  // 给buf额外分配一个blockTrailer的长度
		tmp[0] = blockTypeNoCompression
		b = buf.Bytes()  // 返回buf未读的部分
	}

	// Calculate the checksum.
	n := len(b) - 4
	// 计算校验和
	// |data|compression type|CRC|
	checksum := util.NewCRC(b[:n]).Value()
	binary.LittleEndian.PutUint32(b[n:], checksum)

	// Write the buffer to the file.
	// 将buffer写入到文件
	_, err = w.writer.Write(b)
	if err != nil {
		return
	}
	bh = blockHandle{w.offset, uint64(len(b) - blockTrailerLen)}
	w.offset += uint64(len(b))
	return
}

func (w *Writer) flushPendingBH(key []byte) {
	// 已经把上一个block的索引信息写入到index block中，无需执行flushPendingBH
	if w.pendingBH.length == 0 {
		return
	}
	var separator []byte
	if len(key) == 0 {
		// w.comparerScratch[:0]为nil
		// 要比w.dataBlock.prevKey的末尾加一
		separator = w.cmp.Successor(w.comparerScratch[:0], w.dataBlock.prevKey)
	} else {
		// 找出w.dataBlock.prevKey和key的公共部分，然后求出公共部分的w.dataBlock.prevKey末尾加一
		separator = w.cmp.Separator(w.comparerScratch[:0], w.dataBlock.prevKey, key)
	}
	if separator == nil {
		// separator为空，直接给w.dataBlock.prevKey
		separator = w.dataBlock.prevKey
	} else {
		w.comparerScratch = separator
	}
	// w.pendingBH转成w.scratch[:20]([]byte)
	n := encodeBlockHandle(w.scratch[:20], w.pendingBH)
	// Append the block handle to the index block.
	// 把上一个data block的索引信息写入到index block

	// max key | Offset | Length
	// 组成这个格式
	w.indexBlock.append(separator, w.scratch[:n])
	// Reset prev key of the data block.
	// 重置datablock的preKey
	w.dataBlock.prevKey = w.dataBlock.prevKey[:0]
	// Clear pending block handle.
	// 重置pendingBH
	w.pendingBH = blockHandle{}
}

func (w *Writer) finishBlock() error {
	w.dataBlock.finish()  // 给data block加上一个trailer
	bh, err := w.writeBlock(&w.dataBlock.buf, w.compression)
	if err != nil {
		return err
	}
	w.pendingBH = bh
	// Reset the data block.
	// 重置data block
	w.dataBlock.reset()
	// Flush the filter block.
	w.filterBlock.flush(w.offset)
	return nil
}

// Append appends key/value pair to the table. The keys passed must
// be in increasing order.
//
// It is safe to modify the contents of the arguments after Append returns.
func (w *Writer) Append(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	// prevKey大于key，属于不合法的东西
	// todo:这里需要看上层在哪里做的拦截和限制
	if w.nEntries > 0 && w.cmp.Compare(w.dataBlock.prevKey, key) >= 0 {
		w.err = fmt.Errorf("leveldb/table: Writer: keys are not in increasing order: %q, %q", w.dataBlock.prevKey, key)
		return w.err
	}

	// 若本次写入为新dataBlock的第一次写入，则将上一个dataBlock的索引信息写入。
	w.flushPendingBH(key)
	// Append key/value pair to the data block.
	// 把key,value加入到dataBlock
	w.dataBlock.append(key, value)
	// Add key to the filter block.
	w.filterBlock.add(key)

	// Finish the data block if block size target reached.
	// datablock的大小超过w.blockSize(配置决定的),也需要finishBlock()
	// 若datablock中的数据超过预定上限，则标志着本次datablock写入结束，将内容刷新到磁盘文件中
	if w.dataBlock.bytesLen() >= w.blockSize {
		if err := w.finishBlock(); err != nil {
			w.err = err
			return w.err
		}
	}
	w.nEntries++
	return nil
}

// BlocksLen returns number of blocks written so far.
func (w *Writer) BlocksLen() int {
	n := w.indexBlock.nEntries
	if w.pendingBH.length > 0 {
		// Includes the pending block.
		n++
	}
	return n
}

// EntriesLen returns number of entries added so far.
// 返回保存的entry个数
func (w *Writer) EntriesLen() int {
	return w.nEntries
}

// BytesLen returns number of bytes written so far.
// 至今写了多少个字节
func (w *Writer) BytesLen() int {
	return int(w.offset)
}

// Close will finalize the table. Calling Append is not possible
// after Close, but calling BlocksLen, EntriesLen and BytesLen
// is still possible.
// 在Close函数里面进行文件落盘
func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}

	// Write the last data block. Or empty data block if there
	// aren't any data blocks at all.
	// 1.写dataBlock
	if w.dataBlock.nEntries > 0 || w.nEntries == 0 {
		if err := w.finishBlock(); err != nil {
			w.err = err
			return w.err
		}
	}
	w.flushPendingBH(nil)

	// Write the filter block.
	// 2.写filterBlock
	var filterBH blockHandle
	w.filterBlock.finish()
	if buf := &w.filterBlock.buf; buf.Len() > 0 {
		// 写上filterBlock
		filterBH, w.err = w.writeBlock(buf, opt.NoCompression)
		if w.err != nil {
			return w.err
		}
	}

	// Write the metaindex block.
	if filterBH.length > 0 {
		key := []byte("filter." + w.filter.Name())
		n := encodeBlockHandle(w.scratch[:20], filterBH)
		w.dataBlock.append(key, w.scratch[:n])
	}
	w.dataBlock.finish()
	// 3.写metaindex block
	metaindexBH, err := w.writeBlock(&w.dataBlock.buf, w.compression)
	if err != nil {
		w.err = err
		return w.err
	}

	// Write the index block.
	// 4.写index block
	w.indexBlock.finish()
	indexBH, err := w.writeBlock(&w.indexBlock.buf, w.compression)
	if err != nil {
		w.err = err
		return w.err
	}

	// Write the table footer.
	footer := w.scratch[:footerLen]
	for i := range footer {
		footer[i] = 0
	}
	n := encodeBlockHandle(footer, metaindexBH)
	encodeBlockHandle(footer[n:], indexBH)
	copy(footer[footerLen-len(magic):], magic)
	// 5.写footer
	if _, err := w.writer.Write(footer); err != nil {
		w.err = err
		return w.err
	}
	w.offset += footerLen

	w.err = errors.New("leveldb/table: writer is closed")
	return nil
}

// NewWriter creates a new initialized table writer for the file.
//
// Table writer is not safe for concurrent use.
func NewWriter(f io.Writer, o *opt.Options) *Writer {
	w := &Writer{
		writer:          f,  // 入参决定
		cmp:             o.GetComparer(),  // 入参配置决定
		filter:          o.GetFilter(),    // 入参配置决定
		compression:     o.GetCompression(),  // 入参配置决定
		blockSize:       o.GetBlockSize(),  // 入参配置决定
		comparerScratch: make([]byte, 0),
	}
	// data block
	w.dataBlock.restartInterval = o.GetBlockRestartInterval()
	// The first 20-bytes are used for encoding block handle.
	w.dataBlock.scratch = w.scratch[20:]
	// index block
	w.indexBlock.restartInterval = 1
	w.indexBlock.scratch = w.scratch[20:]
	// filter block
	if w.filter != nil {
		w.filterBlock.generator = w.filter.NewGenerator()  // 生成一个filter block
		w.filterBlock.flush(0)
	}
	return w
}
