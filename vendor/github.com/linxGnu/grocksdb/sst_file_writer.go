package grocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

import (
	"unsafe"
)

// SSTFileWriter is used to create sst files that can be added to database later.
// All keys in files generated by SstFileWriter will have sequence number = 0.
type SSTFileWriter struct {
	c *C.rocksdb_sstfilewriter_t
}

// NewSSTFileWriter creates an SSTFileWriter object.
func NewSSTFileWriter(opts *EnvOptions, dbOpts *Options) *SSTFileWriter {
	c := C.rocksdb_sstfilewriter_create(opts.c, dbOpts.c)
	return &SSTFileWriter{c: c}
}

// NewSSTFileWriterWithComparator creates an SSTFileWriter object with comparator.
func NewSSTFileWriterWithComparator(opts *EnvOptions, dbOpts *Options, cmp *Comparator) *SSTFileWriter {
	cmp_ := unsafe.Pointer(cmp.c)
	return NewSSTFileWriterWithNativeComparator(opts, dbOpts, cmp_)
}

// NewSSTFileWriterWithNativeComparator creates an SSTFileWriter object with native comparator.
func NewSSTFileWriterWithNativeComparator(opts *EnvOptions, dbOpts *Options, cmp unsafe.Pointer) *SSTFileWriter {
	cmp_ := (*C.rocksdb_comparator_t)(cmp)
	c := C.rocksdb_sstfilewriter_create_with_comparator(opts.c, dbOpts.c, cmp_)
	return &SSTFileWriter{c: c}
}

// Open prepares SstFileWriter to write into file located at "path".
func (w *SSTFileWriter) Open(path string) (err error) {
	var (
		cErr  *C.char
		cPath = C.CString(path)
	)

	C.rocksdb_sstfilewriter_open(w.c, cPath, &cErr)
	err = fromCError(cErr)

	C.free(unsafe.Pointer(cPath))
	return
}

// Add adds key, value to currently opened file.
// REQUIRES: key is after any previously added key according to comparator.
func (w *SSTFileWriter) Add(key, value []byte) (err error) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	var cErr *C.char
	C.rocksdb_sstfilewriter_add(w.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	err = fromCError(cErr)
	return
}

// Put key, value to currently opened file.
func (w *SSTFileWriter) Put(key, value []byte) (err error) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	var cErr *C.char
	C.rocksdb_sstfilewriter_put(w.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	err = fromCError(cErr)
	return
}

// Merge key, value to currently opened file.
func (w *SSTFileWriter) Merge(key, value []byte) (err error) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	var cErr *C.char
	C.rocksdb_sstfilewriter_merge(w.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), &cErr)
	err = fromCError(cErr)
	return
}

// Delete key from currently opened file.
func (w *SSTFileWriter) Delete(key []byte) (err error) {
	cKey := byteToChar(key)
	var cErr *C.char
	C.rocksdb_sstfilewriter_delete(w.c, cKey, C.size_t(len(key)), &cErr)
	err = fromCError(cErr)
	return
}

// FileSize returns size of currently opened file.
func (w *SSTFileWriter) FileSize() (size uint64) {
	C.rocksdb_sstfilewriter_file_size(w.c, (*C.uint64_t)(&size))
	return
}

// Finish finishes writing to sst file and close file.
func (w *SSTFileWriter) Finish() (err error) {
	var cErr *C.char
	C.rocksdb_sstfilewriter_finish(w.c, &cErr)
	err = fromCError(cErr)
	return
}

// Destroy destroys the SSTFileWriter object.
func (w *SSTFileWriter) Destroy() {
	C.rocksdb_sstfilewriter_destroy(w.c)
	w.c = nil
}