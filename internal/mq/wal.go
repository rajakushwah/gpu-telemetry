package mq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
)

const (
	frameHeader = 8        // uint32 length + uint32 CRC32C
	maxFrame    = 16 << 20 // sanity bound when reading a possibly corrupt length
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
	errTorn  = errors.New("torn or corrupt frame")
)

// segment is one WAL file; base is the offset of its first record.
type segment struct {
	base uint64
	path string
}

// wal persists records as [len|crc32c|json] frames in rolling segment files.
// It is not safe for concurrent use; Partition serialises access.
type wal struct {
	dir      string
	segBytes int64
	segs     []segment
	f        *os.File
	size     int64
	dirty    bool
}

// openWAL replays every valid record through fn, truncates a torn tail left
// by a crash, and opens the newest segment for appending.
func openWAL(dir string, segBytes int64, fn func(Record)) (*wal, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths) // zero-padded names sort by base offset
	w := &wal{dir: dir, segBytes: segBytes}
	for _, p := range paths {
		var base uint64
		if _, err := fmt.Sscanf(filepath.Base(p), "%020d.log", &base); err != nil {
			continue
		}
		if err := replay(p, fn); err != nil {
			return nil, fmt.Errorf("replay %s: %w", p, err)
		}
		w.segs = append(w.segs, segment{base, p})
	}
	if len(w.segs) == 0 {
		return w, w.roll(0)
	}
	last := w.segs[len(w.segs)-1].path
	if w.f, err = os.OpenFile(last, os.O_WRONLY|os.O_APPEND, 0); err != nil {
		return nil, err
	}
	info, err := w.f.Stat()
	if err != nil {
		return nil, err
	}
	w.size = info.Size()
	return w, nil
}

// replay streams one segment's records and cuts off any partial frame at the end.
func replay(path string, fn func(Record)) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	valid, err := readFrames(bufio.NewReader(f), fn)
	if errors.Is(err, errTorn) {
		slog.Warn("truncating torn WAL tail", "path", path, "at", valid)
		return f.Truncate(valid)
	}
	return err
}

// readFrames decodes frames until EOF and returns the byte length of the valid prefix.
func readFrames(r io.Reader, fn func(Record)) (int64, error) {
	var n int64
	hdr := make([]byte, frameHeader)
	for {
		if _, err := io.ReadFull(r, hdr); err == io.EOF {
			return n, nil
		} else if err == io.ErrUnexpectedEOF {
			return n, errTorn
		} else if err != nil {
			return n, err
		}
		size := binary.BigEndian.Uint32(hdr[:4])
		if size > maxFrame {
			return n, errTorn
		}
		body := make([]byte, size)
		if _, err := io.ReadFull(r, body); err != nil {
			return n, errTorn
		}
		var rec Record
		if crc32.Checksum(body, crcTable) != binary.BigEndian.Uint32(hdr[4:]) || json.Unmarshal(body, &rec) != nil {
			return n, errTorn
		}
		fn(rec)
		n += frameHeader + int64(size)
	}
}

// append writes a batch with a single write call. A full segment is rolled
// before writing so a failed roll never leaves records on disk unacknowledged.
func (w *wal) append(recs []Record) error {
	if w.size >= w.segBytes {
		if err := w.roll(recs[0].Offset); err != nil {
			return err
		}
	}
	var buf bytes.Buffer
	for _, r := range recs {
		body, err := json.Marshal(r)
		if err != nil {
			return err
		}
		var hdr [frameHeader]byte
		binary.BigEndian.PutUint32(hdr[:4], uint32(len(body)))
		binary.BigEndian.PutUint32(hdr[4:], crc32.Checksum(body, crcTable))
		buf.Write(hdr[:])
		buf.Write(body)
	}
	n, err := w.f.Write(buf.Bytes())
	if err != nil {
		_ = w.f.Truncate(w.size) // drop a partial write so later frames stay readable
		return err
	}
	w.size += int64(n)
	w.dirty = true
	return nil
}

// roll closes the active segment and starts a new one at base.
func (w *wal) roll(base uint64) error {
	if w.f != nil {
		if err := w.close(); err != nil {
			return err
		}
	}
	path := filepath.Join(w.dir, fmt.Sprintf("%020d.log", base))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	w.f, w.size = f, 0
	w.segs = append(w.segs, segment{base, path})
	return nil
}

// dropBefore deletes whole segments whose records are all below offset.
func (w *wal) dropBefore(offset uint64) {
	for len(w.segs) > 1 && w.segs[1].base <= offset {
		if err := os.Remove(w.segs[0].path); err != nil {
			slog.Warn("remove WAL segment", "path", w.segs[0].path, "err", err)
			return
		}
		w.segs = w.segs[1:]
	}
}

// lastBase is the base offset of the newest segment (the next offset if it is empty).
func (w *wal) lastBase() uint64 { return w.segs[len(w.segs)-1].base }

// sync fsyncs the active segment if anything was written since the last sync.
func (w *wal) sync() error {
	if !w.dirty {
		return nil
	}
	w.dirty = false
	return w.f.Sync()
}

func (w *wal) close() error {
	return errors.Join(w.sync(), w.f.Close())
}
