// Package ltx implements the project's independent LTX v2 snapshot and delta profile.
package ltx

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"

	"github.com/pierrec/lz4/v4"
)

const (
	Magic       = "LTX1"
	Version     = 2
	HeaderSize  = 100
	PageHdrSize = 4
	TrailerSize = 16

	ChecksumFlag uint64 = 1 << 63
	LockByte            = 0x40000000
)

var (
	ErrInvalidFile      = errors.New("invalid LTX v2 file")
	ErrChecksumMismatch = errors.New("LTX checksum mismatch")
	crcTable            = crc64.MakeTable(crc64.ISO)
)

// Header contains the fields used by the LTX v2 100-byte header.
type Header struct {
	Flags            uint32
	PageSize         uint32
	Commit           uint32
	MinTXID          uint64
	MaxTXID          uint64
	Timestamp        int64
	PreApplyChecksum uint64
	WALOffset        int64
	WALSize          int64
	WALSalt1         uint32
	WALSalt2         uint32
	NodeID           uint64
}

type pageIndexEntry struct {
	pageNo uint32
	offset uint64
	size   uint64
}

// Page is one changed SQLite page in a non-snapshot LTX transaction.
type Page struct {
	PageNumber uint32
	Data       []byte
}

// DatabaseState tracks the page checksums required to validate an LTX chain.
type DatabaseState struct {
	PageSize      uint32
	Commit        uint32
	TXID          uint64
	Checksum      uint64
	pageChecksums map[uint32]uint64
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func lockPageNo(pageSize uint32) uint32 {
	return uint32(LockByte/int64(pageSize)) + 1
}

func validPageSize(pageSize uint32) bool {
	if pageSize < 512 || pageSize > 65536 {
		return false
	}
	return pageSize&(pageSize-1) == 0
}

func (h Header) validate() error {
	if h.Flags != 0 || !validPageSize(h.PageSize) || h.MinTXID == 0 || h.MaxTXID < h.MinTXID {
		return ErrInvalidFile
	}
	if h.MinTXID == 1 {
		if h.Commit == 0 || h.PreApplyChecksum != 0 {
			return ErrInvalidFile
		}
	} else if h.PreApplyChecksum == 0 || h.PreApplyChecksum&ChecksumFlag == 0 {
		return ErrInvalidFile
	}
	if h.WALOffset < 0 || h.WALSize < 0 {
		return ErrInvalidFile
	}
	if h.WALOffset == 0 && h.WALSize != 0 {
		return ErrInvalidFile
	}
	if (h.WALSalt1 != 0 || h.WALSalt2 != 0) && h.WALOffset == 0 {
		return ErrInvalidFile
	}
	return nil
}

func (h Header) marshal() []byte {
	b := make([]byte, HeaderSize)
	copy(b[:4], Magic)
	binary.BigEndian.PutUint32(b[4:8], h.Flags)
	binary.BigEndian.PutUint32(b[8:12], h.PageSize)
	binary.BigEndian.PutUint32(b[12:16], h.Commit)
	binary.BigEndian.PutUint64(b[16:24], h.MinTXID)
	binary.BigEndian.PutUint64(b[24:32], h.MaxTXID)
	binary.BigEndian.PutUint64(b[32:40], uint64(h.Timestamp))
	binary.BigEndian.PutUint64(b[40:48], h.PreApplyChecksum)
	binary.BigEndian.PutUint64(b[48:56], uint64(h.WALOffset))
	binary.BigEndian.PutUint64(b[56:64], uint64(h.WALSize))
	binary.BigEndian.PutUint32(b[64:68], h.WALSalt1)
	binary.BigEndian.PutUint32(b[68:72], h.WALSalt2)
	binary.BigEndian.PutUint64(b[72:80], h.NodeID)
	return b
}

func unmarshalHeader(b []byte) (Header, error) {
	if len(b) != HeaderSize || string(b[:4]) != Magic {
		return Header{}, ErrInvalidFile
	}
	for _, reserved := range b[80:] {
		if reserved != 0 {
			return Header{}, fmt.Errorf("%w: nonzero reserved header bytes", ErrInvalidFile)
		}
	}
	h := Header{
		Flags:            binary.BigEndian.Uint32(b[4:8]),
		PageSize:         binary.BigEndian.Uint32(b[8:12]),
		Commit:           binary.BigEndian.Uint32(b[12:16]),
		MinTXID:          binary.BigEndian.Uint64(b[16:24]),
		MaxTXID:          binary.BigEndian.Uint64(b[24:32]),
		Timestamp:        int64(binary.BigEndian.Uint64(b[32:40])),
		PreApplyChecksum: binary.BigEndian.Uint64(b[40:48]),
		WALOffset:        int64(binary.BigEndian.Uint64(b[48:56])),
		WALSize:          int64(binary.BigEndian.Uint64(b[56:64])),
		WALSalt1:         binary.BigEndian.Uint32(b[64:68]),
		WALSalt2:         binary.BigEndian.Uint32(b[68:72]),
		NodeID:           binary.BigEndian.Uint64(b[72:80]),
	}
	if err := h.validate(); err != nil {
		return Header{}, err
	}
	return h, nil
}

// EncodeSnapshot encodes a SQLite database file as a full LTX v2 snapshot.
func EncodeSnapshot(src io.ReaderAt, fileSize int64, dst io.Writer, timestampMillis int64) (Header, error) {
	return EncodeSnapshotAt(src, fileSize, dst, 1, timestampMillis)
}

// EncodeSnapshotAt encodes a full snapshot representing the database at txid.
// The LTX snapshot range begins at TXID 1 and ends inclusively at txid.
func EncodeSnapshotAt(src io.ReaderAt, fileSize int64, dst io.Writer, txid uint64, timestampMillis int64) (Header, error) {
	if txid == 0 {
		return Header{}, fmt.Errorf("snapshot TXID must be positive")
	}
	if fileSize < 100 || fileSize > int64(^uint64(0)>>1) {
		return Header{}, fmt.Errorf("invalid SQLite file size %d", fileSize)
	}
	var dbHeader [100]byte
	if _, err := src.ReadAt(dbHeader[:], 0); err != nil {
		return Header{}, fmt.Errorf("read SQLite header: %w", err)
	}
	if string(dbHeader[:16]) != "SQLite format 3\x00" {
		return Header{}, fmt.Errorf("invalid SQLite database header")
	}
	pageSize := uint32(binary.BigEndian.Uint16(dbHeader[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	if !validPageSize(pageSize) || fileSize%int64(pageSize) != 0 {
		return Header{}, fmt.Errorf("invalid SQLite page size or file length")
	}
	pageCount := uint64(fileSize / int64(pageSize))
	if pageCount == 0 || pageCount > uint64(^uint32(0)) {
		return Header{}, fmt.Errorf("SQLite page count out of range")
	}
	dbPageCount := binary.BigEndian.Uint32(dbHeader[28:32])
	if dbPageCount != 0 && uint64(dbPageCount) != pageCount {
		return Header{}, fmt.Errorf("SQLite header page count %d does not match file page count %d", dbPageCount, pageCount)
	}

	hdr := Header{PageSize: pageSize, Commit: uint32(pageCount), MinTXID: 1, MaxTXID: txid, Timestamp: timestampMillis}
	if err := hdr.validate(); err != nil {
		return Header{}, err
	}
	hash := crc64.New(crcTable)
	writeHashed := func(p []byte) error {
		if err := writeFull(dst, p); err != nil {
			return err
		}
		_, _ = hash.Write(p)
		return nil
	}
	if err := writeHashed(hdr.marshal()); err != nil {
		return Header{}, fmt.Errorf("write LTX header: %w", err)
	}

	page := make([]byte, pageSize)
	index := make([]pageIndexEntry, 0, pageCount)
	var rolling uint64
	for pgno := uint32(1); pgno <= hdr.Commit; pgno++ {
		if pgno == lockPageNo(pageSize) {
			continue
		}
		if _, err := src.ReadAt(page, int64(pgno-1)*int64(pageSize)); err != nil {
			return Header{}, fmt.Errorf("read SQLite page %d: %w", pgno, err)
		}
		offset := uint64(HeaderSize)
		if len(index) > 0 {
			last := index[len(index)-1]
			offset = last.offset + last.size
		}
		pageHeader := make([]byte, PageHdrSize)
		binary.BigEndian.PutUint32(pageHeader, pgno)
		if err := writeHashed(pageHeader); err != nil {
			return Header{}, fmt.Errorf("write page %d header: %w", pgno, err)
		}
		var compressed bytes.Buffer
		lz4Writer := lz4.NewWriter(&compressed)
		if _, err := lz4Writer.Write(page); err != nil {
			return Header{}, fmt.Errorf("compress SQLite page %d: %w", pgno, err)
		}
		if err := lz4Writer.Close(); err != nil {
			return Header{}, fmt.Errorf("finish compressed page %d: %w", pgno, err)
		}
		if err := writeFull(dst, compressed.Bytes()); err != nil {
			return Header{}, fmt.Errorf("write compressed page %d: %w", pgno, err)
		}
		_, _ = hash.Write(page)
		index = append(index, pageIndexEntry{pageNo: pgno, offset: offset, size: uint64(PageHdrSize + compressed.Len())})
		rolling ^= pageChecksum(pgno, page)
	}

	var terminator [PageHdrSize]byte
	if err := writeHashed(terminator[:]); err != nil {
		return Header{}, fmt.Errorf("write page terminator: %w", err)
	}
	indexBytes := make([]byte, 0, len(index)*24+9)
	for _, entry := range index {
		indexBytes = appendUvarint(indexBytes, uint64(entry.pageNo))
		indexBytes = appendUvarint(indexBytes, entry.offset)
		indexBytes = appendUvarint(indexBytes, entry.size)
	}
	indexBytes = appendUvarint(indexBytes, 0)
	indexBytes = binary.BigEndian.AppendUint64(indexBytes, uint64(len(indexBytes)))
	if err := writeHashed(indexBytes); err != nil {
		return Header{}, fmt.Errorf("write page index: %w", err)
	}
	postApplyChecksum := ChecksumFlag | (rolling &^ ChecksumFlag)
	var trailer [TrailerSize]byte
	binary.BigEndian.PutUint64(trailer[:8], postApplyChecksum)
	_, _ = hash.Write(trailer[:8])
	binary.BigEndian.PutUint64(trailer[8:], ChecksumFlag|hash.Sum64())
	if err := writeFull(dst, trailer[:]); err != nil {
		return Header{}, fmt.Errorf("write LTX trailer: %w", err)
	}
	return hdr, nil
}

// LoadDatabaseState computes the LTX checksum state for a verified SQLite image.
func LoadDatabaseState(src io.ReaderAt, fileSize int64, txid uint64) (DatabaseState, error) {
	if fileSize < 100 || txid == 0 {
		return DatabaseState{}, fmt.Errorf("invalid database image size or TXID")
	}
	var databaseHeader [100]byte
	if _, err := src.ReadAt(databaseHeader[:], 0); err != nil {
		return DatabaseState{}, fmt.Errorf("read SQLite database header: %w", err)
	}
	if string(databaseHeader[:16]) != "SQLite format 3\x00" {
		return DatabaseState{}, fmt.Errorf("invalid SQLite database header")
	}
	pageSize := uint32(binary.BigEndian.Uint16(databaseHeader[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	if !validPageSize(pageSize) || fileSize%int64(pageSize) != 0 {
		return DatabaseState{}, fmt.Errorf("invalid SQLite page size or image length")
	}
	pageCount := uint64(fileSize / int64(pageSize))
	if pageCount == 0 || pageCount > uint64(^uint32(0)) {
		return DatabaseState{}, fmt.Errorf("SQLite page count out of range")
	}
	headerPageCount := binary.BigEndian.Uint32(databaseHeader[28:32])
	if headerPageCount != 0 && uint64(headerPageCount) != pageCount {
		return DatabaseState{}, fmt.Errorf("SQLite header and image page counts differ")
	}

	state := DatabaseState{
		PageSize:      pageSize,
		Commit:        uint32(pageCount),
		TXID:          txid,
		Checksum:      ChecksumFlag,
		pageChecksums: make(map[uint32]uint64, pageCount),
	}
	page := make([]byte, pageSize)
	lockPage := lockPageNo(pageSize)
	for index := uint64(0); index < pageCount; index++ {
		pageNumber := uint32(index + 1)
		if pageNumber == lockPage {
			continue
		}
		offset := int64(index * uint64(pageSize))
		if _, err := src.ReadAt(page, offset); err != nil {
			return DatabaseState{}, fmt.Errorf("read SQLite page %d: %w", pageNumber, err)
		}
		checksum := pageChecksum(pageNumber, page)
		state.pageChecksums[pageNumber] = checksum
		state.Checksum ^= checksum
	}
	state.Checksum = ChecksumFlag | (state.Checksum &^ ChecksumFlag)
	return state, nil
}

// ApplyPages returns the resulting checksum state without mutating the receiver.
func (state DatabaseState) ApplyPages(header Header, pages []Page) (DatabaseState, error) {
	if err := header.validate(); err != nil {
		return DatabaseState{}, err
	}
	if state.TXID == ^uint64(0) || header.MinTXID == 1 || header.MinTXID != state.TXID+1 || header.PreApplyChecksum != state.Checksum {
		return DatabaseState{}, fmt.Errorf("LTX delta does not continue the current database position")
	}
	if header.PageSize != state.PageSize {
		return DatabaseState{}, fmt.Errorf("LTX delta page size differs from database state")
	}
	if err := validateDeltaPages(header, pages); err != nil {
		return DatabaseState{}, err
	}

	checksums := make(map[uint32]uint64, len(state.pageChecksums)+len(pages))
	for pageNumber, checksum := range state.pageChecksums {
		checksums[pageNumber] = checksum
	}
	rolling := state.Checksum &^ ChecksumFlag
	for pageNumber, checksum := range checksums {
		if pageNumber > header.Commit {
			rolling ^= checksum
			delete(checksums, pageNumber)
		}
	}
	lockPage := lockPageNo(header.PageSize)
	for _, page := range pages {
		previousChecksum, exists := checksums[page.PageNumber]
		if !exists && page.PageNumber <= state.Commit && page.PageNumber != lockPage {
			return DatabaseState{}, fmt.Errorf("database state is missing existing page %d", page.PageNumber)
		}
		if exists {
			rolling ^= previousChecksum
		}
		checksum := pageChecksum(page.PageNumber, page.Data)
		rolling ^= checksum
		checksums[page.PageNumber] = checksum
	}

	zeroPage := make([]byte, header.PageSize)
	for pageNumber := uint64(state.Commit) + 1; pageNumber <= uint64(header.Commit); pageNumber++ {
		pageNo := uint32(pageNumber)
		if pageNo == lockPage {
			continue
		}
		if _, exists := checksums[pageNo]; exists {
			continue
		}
		checksum := pageChecksum(pageNo, zeroPage)
		checksums[pageNo] = checksum
		rolling ^= checksum
	}
	postApplyChecksum := ChecksumFlag | (rolling &^ ChecksumFlag)
	return DatabaseState{
		PageSize:      state.PageSize,
		Commit:        header.Commit,
		TXID:          header.MaxTXID,
		Checksum:      postApplyChecksum,
		pageChecksums: checksums,
	}, nil
}

// ApplyDelta validates the pre/post checksums and returns the resulting state.
func (state DatabaseState) ApplyDelta(header Header, pages []Page, postApplyChecksum uint64) (DatabaseState, error) {
	updated, err := state.ApplyPages(header, pages)
	if err != nil {
		return DatabaseState{}, err
	}
	if updated.Checksum != postApplyChecksum {
		return DatabaseState{}, fmt.Errorf("LTX post-apply checksum mismatch")
	}
	return updated, nil
}

// DeltaToDatabaseImage creates a delta that reconciles state with a consistent
// SQLite image. It is used after a WAL generation was reset or recovery needs
// to account for commits that were checkpointed before they reached the spool.
func (state DatabaseState) DeltaToDatabaseImage(src io.ReaderAt, fileSize int64, timestampMillis int64) (*Header, []Page, DatabaseState, error) {
	if state.TXID == ^uint64(0) {
		return nil, nil, DatabaseState{}, fmt.Errorf("LTX transaction ID exhausted")
	}
	target, err := LoadDatabaseState(src, fileSize, state.TXID+1)
	if err != nil {
		return nil, nil, DatabaseState{}, err
	}
	if target.PageSize != state.PageSize {
		return nil, nil, DatabaseState{}, fmt.Errorf("SQLite image page size differs from staged database")
	}
	if target.Commit == state.Commit && target.Checksum == state.Checksum {
		return nil, nil, state, nil
	}

	pages := make([]Page, 0)
	pageData := make([]byte, state.PageSize)
	lockPage := lockPageNo(state.PageSize)
	for pageIndex := uint64(1); pageIndex <= uint64(target.Commit); pageIndex++ {
		pageNumber := uint32(pageIndex)
		if pageNumber == lockPage {
			continue
		}
		targetChecksum, exists := target.pageChecksums[pageNumber]
		if !exists {
			return nil, nil, DatabaseState{}, fmt.Errorf("target image is missing page checksum %d", pageNumber)
		}
		if previousChecksum, exists := state.pageChecksums[pageNumber]; exists && previousChecksum == targetChecksum {
			continue
		}
		if _, err := src.ReadAt(pageData, int64(pageIndex-1)*int64(state.PageSize)); err != nil {
			return nil, nil, DatabaseState{}, fmt.Errorf("read reconciled SQLite page %d: %w", pageNumber, err)
		}
		pages = append(pages, Page{PageNumber: pageNumber, Data: append([]byte(nil), pageData...)})
	}
	header := Header{
		PageSize:         state.PageSize,
		Commit:           target.Commit,
		MinTXID:          state.TXID + 1,
		MaxTXID:          state.TXID + 1,
		Timestamp:        timestampMillis,
		PreApplyChecksum: state.Checksum,
	}
	updated, err := state.ApplyDelta(header, pages, target.Checksum)
	if err != nil {
		return nil, nil, DatabaseState{}, fmt.Errorf("verify reconciled database delta: %w", err)
	}
	return &header, pages, updated, nil
}

// EncodeDelta encodes sorted changed pages as one non-snapshot LTX v2 file.
// The caller supplies the database checksum after applying these pages.
func EncodeDelta(header Header, pages []Page, postApplyChecksum uint64, dst io.Writer) error {
	if err := header.validate(); err != nil {
		return err
	}
	if header.MinTXID == 1 {
		return fmt.Errorf("delta LTX must not be a snapshot")
	}
	if postApplyChecksum&ChecksumFlag == 0 {
		return fmt.Errorf("post-apply checksum must include the checksum flag")
	}
	if err := validateDeltaPages(header, pages); err != nil {
		return err
	}

	hash := crc64.New(crcTable)
	writeHashed := func(data []byte) error {
		if err := writeFull(dst, data); err != nil {
			return err
		}
		_, _ = hash.Write(data)
		return nil
	}
	if err := writeHashed(header.marshal()); err != nil {
		return fmt.Errorf("write LTX delta header: %w", err)
	}

	index := make([]pageIndexEntry, 0, len(pages))
	var offset uint64 = HeaderSize
	for _, page := range pages {
		pageHeader := make([]byte, PageHdrSize)
		binary.BigEndian.PutUint32(pageHeader, page.PageNumber)
		if err := writeHashed(pageHeader); err != nil {
			return fmt.Errorf("write page %d header: %w", page.PageNumber, err)
		}
		var compressed bytes.Buffer
		lz4Writer := lz4.NewWriter(&compressed)
		if _, err := lz4Writer.Write(page.Data); err != nil {
			return fmt.Errorf("compress page %d: %w", page.PageNumber, err)
		}
		if err := lz4Writer.Close(); err != nil {
			return fmt.Errorf("finish compressed page %d: %w", page.PageNumber, err)
		}
		if err := writeFull(dst, compressed.Bytes()); err != nil {
			return fmt.Errorf("write compressed page %d: %w", page.PageNumber, err)
		}
		_, _ = hash.Write(page.Data)
		index = append(index, pageIndexEntry{
			pageNo: page.PageNumber,
			offset: offset,
			size:   uint64(PageHdrSize + compressed.Len()),
		})
		offset += uint64(PageHdrSize + compressed.Len())
	}

	if err := writeHashed(make([]byte, PageHdrSize)); err != nil {
		return fmt.Errorf("write delta page terminator: %w", err)
	}
	indexBytes := make([]byte, 0, len(index)*24+9)
	for _, entry := range index {
		indexBytes = appendUvarint(indexBytes, uint64(entry.pageNo))
		indexBytes = appendUvarint(indexBytes, entry.offset)
		indexBytes = appendUvarint(indexBytes, entry.size)
	}
	indexBytes = appendUvarint(indexBytes, 0)
	indexBytes = binary.BigEndian.AppendUint64(indexBytes, uint64(len(indexBytes)))
	if err := writeHashed(indexBytes); err != nil {
		return fmt.Errorf("write delta page index: %w", err)
	}

	var trailer [TrailerSize]byte
	binary.BigEndian.PutUint64(trailer[:8], postApplyChecksum)
	_, _ = hash.Write(trailer[:8])
	binary.BigEndian.PutUint64(trailer[8:], ChecksumFlag|hash.Sum64())
	if err := writeFull(dst, trailer[:]); err != nil {
		return fmt.Errorf("write LTX delta trailer: %w", err)
	}
	return nil
}

func validateDeltaPages(header Header, pages []Page) error {
	lockPage := lockPageNo(header.PageSize)
	var previousPage uint32
	for _, page := range pages {
		if page.PageNumber == 0 || page.PageNumber > header.Commit || page.PageNumber == lockPage {
			return fmt.Errorf("invalid delta page number %d for commit %d", page.PageNumber, header.Commit)
		}
		if page.PageNumber <= previousPage {
			return fmt.Errorf("delta pages are not strictly ordered at page %d", page.PageNumber)
		}
		if uint32(len(page.Data)) != header.PageSize {
			return fmt.Errorf("invalid page %d size: got %d, want %d", page.PageNumber, len(page.Data), header.PageSize)
		}
		previousPage = page.PageNumber
	}
	return nil
}

// DecodeSnapshot validates an LTX v2 snapshot and writes its database image to dst.
func DecodeSnapshot(src io.Reader, dst io.Writer) (Header, error) {
	counter := &countingReader{r: src}
	reader := bufio.NewReaderSize(counter, 64*1024)
	hash := crc64.New(crcTable)
	readHashed := func(p []byte) error {
		if _, err := io.ReadFull(reader, p); err != nil {
			return err
		}
		_, _ = hash.Write(p)
		return nil
	}
	var headerBytes [HeaderSize]byte
	if err := readHashed(headerBytes[:]); err != nil {
		return Header{}, fmt.Errorf("read LTX header: %w", err)
	}
	hdr, err := unmarshalHeader(headerBytes[:])
	if err != nil {
		return Header{}, err
	}
	if hdr.MinTXID != 1 {
		return Header{}, fmt.Errorf("cannot decode non-snapshot LTX as a database image")
	}
	lockPgno := lockPageNo(hdr.PageSize)
	page := make([]byte, hdr.PageSize)
	index := make([]pageIndexEntry, 0)
	var rolling uint64
	var expectedPage uint32 = 1
	for {
		frameOffset := logicalOffset(counter, reader)
		var pageHeader [PageHdrSize]byte
		if err := readHashed(pageHeader[:]); err != nil {
			return Header{}, fmt.Errorf("read page header: %w", err)
		}
		pgno := binary.BigEndian.Uint32(pageHeader[:])
		if pgno == 0 {
			break
		}
		if expectedPage == lockPgno && lockPgno <= hdr.Commit {
			if err := writeZeros(dst, int(hdr.PageSize)); err != nil {
				return Header{}, fmt.Errorf("write restored lock page: %w", err)
			}
			expectedPage++
		}
		if pgno > hdr.Commit || pgno == lockPgno || pgno != expectedPage {
			return Header{}, fmt.Errorf("%w: unexpected page number %d, expected %d", ErrInvalidFile, pgno, expectedPage)
		}
		expectedPage = pgno + 1
		lz4Reader := lz4.NewReader(reader)
		if _, err := io.ReadFull(lz4Reader, page); err != nil {
			return Header{}, fmt.Errorf("read LZ4 page %d: %w", pgno, err)
		}
		var extra [1]byte
		if n, err := lz4Reader.Read(extra[:]); n != 0 || err != io.EOF {
			return Header{}, fmt.Errorf("%w: invalid LZ4 page frame %d", ErrInvalidFile, pgno)
		}
		_, _ = hash.Write(page)
		rolling ^= pageChecksum(pgno, page)
		if err := writeFull(dst, page); err != nil {
			return Header{}, fmt.Errorf("write restored page %d: %w", pgno, err)
		}
		frameEnd := logicalOffset(counter, reader)
		index = append(index, pageIndexEntry{pageNo: pgno, offset: uint64(frameOffset), size: uint64(frameEnd - frameOffset)})
	}
	if expectedPage == lockPgno && lockPgno <= hdr.Commit {
		if err := writeZeros(dst, int(hdr.PageSize)); err != nil {
			return Header{}, fmt.Errorf("write restored lock page: %w", err)
		}
		expectedPage++
	}
	if expectedPage != hdr.Commit+1 {
		return Header{}, fmt.Errorf("%w: snapshot ends at page %d, commit is %d", ErrInvalidFile, expectedPage-1, hdr.Commit)
	}
	if err := readAndValidateIndex(reader, hash, index); err != nil {
		return Header{}, err
	}
	var trailer [TrailerSize]byte
	if _, err := io.ReadFull(reader, trailer[:]); err != nil {
		return Header{}, fmt.Errorf("read LTX trailer: %w", err)
	}
	postChecksum := binary.BigEndian.Uint64(trailer[:8])
	fileChecksum := binary.BigEndian.Uint64(trailer[8:])
	if postChecksum != ChecksumFlag|(rolling&^ChecksumFlag) || postChecksum&ChecksumFlag == 0 {
		return Header{}, ErrChecksumMismatch
	}
	_, _ = hash.Write(trailer[:8])
	if fileChecksum != ChecksumFlag|hash.Sum64() || fileChecksum&ChecksumFlag == 0 {
		return Header{}, ErrChecksumMismatch
	}
	if _, err := reader.ReadByte(); err != io.EOF {
		if err == nil {
			return Header{}, fmt.Errorf("%w: trailing bytes", ErrInvalidFile)
		}
		return Header{}, fmt.Errorf("read trailing data: %w", err)
	}
	return hdr, nil
}

// DecodeDelta validates and decodes one non-snapshot LTX v2 file.
func DecodeDelta(src io.Reader) (Header, []Page, uint64, error) {
	counter := &countingReader{r: src}
	reader := bufio.NewReaderSize(counter, 64*1024)
	hash := crc64.New(crcTable)
	readHashed := func(data []byte) error {
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}
		_, _ = hash.Write(data)
		return nil
	}

	var headerBytes [HeaderSize]byte
	if err := readHashed(headerBytes[:]); err != nil {
		return Header{}, nil, 0, fmt.Errorf("read LTX delta header: %w", err)
	}
	header, err := unmarshalHeader(headerBytes[:])
	if err != nil {
		return Header{}, nil, 0, err
	}
	if header.MinTXID == 1 {
		return Header{}, nil, 0, fmt.Errorf("LTX file is a snapshot, not a delta")
	}

	pages := make([]Page, 0)
	index := make([]pageIndexEntry, 0)
	pageData := make([]byte, header.PageSize)
	lockPage := lockPageNo(header.PageSize)
	var previousPage uint32
	for {
		frameOffset := logicalOffset(counter, reader)
		var pageHeader [PageHdrSize]byte
		if err := readHashed(pageHeader[:]); err != nil {
			return Header{}, nil, 0, fmt.Errorf("read LTX delta page header: %w", err)
		}
		pageNumber := binary.BigEndian.Uint32(pageHeader[:])
		if pageNumber == 0 {
			break
		}
		if pageNumber > header.Commit || pageNumber == lockPage || pageNumber <= previousPage {
			return Header{}, nil, 0, fmt.Errorf("%w: invalid or out-of-order delta page %d", ErrInvalidFile, pageNumber)
		}
		previousPage = pageNumber

		lz4Reader := lz4.NewReader(reader)
		if _, err := io.ReadFull(lz4Reader, pageData); err != nil {
			return Header{}, nil, 0, fmt.Errorf("read compressed LTX delta page %d: %w", pageNumber, err)
		}
		var extra [1]byte
		if n, err := lz4Reader.Read(extra[:]); n != 0 || err != io.EOF {
			return Header{}, nil, 0, fmt.Errorf("%w: invalid compressed LTX delta page %d", ErrInvalidFile, pageNumber)
		}
		_, _ = hash.Write(pageData)
		pages = append(pages, Page{PageNumber: pageNumber, Data: append([]byte(nil), pageData...)})
		frameEnd := logicalOffset(counter, reader)
		index = append(index, pageIndexEntry{
			pageNo: pageNumber,
			offset: uint64(frameOffset),
			size:   uint64(frameEnd - frameOffset),
		})
	}

	if err := readAndValidateIndex(reader, hash, index); err != nil {
		return Header{}, nil, 0, err
	}
	var trailer [TrailerSize]byte
	if _, err := io.ReadFull(reader, trailer[:]); err != nil {
		return Header{}, nil, 0, fmt.Errorf("read LTX delta trailer: %w", err)
	}
	postApplyChecksum := binary.BigEndian.Uint64(trailer[:8])
	fileChecksum := binary.BigEndian.Uint64(trailer[8:])
	if postApplyChecksum&ChecksumFlag == 0 || fileChecksum&ChecksumFlag == 0 {
		return Header{}, nil, 0, ErrChecksumMismatch
	}
	_, _ = hash.Write(trailer[:8])
	if fileChecksum != ChecksumFlag|hash.Sum64() {
		return Header{}, nil, 0, ErrChecksumMismatch
	}
	if _, err := reader.ReadByte(); err != io.EOF {
		if err == nil {
			return Header{}, nil, 0, fmt.Errorf("%w: trailing bytes", ErrInvalidFile)
		}
		return Header{}, nil, 0, fmt.Errorf("read trailing data: %w", err)
	}
	return header, pages, postApplyChecksum, nil
}

func readAndValidateIndex(reader *bufio.Reader, hash io.Writer, actual []pageIndexEntry) error {
	var encodedSize uint64
	for i := 0; ; i++ {
		pageNo, n, err := readUvarintHashed(reader, hash)
		if err != nil {
			return fmt.Errorf("read page index page number: %w", err)
		}
		encodedSize += uint64(n)
		if pageNo == 0 {
			if i != len(actual) {
				return fmt.Errorf("%w: page index count mismatch", ErrInvalidFile)
			}
			break
		}
		offset, n, err := readUvarintHashed(reader, hash)
		if err != nil {
			return fmt.Errorf("read page index offset: %w", err)
		}
		encodedSize += uint64(n)
		size, n, err := readUvarintHashed(reader, hash)
		if err != nil {
			return fmt.Errorf("read page index size: %w", err)
		}
		encodedSize += uint64(n)
		if i >= len(actual) || uint64(actual[i].pageNo) != pageNo || actual[i].offset != offset || actual[i].size != size {
			return fmt.Errorf("%w: page index entry mismatch", ErrInvalidFile)
		}
	}
	var sizeBytes [8]byte
	if _, err := io.ReadFull(reader, sizeBytes[:]); err != nil {
		return fmt.Errorf("read page index length: %w", err)
	}
	_, _ = hash.Write(sizeBytes[:])
	if binary.BigEndian.Uint64(sizeBytes[:]) != encodedSize {
		return fmt.Errorf("%w: invalid page index length", ErrInvalidFile)
	}
	return nil
}

func readUvarintHashed(reader io.ByteReader, hash io.Writer) (uint64, int, error) {
	var value uint64
	for i := 0; i < binary.MaxVarintLen64; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, i, err
		}
		if _, err := hash.Write([]byte{b}); err != nil {
			return 0, i + 1, err
		}
		if i == binary.MaxVarintLen64-1 && b > 1 {
			return 0, i + 1, errors.New("unsigned varint overflow")
		}
		value |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return value, i + 1, nil
		}
	}
	return 0, binary.MaxVarintLen64, errors.New("unsigned varint too long")
}

func appendUvarint(dst []byte, value uint64) []byte {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], value)
	return append(dst, b[:n]...)
}

func logicalOffset(counter *countingReader, reader *bufio.Reader) int64 {
	return counter.n - int64(reader.Buffered())
}

func pageChecksum(pgno uint32, page []byte) uint64 {
	var pageNo [4]byte
	binary.BigEndian.PutUint32(pageNo[:], pgno)
	h := crc64.New(crcTable)
	_, _ = h.Write(pageNo[:])
	_, _ = h.Write(page)
	return h.Sum64() &^ ChecksumFlag
}

func writeFull(dst io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := dst.Write(b)
		if n > 0 {
			b = b[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func writeZeros(dst io.Writer, length int) error {
	zeros := make([]byte, length)
	return writeFull(dst, zeros)
}
