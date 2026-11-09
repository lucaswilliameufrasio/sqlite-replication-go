// Package wal parses the SQLite WAL file format. It does not coordinate SQLite
// readers, writers, or checkpoints and therefore does not establish CP-01.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	headerSize          = 32
	frameHeaderSize     = 24
	walFormatVersion    = 3_007_000
	magicChecksumLittle = 0x377f0682
	magicChecksumBig    = 0x377f0683
	maxPageNumber       = uint32(0xfffffffe)
)

var (
	ErrInvalidHeader    = errors.New("invalid SQLite WAL header")
	ErrInvalidFrame     = errors.New("invalid SQLite WAL frame")
	ErrChecksumMismatch = errors.New("SQLite WAL checksum mismatch")
)

// Header contains the validated fields from a SQLite WAL header.
type Header struct {
	PageSize           uint32
	CheckpointSequence uint32
	Salt1              uint32
	Salt2              uint32
	Checksum1          uint32
	Checksum2          uint32
}

// Frame describes a validated frame. PageOffset points to its page bytes in
// the input passed to Scan; it is not an independently durable WAL position.
type Frame struct {
	PageNumber   uint32
	DatabaseSize uint32
	PageOffset   int64
	Checksum1    uint32
	Checksum2    uint32
}

// Result describes the longest valid frame prefix. CommittedFrameCount and
// DatabaseSize identify the last complete transaction in that prefix.
type Result struct {
	Header              Header
	Frames              []Frame
	CommittedFrameCount int
	DatabaseSize        uint32
	IncompleteTailBytes int
}

// Scan validates one immutable byte slice containing a WAL header and frames.
// A partial trailing frame is reported but not treated as committed. A fully
// present invalid frame fails the entire scan closed.
func Scan(data []byte) (Result, error) {
	if len(data) < headerSize {
		return Result{}, fmt.Errorf("%w: incomplete header", ErrInvalidHeader)
	}

	magic := binary.BigEndian.Uint32(data[:4])
	var checksumOrder binary.ByteOrder
	switch magic {
	case magicChecksumLittle:
		checksumOrder = binary.LittleEndian
	case magicChecksumBig:
		checksumOrder = binary.BigEndian
	default:
		return Result{}, fmt.Errorf("%w: unsupported magic %#08x", ErrInvalidHeader, magic)
	}
	if version := binary.BigEndian.Uint32(data[4:8]); version != walFormatVersion {
		return Result{}, fmt.Errorf("%w: unsupported format version %d", ErrInvalidHeader, version)
	}

	pageSize := binary.BigEndian.Uint32(data[8:12])
	if !validPageSize(pageSize) {
		return Result{}, fmt.Errorf("%w: invalid page size %d", ErrInvalidHeader, pageSize)
	}

	checksum1, checksum2 := walChecksum(data[:24], 0, 0, checksumOrder)
	header := Header{
		PageSize:           pageSize,
		CheckpointSequence: binary.BigEndian.Uint32(data[12:16]),
		Salt1:              binary.BigEndian.Uint32(data[16:20]),
		Salt2:              binary.BigEndian.Uint32(data[20:24]),
		Checksum1:          binary.BigEndian.Uint32(data[24:28]),
		Checksum2:          binary.BigEndian.Uint32(data[28:32]),
	}
	if checksum1 != header.Checksum1 || checksum2 != header.Checksum2 {
		return Result{}, fmt.Errorf("%w: header", ErrChecksumMismatch)
	}

	result := Result{Header: header, Frames: make([]Frame, 0)}
	frameSize := frameHeaderSize + int(pageSize)
	offset := headerSize
	for offset < len(data) {
		remaining := len(data) - offset
		if remaining < frameSize {
			result.IncompleteTailBytes = remaining
			break
		}

		frameHeader := data[offset : offset+frameHeaderSize]
		page := data[offset+frameHeaderSize : offset+frameSize]
		pageNumber := binary.BigEndian.Uint32(frameHeader[:4])
		databaseSize := binary.BigEndian.Uint32(frameHeader[4:8])
		salt1 := binary.BigEndian.Uint32(frameHeader[8:12])
		salt2 := binary.BigEndian.Uint32(frameHeader[12:16])
		if pageNumber == 0 || pageNumber > maxPageNumber ||
			(databaseSize != 0 && databaseSize > maxPageNumber) ||
			salt1 != header.Salt1 || salt2 != header.Salt2 {
			return Result{}, fmt.Errorf("%w at offset %d: page number or salt", ErrInvalidFrame, offset)
		}

		checksum1, checksum2 = walChecksum(frameHeader[:8], checksum1, checksum2, checksumOrder)
		checksum1, checksum2 = walChecksum(page, checksum1, checksum2, checksumOrder)
		if checksum1 != binary.BigEndian.Uint32(frameHeader[16:20]) ||
			checksum2 != binary.BigEndian.Uint32(frameHeader[20:24]) {
			return Result{}, fmt.Errorf("%w at offset %d: %w", ErrInvalidFrame, offset, ErrChecksumMismatch)
		}

		result.Frames = append(result.Frames, Frame{
			PageNumber:   pageNumber,
			DatabaseSize: databaseSize,
			PageOffset:   int64(offset + frameHeaderSize),
			Checksum1:    checksum1,
			Checksum2:    checksum2,
		})
		if databaseSize != 0 {
			result.CommittedFrameCount = len(result.Frames)
			result.DatabaseSize = databaseSize
		}
		checksum1 = binary.BigEndian.Uint32(frameHeader[16:20])
		checksum2 = binary.BigEndian.Uint32(frameHeader[20:24])
		offset += frameSize
	}

	return result, nil
}

func validPageSize(pageSize uint32) bool {
	if pageSize < 512 || pageSize > 65536 {
		return false
	}
	return pageSize&(pageSize-1) == 0
}

func walChecksum(data []byte, checksum1, checksum2 uint32, order binary.ByteOrder) (uint32, uint32) {
	for offset := 0; offset < len(data); offset += 8 {
		checksum1 += order.Uint32(data[offset:offset+4]) + checksum2
		checksum2 += order.Uint32(data[offset+4:offset+8]) + checksum1
	}
	return checksum1, checksum2
}
