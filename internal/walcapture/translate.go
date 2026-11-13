package walcapture

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/wal"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
)

type Position struct {
	Initialized        bool   `json:"initialized"`
	CheckpointSequence uint32 `json:"checkpoint_sequence"`
	Salt1              uint32 `json:"salt_1"`
	Salt2              uint32 `json:"salt_2"`
	FrameCount         uint32 `json:"frame_count"`
	Checksum1          uint32 `json:"checksum_1"`
	Checksum2          uint32 `json:"checksum_2"`
}

type State struct {
	Database ltx.DatabaseState
	WAL      Position
}

type Transaction struct {
	StartFrame   uint32
	EndFrame     uint32
	Header       ltx.Header
	Pages        []ltx.Page
	PostChecksum uint64
	Encoded      []byte
}

// TranslateWAL converts complete commits from a stable WAL read into verified LTX deltas.
// The caller must hold the SQLite writer guard and retain the previous read pin.
func TranslateWAL(data []byte, state State, timestampMillis int64) ([]Transaction, State, error) {
	if len(data) == 0 {
		if state.WAL.Initialized && state.WAL.FrameCount > 0 {
			return nil, state, fmt.Errorf("WAL disappeared while its captured position was non-zero")
		}
		return nil, state, nil
	}
	if len(data) < 32 {
		return nil, state, fmt.Errorf("WAL prefix is shorter than its header")
	}

	scan, err := wal.Scan(data)
	if err != nil {
		return nil, state, fmt.Errorf("validate WAL prefix: %w", err)
	}
	frameSize := 24 + int(scan.Header.PageSize)
	if scan.Header.PageSize != state.Database.PageSize {
		return nil, state, fmt.Errorf("WAL page size differs from the staged database")
	}

	position := Position{
		Initialized:        true,
		CheckpointSequence: scan.Header.CheckpointSequence,
		Salt1:              scan.Header.Salt1,
		Salt2:              scan.Header.Salt2,
		FrameCount:         uint32(scan.CommittedFrameCount),
	}
	position.Checksum1 = lastChecksum1(scan)
	position.Checksum2 = lastChecksum2(scan)
	startFrame := 0
	if sameGeneration(state.WAL, position) {
		if state.WAL.FrameCount > position.FrameCount {
			return nil, state, fmt.Errorf("WAL frame count regressed without a generation change")
		}
		startFrame = int(state.WAL.FrameCount)
		if startFrame > 0 && scan.Frames[startFrame-1].DatabaseSize == 0 {
			return nil, state, fmt.Errorf("staged WAL position does not end at a commit frame")
		}
		checksum1, checksum2 := scan.Header.Checksum1, scan.Header.Checksum2
		if startFrame > 0 {
			checksum1 = scan.Frames[startFrame-1].Checksum1
			checksum2 = scan.Frames[startFrame-1].Checksum2
		}
		if checksum1 != state.WAL.Checksum1 || checksum2 != state.WAL.Checksum2 {
			return nil, state, fmt.Errorf("previously staged WAL prefix changed without a generation change")
		}
	}

	transactions := make([]Transaction, 0)
	currentState := state
	changedPages := make(map[uint32][]byte)
	groupStart := startFrame
	for frameIndex := startFrame; frameIndex < scan.CommittedFrameCount; frameIndex++ {
		frame := scan.Frames[frameIndex]
		pageStart := int(frame.PageOffset)
		pageEnd := pageStart + int(scan.Header.PageSize)
		if pageStart < 0 || pageEnd > len(data) {
			return nil, state, fmt.Errorf("WAL page %d falls outside the validated prefix", frame.PageNumber)
		}
		changedPages[frame.PageNumber] = append([]byte(nil), data[pageStart:pageEnd]...)
		if frame.DatabaseSize == 0 {
			continue
		}

		pageNumbers := make([]uint32, 0, len(changedPages))
		lockPage := uint32(ltx.LockByte/int64(scan.Header.PageSize)) + 1
		for pageNumber := range changedPages {
			if pageNumber <= frame.DatabaseSize && pageNumber != lockPage {
				pageNumbers = append(pageNumbers, pageNumber)
			}
		}
		sort.Slice(pageNumbers, func(i, j int) bool {
			return pageNumbers[i] < pageNumbers[j]
		})
		pages := make([]ltx.Page, 0, len(pageNumbers))
		for _, pageNumber := range pageNumbers {
			pages = append(pages, ltx.Page{
				PageNumber: pageNumber,
				Data:       changedPages[pageNumber],
			})
		}

		if currentState.Database.TXID == ^uint64(0) {
			return nil, state, fmt.Errorf("LTX transaction ID exhausted")
		}
		txid := currentState.Database.TXID + 1
		walOffset := int64(32 + groupStart*frameSize)
		walSize := int64(32 + (frameIndex+1)*frameSize)
		header := ltx.Header{
			PageSize:         scan.Header.PageSize,
			Commit:           frame.DatabaseSize,
			MinTXID:          txid,
			MaxTXID:          txid,
			Timestamp:        timestampMillis,
			PreApplyChecksum: currentState.Database.Checksum,
			WALOffset:        walOffset,
			WALSize:          walSize,
			WALSalt1:         scan.Header.Salt1,
			WALSalt2:         scan.Header.Salt2,
		}
		nextDatabase, err := currentState.Database.ApplyPages(header, pages)
		if err != nil {
			return nil, state, fmt.Errorf("calculate LTX commit %d: %w", txid, err)
		}
		var encoded bytes.Buffer
		if err := ltx.EncodeDelta(header, pages, nextDatabase.Checksum, &encoded); err != nil {
			return nil, state, fmt.Errorf("encode LTX commit %d: %w", txid, err)
		}
		decodedHeader, decodedPages, postChecksum, err := ltx.DecodeDelta(bytes.NewReader(encoded.Bytes()))
		if err != nil {
			return nil, state, fmt.Errorf("validate encoded LTX commit %d: %w", txid, err)
		}
		verifiedDatabase, err := currentState.Database.ApplyDelta(decodedHeader, decodedPages, postChecksum)
		if err != nil || verifiedDatabase.Checksum != nextDatabase.Checksum {
			if err == nil {
				err = fmt.Errorf("post-apply database checksum differs after LTX validation")
			}
			return nil, state, fmt.Errorf("verify LTX commit %d: %w", txid, err)
		}
		transactions = append(transactions, Transaction{
			StartFrame:   uint32(groupStart + 1),
			EndFrame:     uint32(frameIndex + 1),
			Header:       header,
			Pages:        pages,
			PostChecksum: postChecksum,
			Encoded:      append([]byte(nil), encoded.Bytes()...),
		})
		currentState.Database = verifiedDatabase
		groupStart = frameIndex + 1
		changedPages = make(map[uint32][]byte)
	}
	if len(changedPages) != 0 {
		return nil, state, fmt.Errorf("WAL prefix ends with an uncommitted transaction")
	}
	currentState.WAL = position
	return transactions, currentState, nil
}

func sameGeneration(previous, current Position) bool {
	return previous.Initialized && current.Initialized &&
		previous.CheckpointSequence == current.CheckpointSequence &&
		previous.Salt1 == current.Salt1 && previous.Salt2 == current.Salt2
}

// PositionFromWAL records the last committed boundary in a stable WAL read.
func PositionFromWAL(data []byte) (Position, error) {
	if len(data) == 0 {
		return Position{}, nil
	}
	scan, err := wal.Scan(data)
	if err != nil {
		return Position{}, fmt.Errorf("validate WAL boundary: %w", err)
	}
	return positionFromScan(scan), nil
}

// PositionFromWALPrefix requires the given bytes to end exactly at a commit frame.
func PositionFromWALPrefix(data []byte, prefixSize int64) (Position, error) {
	if prefixSize < 0 || prefixSize > int64(len(data)) {
		return Position{}, fmt.Errorf("WAL prefix length is outside the supplied bytes")
	}
	if prefixSize == 0 {
		return Position{}, nil
	}
	data = data[:int(prefixSize)]
	scan, err := wal.Scan(data)
	if err != nil {
		return Position{}, fmt.Errorf("validate WAL prefix: %w", err)
	}
	if scan.CommittedFrameCount != len(scan.Frames) || scan.IncompleteTailBytes != 0 || scan.StaleTailBytes != 0 {
		return Position{}, fmt.Errorf("WAL prefix does not end at a complete commit boundary")
	}
	return positionFromScan(scan), nil
}

func positionFromScan(scan wal.Result) Position {
	return Position{
		Initialized:        true,
		CheckpointSequence: scan.Header.CheckpointSequence,
		Salt1:              scan.Header.Salt1,
		Salt2:              scan.Header.Salt2,
		FrameCount:         uint32(scan.CommittedFrameCount),
		Checksum1:          lastChecksum1(scan),
		Checksum2:          lastChecksum2(scan),
	}
}

func lastChecksum1(scan wal.Result) uint32 {
	if scan.CommittedFrameCount == 0 {
		return scan.Header.Checksum1
	}
	return scan.Frames[scan.CommittedFrameCount-1].Checksum1
}

func lastChecksum2(scan wal.Result) uint32 {
	if scan.CommittedFrameCount == 0 {
		return scan.Header.Checksum2
	}
	return scan.Frames[scan.CommittedFrameCount-1].Checksum2
}
