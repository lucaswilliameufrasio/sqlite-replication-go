package spool

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
)

const remoteAckVersion = 1

// RemoteAck records the highest LTX chain position fully confirmed for a
// caller-defined remote identity and prefix.
type RemoteAck struct {
	Version          int    `json:"version"`
	RemoteID         string `json:"remote_id"`
	Prefix           string `json:"prefix"`
	TXID             uint64 `json:"txid"`
	DatabaseChecksum uint64 `json:"database_checksum"`
}

func ReadRemoteAck(root string) (RemoteAck, error) {
	path := filepath.Join(root, "_meta", "remote-ack.json")
	file, err := os.Open(path)
	if err != nil {
		return RemoteAck{}, fmt.Errorf("open remote acknowledgement: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var ack RemoteAck
	if err := decoder.Decode(&ack); err != nil {
		return RemoteAck{}, fmt.Errorf("decode remote acknowledgement: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return RemoteAck{}, fmt.Errorf("remote acknowledgement contains trailing data")
	}
	if err := validateRemoteAck(ack); err != nil {
		return RemoteAck{}, err
	}
	return ack, nil
}

func WriteRemoteAck(root string, ack RemoteAck) error {
	if err := validateRemoteAck(ack); err != nil {
		return err
	}
	metadataDirectory := filepath.Join(root, "_meta")
	if err := os.MkdirAll(metadataDirectory, 0o750); err != nil {
		return fmt.Errorf("create metadata directory for remote acknowledgement: %w", err)
	}
	staged, err := os.CreateTemp(metadataDirectory, ".remote-ack-*.tmp")
	if err != nil {
		return fmt.Errorf("stage remote acknowledgement: %w", err)
	}
	stagedPath := staged.Name()
	defer os.Remove(stagedPath)
	encoder := json.NewEncoder(staged)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(ack); err != nil {
		_ = staged.Close()
		return fmt.Errorf("encode remote acknowledgement: %w", err)
	}
	if err := staged.Sync(); err != nil {
		_ = staged.Close()
		return fmt.Errorf("sync remote acknowledgement: %w", err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("close remote acknowledgement: %w", err)
	}
	if err := os.Rename(stagedPath, filepath.Join(metadataDirectory, "remote-ack.json")); err != nil {
		return fmt.Errorf("publish remote acknowledgement: %w", err)
	}
	if err := storagefs.SyncDirectory(metadataDirectory); err != nil {
		return fmt.Errorf("sync remote acknowledgement directory: %w", err)
	}
	return nil
}

func validateRemoteAck(ack RemoteAck) error {
	if ack.Version != remoteAckVersion || strings.TrimSpace(ack.RemoteID) == "" || ack.TXID == 0 || ack.DatabaseChecksum == 0 {
		return fmt.Errorf("remote acknowledgement is incompatible")
	}
	for _, character := range ack.RemoteID {
		if unicode.IsControl(character) {
			return fmt.Errorf("remote acknowledgement has an invalid remote ID")
		}
	}
	return nil
}
