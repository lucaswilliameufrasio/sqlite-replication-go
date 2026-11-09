package spool

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRemoteAckDurableRoundTripAndStrictDecode(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	want := RemoteAck{
		Version:          1,
		RemoteID:         "production-eu-west-replica",
		Prefix:           "tenant/database",
		TXID:             42,
		DatabaseChecksum: 1 << 63,
	}
	if err := WriteRemoteAck(root, want); err != nil {
		t.Fatalf("write remote acknowledgement: %v", err)
	}
	got, err := ReadRemoteAck(root)
	if err != nil {
		t.Fatalf("read remote acknowledgement: %v", err)
	}
	if got != want {
		t.Fatalf("remote acknowledgement = %+v, want %+v", got, want)
	}
	metadataDirectory, err := os.Open(filepath.Join(root, "_meta"))
	if err != nil {
		t.Fatal(err)
	}
	if err := metadataDirectory.Sync(); err != nil {
		t.Fatalf("sync acknowledgement directory: %v", err)
	}
	if err := metadataDirectory.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(root, "_meta", "remote-ack.json")
	for _, contents := range []string{
		`{"version":1,"remote_id":"r","prefix":"p","txid":1,"database_checksum":9223372036854775808,"unexpected":true}`,
		`{"version":1,"remote_id":"r","prefix":"p","txid":1,"database_checksum":9223372036854775808} {}`,
	} {
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadRemoteAck(root); err == nil {
			t.Fatalf("accepted malformed remote acknowledgement %q", contents)
		}
	}
}

func TestWriteRemoteAckRejectsInvalidIdentityAndPosition(t *testing.T) {
	root := t.TempDir()
	for _, ack := range []RemoteAck{
		{Version: 1, RemoteID: "  ", TXID: 1, DatabaseChecksum: 1 << 63},
		{Version: 1, RemoteID: "remote\nname", TXID: 1, DatabaseChecksum: 1 << 63},
		{Version: 1, RemoteID: "remote", TXID: 0, DatabaseChecksum: 1 << 63},
	} {
		if err := WriteRemoteAck(root, ack); err == nil {
			t.Fatalf("accepted invalid remote acknowledgement %+v", ack)
		}
	}
}
