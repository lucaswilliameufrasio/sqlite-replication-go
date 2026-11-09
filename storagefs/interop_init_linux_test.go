//go:build linux

package storagefs

import (
	"bufio"
	"fmt"
	"os"
	"testing"
)

func TestInitializeForRustInterop(t *testing.T) {
	root := os.Getenv("SQLITE_REPLICATION_INIT_INTEROP_ROOT")
	if root == "" {
		t.Skip("cross-process initialization helper")
	}
	if _, err := fmt.Fprintln(os.Stdout, "READY"); err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	if err := EnsureRepository(root); err != nil {
		t.Fatal(err)
	}
	repository, err := ReadRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(os.Stdout, "IDENTITY %s %s\n", repository.RepositoryID, repository.DatabaseID); err != nil {
		t.Fatal(err)
	}
}
