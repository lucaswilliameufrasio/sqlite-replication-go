package storagefs

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestReadRepositoryRejectsInvalidIdentity(t *testing.T) {
	for _, identity := range []string{
		"00000000-0000-0000-0000-000000000000",
		"550e8400-e29b-11d4-a716-446655440000",
		"550e8400-e29b-41d4-0716-446655440000",
		"550e8400e29b41d4a716446655440000",
		"urn:uuid:550e8400-e29b-41d4-a716-446655440000",
		"{550e8400-e29b-41d4-a716-446655440000}",
	} {
		for _, field := range []string{"repository_id", "database_id"} {
			t.Run(field+"/"+identity, func(t *testing.T) {
				root := t.TempDir()
				if err := EnsureRepository(root); err != nil {
					t.Fatal(err)
				}
				repositoryID, databaseID := "550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440001"
				if field == "repository_id" {
					repositoryID = identity
				} else {
					databaseID = identity
				}
				manifest := fmt.Sprintf(`{"schema_version":1,"format_version":2,"repository_id":%q,"database_id":%q}`, repositoryID, databaseID)
				if err := os.WriteFile(filepath.Join(root, "_meta", "repository.json"), []byte(manifest), 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := ReadRepository(root); err == nil {
					t.Fatal("accepted an invalid repository identity")
				}
				if err := EnsureRepository(root); err == nil {
					t.Fatal("reinitialization accepted invalid repository identity")
				}
				content, err := os.ReadFile(filepath.Join(root, "_meta", "repository.json"))
				if err != nil || string(content) != manifest {
					t.Fatalf("invalid metadata was modified: %v", err)
				}
			})
		}
	}
}

func TestReadRepositoryAcceptsUppercaseIdentity(t *testing.T) {
	root := t.TempDir()
	if err := EnsureRepository(root); err != nil {
		t.Fatal(err)
	}
	manifest := `{"schema_version":1,"format_version":2,"repository_id":"550E8400-E29B-41D4-A716-446655440000","database_id":"550E8400-E29B-41D4-A716-446655440001"}`
	if err := os.WriteFile(filepath.Join(root, "_meta", "repository.json"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRepository(root); err != nil {
		t.Fatalf("valid uppercase identities rejected: %v", err)
	}
}

func TestEnsureRepositoryIsVersionedAndStable(t *testing.T) {
	root := t.TempDir()
	if err := EnsureRepository(root); err != nil {
		t.Fatalf("ensure repository: %v", err)
	}
	first, err := ReadRepository(root)
	if err != nil {
		t.Fatalf("read repository: %v", err)
	}
	if first.FormatVersion != FormatVersion || first.RepositoryID == "" || first.DatabaseID == "" {
		t.Fatalf("invalid repository metadata: %+v", first)
	}
	if err := EnsureRepository(root); err != nil {
		t.Fatalf("ensure repository again: %v", err)
	}
	second, err := ReadRepository(root)
	if err != nil {
		t.Fatalf("read repository again: %v", err)
	}
	if first != second {
		t.Fatal("repository identity changed on repeated initialization")
	}
}

func TestPublishIsImmutableAndAcceptsOnlyIdenticalRetry(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "key.ltx")
	first, err := os.CreateTemp(dir, "stage-*")
	if err != nil {
		t.Fatal(err)
	}
	firstPath := first.Name()
	if _, err := first.WriteString("content"); err != nil {
		t.Fatal(err)
	}
	if err := first.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Publish(firstPath, finalPath); err != nil {
		t.Fatalf("publish first object: %v", err)
	}

	identical, err := os.CreateTemp(dir, "stage-*")
	if err != nil {
		t.Fatal(err)
	}
	identicalPath := identical.Name()
	_, _ = identical.WriteString("content")
	_ = identical.Sync()
	_ = identical.Close()
	if err := Publish(identicalPath, finalPath); err != nil {
		t.Fatalf("publish identical retry: %v", err)
	}

	different, err := os.CreateTemp(dir, "stage-*")
	if err != nil {
		t.Fatal(err)
	}
	differentPath := different.Name()
	_, _ = different.WriteString("different")
	_ = different.Sync()
	_ = different.Close()
	if err := Publish(differentPath, finalPath); err == nil {
		t.Fatal("expected divergent immutable retry to fail")
	}
	stored, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored) != "content" {
		t.Fatal("existing immutable object changed")
	}
}
