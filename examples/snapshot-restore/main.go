package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	replication "github.com/lucaswilliameufrasio/sqlite-replication-go"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) != 3 {
		return errors.New("usage: snapshot-restore DATABASE REPOSITORY RESTORE_DESTINATION")
	}

	ctx := context.Background()
	ltxPath, err := replication.Snapshot(ctx, arguments[0], arguments[1])
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	if err := replication.RestoreSnapshot(ctx, arguments[1], arguments[2]); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	fmt.Printf("published %s and restored %s\n", ltxPath, arguments[2])
	return nil
}
