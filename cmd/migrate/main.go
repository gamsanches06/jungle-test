// Command migrate applies or reverts the versioned SQL migrations.
//
//	migrate up            apply every pending migration
//	migrate down [n]      revert n migrations (default 1); "down all" reverts everything
//	migrate goto <v>      migrate up or down to version v
//	migrate version       print the current version
//	migrate force <v>     mark version v as applied after a failed migration
//
// The database URL is read from MIGRATE_DATABASE_URL (a role that owns the
// schema, not the application role).
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/golang-migrate/migrate/v4"

	"github.com/gamsanches06/jungle-test/migrations"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: migrate up|down [n|all]|goto <v>|version|force <v>")
	}
	url := os.Getenv("MIGRATE_DATABASE_URL")
	if url == "" {
		return errors.New("MIGRATE_DATABASE_URL is required")
	}
	m, err := migrations.New(url)
	if err != nil {
		return err
	}
	defer m.Close()

	switch args[0] {
	case "up":
		err = m.Up()
	case "down":
		switch {
		case len(args) > 1 && args[1] == "all":
			err = m.Down()
		default:
			n := 1
			if len(args) > 1 {
				if n, err = strconv.Atoi(args[1]); err != nil || n < 1 {
					return fmt.Errorf("invalid step count %q", args[1])
				}
			}
			err = m.Steps(-n)
		}
	case "goto":
		if len(args) < 2 {
			return errors.New("goto requires a version")
		}
		v, perr := strconv.ParseUint(args[1], 10, 32)
		if perr != nil {
			return perr
		}
		err = m.Migrate(uint(v))
	case "force":
		if len(args) < 2 {
			return errors.New("force requires a version")
		}
		v, perr := strconv.Atoi(args[1])
		if perr != nil {
			return perr
		}
		err = m.Force(v)
	case "version":
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	v, dirty, verr := m.Version()
	if errors.Is(verr, migrate.ErrNilVersion) {
		fmt.Println("version: none")
		return nil
	}
	if verr != nil {
		return verr
	}
	fmt.Printf("version: %d dirty: %v\n", v, dirty)
	return nil
}
