// Package migrations embeds the versioned SQL migrations (golang-migrate
// format) and builds a migrator for them.
package migrations

import (
	"embed"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // pgx5:// driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// FS contains NNNNNN_name.{up,down}.sql files.
//
//go:embed *.sql
var FS embed.FS

// New builds a migrator for a postgres:// URL.
func New(url string) (*migrate.Migrate, error) {
	src, err := iofs.New(FS, ".")
	if err != nil {
		return nil, err
	}
	url = strings.Replace(url, "postgresql://", "pgx5://", 1)
	url = strings.Replace(url, "postgres://", "pgx5://", 1)
	return migrate.NewWithSourceInstance("iofs", src, url)
}
