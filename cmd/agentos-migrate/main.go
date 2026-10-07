// Command agentos-migrate applies the GoAgent-owned PostgreSQL migration set.
// It is intended for deployment jobs and deliberately does not start AgentOS.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var errPostgresURLRequired = errors.New("PG_URL is required")

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (runErr error) {
	databaseURL := strings.TrimSpace(os.Getenv("PG_URL"))
	if databaseURL == "" {
		return errPostgresURLRequired
	}

	migrationsURL := strings.TrimSpace(os.Getenv("AGENTOS_MIGRATIONS_URL"))
	if migrationsURL == "" {
		migrationsURL = "file://migrations"
	}

	migrator, err := migrate.New(migrationsURL, postgresURL(databaseURL))
	if err != nil {
		return fmt.Errorf("open migrations: %w", err)
	}

	defer func() {
		sourceErr, databaseErr := migrator.Close()
		runErr = errors.Join(runErr, sourceErr, databaseErr)
	}()

	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}

	log.Print("AgentOS migrations are current")

	return nil
}

func postgresURL(value string) string {
	if strings.Contains(value, "?") {
		return value
	}

	return fmt.Sprintf("%s?sslmode=disable", value)
}
