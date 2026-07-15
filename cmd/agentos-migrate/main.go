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

func main() {
	databaseURL := strings.TrimSpace(os.Getenv("PG_URL"))
	if databaseURL == "" {
		log.Fatal("PG_URL is required")
	}

	migrationsURL := strings.TrimSpace(os.Getenv("AGENTOS_MIGRATIONS_URL"))
	if migrationsURL == "" {
		migrationsURL = "file://migrations"
	}
	migrator, err := migrate.New(migrationsURL, postgresURL(databaseURL))
	if err != nil {
		log.Fatalf("open migrations: %v", err)
	}
	defer migrator.Close()
	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatalf("apply migrations: %v", err)
	}
	log.Print("AgentOS migrations are current")
}

func postgresURL(value string) string {
	if strings.Contains(value, "?") {
		return value
	}
	return fmt.Sprintf("%s?sslmode=disable", value)
}
