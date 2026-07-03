package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// Base settings
	attempts = 60

	// Attempts connection
	requestTimeout = 5 * time.Second
)

// host returns the integration test target host.
// Override via INTEGRATION_TEST_HOST env var (e.g. "localhost" when running from host).
func host() string {
	if h := os.Getenv("INTEGRATION_TEST_HOST"); h != "" {
		return h
	}

	return "app"
}

func httpURL() string {
	if u := os.Getenv("INTEGRATION_TEST_BASE_URL"); u != "" {
		return u
	}

	return "http://" + host() + ":8080"
}

func healthPath() string {
	return httpURL() + "/healthz"
}

func basePathV1() string {
	return httpURL() + "/v1"
}

var errIntegrationURLNotAvailable = errors.New("integration url is not available")

func errHealthCheck() error {
	return fmt.Errorf("%w: %s", errIntegrationURLNotAvailable, healthPath())
}

// getPGURL returns the PostgreSQL connection string.
// Checks INTEGRATION_TEST_PG_URL first, then falls back to PG_URL.
func getPGURL() string {
	if u := os.Getenv("INTEGRATION_TEST_PG_URL"); u != "" {
		return u
	}

	return os.Getenv("PG_URL")
}

var errPGURLNotSet = errors.New("PG_URL not set")

func doWebRequestWithTimeout(ctx context.Context, method, url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	return http.DefaultClient.Do(req)
}

func getHealthCheck(url string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)

	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return -1, err
	}

	defer resp.Body.Close()

	return resp.StatusCode, nil
}

func healthCheck(attempts int) error {
	for attempts > 0 {
		statusCode, err := getHealthCheck(healthPath())
		if err == nil && statusCode == http.StatusOK {
			return nil
		}

		if err != nil {
			log.Printf("Integration tests: url %s is not available: %v, attempts left: %d", healthPath(), err, attempts)
		} else {
			log.Printf("Integration tests: url %s is not available, attempts left: %d", healthPath(), attempts)
		}

		time.Sleep(time.Second)

		attempts--
	}

	return errHealthCheck()
}

// seedTestAccount ensures the test account has a non-zero credit balance.
// Called before tests run so that the billing prep-check passes.
func seedTestAccount() error {
	pgURL := getPGURL()
	if pgURL == "" {
		return errPGURLNotSet
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return fmt.Errorf("pgxpool.New: %w", err)
	}
	defer pool.Close()

	// Wait for the credit_accounts table to exist (migrations may still be running)
	for range 30 {
		var exists bool

		err := pool.QueryRow(
			ctx,
			"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'credit_accounts')",
		).Scan(&exists)
		if err == nil && exists {
			break
		}

		time.Sleep(time.Second)
	}

	// Upsert the test account with a $100 balance
	_, err = pool.Exec(ctx, `
		INSERT INTO credit_accounts (account_id, balance, currency)
		VALUES ('e2e-test-account', 100, 'USD')
		ON CONFLICT (account_id)
		DO UPDATE SET balance = 100, version = credit_accounts.version + 1, updated_at = NOW()
	`)
	if err != nil {
		return fmt.Errorf("seed credit_account: %w", err)
	}

	log.Printf("Integration tests: seeded e2e-test-account with $100")

	return nil
}

func TestMain(m *testing.M) {
	err := healthCheck(attempts)
	if err != nil {
		log.Fatalf("Integration tests: httpURL %s is not available: %s", httpURL(), err)
	}

	log.Printf("Integration tests: httpURL %s is available", httpURL())

	if err := seedTestAccount(); err != nil {
		log.Fatalf("Integration tests: seed account: %v", err)
	}

	code := m.Run()
	os.Exit(code)
}
