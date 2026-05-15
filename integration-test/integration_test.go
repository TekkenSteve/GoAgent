package integration_test

import (
	"context"
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
	host     = "app"
	attempts = 60

	// Attempts connection
	httpURL        = "http://" + host + ":8080"
	healthPath     = httpURL + "/healthz"
	requestTimeout = 5 * time.Second

	// HTTP REST
	basePathV1 = httpURL + "/v1"
)

var errHealthCheck = fmt.Errorf("url %s is not available", healthPath)

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
		statusCode, err := getHealthCheck(healthPath)
		if err == nil && statusCode == http.StatusOK {
			return nil
		}

		if err != nil {
			log.Printf("Integration tests: url %s is not available: %v, attempts left: %d", healthPath, err, attempts)
		} else {
			log.Printf("Integration tests: url %s is not available, attempts left: %d", healthPath, attempts)
		}

		time.Sleep(time.Second)

		attempts--
	}

	return errHealthCheck
}

// seedTestAccount ensures the test account has a non-zero credit balance.
// Called before tests run so that the billing prep-check passes.
func seedTestAccount() {
	pgURL := os.Getenv("PG_URL")
	if pgURL == "" {
		log.Fatalf("Integration tests: PG_URL not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		log.Fatalf("Integration tests: pgxpool.New: %v", err)
	}
	defer pool.Close()

	// Wait for the credit_accounts table to exist (migrations may still be running)
	for i := 0; i < 30; i++ {
		var exists bool
		err := pool.QueryRow(ctx,
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
		log.Fatalf("Integration tests: seed credit_account: %v", err)
	}
	log.Printf("Integration tests: seeded e2e-test-account with $100")
}

func TestMain(m *testing.M) {
	err := healthCheck(attempts)
	if err != nil {
		log.Fatalf("Integration tests: httpURL %s is not available: %s", httpURL, err)
	}

	log.Printf("Integration tests: httpURL %s is available", httpURL)

	seedTestAccount()

	code := m.Run()
	os.Exit(code)
}

