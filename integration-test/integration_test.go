package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// Base settings.
	attempts = 60

	// Attempts connection.
	requestTimeout = 5 * time.Second

	// testAccountID is the tenant the suite acts as: the funded account
	// seedTestAccount upserts, and the subject of every token it mints. The
	// account a request runs in comes from the credential, never from the body
	// (see internal/controller/restapi/v1/tenant.go), so these must agree.
	testAccountID = "e2e-test-account"
)

// The variables the suite reads its authentication settings from. It is one
// more client of the contract the app enforces: in development the app trusts
// HS256 tokens signed with AUTH_HMAC_SECRET, so the suite mints one per
// request. Compose passes these to the test container; they must match the
// app's, and a drift shows up as a 401 on every protected route rather than as
// a skipped check.
//
// The constants hold variable names, not values — hence the "Name" suffix,
// which is also what keeps a credential scanner from reading the identifier as
// a hardcoded secret.
const (
	envAuthHMACName     = "AUTH_HMAC_SECRET"
	envAuthIssuerName   = "AUTH_ISSUER"
	envAuthAudienceName = "AUTH_AUDIENCE"
)

var errAuthConfigMissing = errors.New("auth configuration is missing")

// authToken mints the bearer token the stack accepts: the test account as the
// subject, an expiry the verifier requires, and the issuer and audience the app
// is configured to check when they are set.
func authToken() (string, error) {
	secret := strings.TrimSpace(os.Getenv(envAuthHMACName))
	if secret == "" {
		return "", fmt.Errorf("%w: set %s", errAuthConfigMissing, envAuthHMACName)
	}

	claims := jwt.MapClaims{
		"sub": testAccountID,
		"exp": time.Now().Add(time.Hour).Unix(),
	}

	if issuer := strings.TrimSpace(os.Getenv(envAuthIssuerName)); issuer != "" {
		claims["iss"] = issuer
	}

	if audience := strings.TrimSpace(os.Getenv(envAuthAudienceName)); audience != "" {
		claims["aud"] = audience
	}

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("sign integration token: %w", err)
	}

	return signed, nil
}

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

	token, err := authToken()
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	return http.DefaultClient.Do(req)
}

func getHealthCheck(url string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)

	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return -1, err
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("health check: close response body: %w", closeErr))
		}
	}()

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
		VALUES ($1, 100, 'USD')
		ON CONFLICT (account_id)
		DO UPDATE SET balance = 100, version = credit_accounts.version + 1, updated_at = NOW()
	`, testAccountID)
	if err != nil {
		return fmt.Errorf("seed credit_account: %w", err)
	}

	log.Printf("Integration tests: seeded %s with $100", testAccountID)

	return nil
}

func TestMain(m *testing.M) {
	if _, err := authToken(); err != nil {
		log.Fatalf("Integration tests: %v", err)
	}

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
