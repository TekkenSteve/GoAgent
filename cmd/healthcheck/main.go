// Command healthcheck probes the application's own health endpoint.
//
// It exists because the runtime image is built FROM scratch: there is no
// shell, no curl and no wget inside it, so Docker's HEALTHCHECK has nothing to
// run. This binary is statically linked, has no dependencies, and speaks to
// the loopback interface only.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"
)

const (
	exitHealthy   = 0
	exitUnhealthy = 1

	defaultTimeout = 2 * time.Second
)

func main() {
	url := flag.String("url", defaultURL(), "health endpoint to probe")
	timeout := flag.Duration("timeout", defaultTimeout, "probe timeout")
	whoami := flag.Bool("whoami", false, "print the effective user id and exit (the image has no shell to ask)")

	flag.Parse()

	// A scratch image has no `id`, so this is how an operator confirms the
	// process is not running as root.
	if *whoami {
		fmt.Fprintf(os.Stdout, "uid=%d gid=%d\n", os.Getuid(), os.Getgid())

		return
	}

	os.Exit(probe(*url, *timeout))
}

// probe returns the process exit code for one health request.
func probe(url string, timeout time.Duration) int {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %s: %v\n", url, err)

		return exitUnhealthy
	}

	client := &http.Client{Timeout: timeout}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %s: %v\n", url, err)

		return exitUnhealthy
	}

	// The body is closed before the exit code is decided, so nothing is left
	// to a deferred call the process never runs.
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: %s: status %d\n", url, resp.StatusCode)

		return exitUnhealthy
	}

	return exitHealthy
}

func defaultURL() string {
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}

	return "http://127.0.0.1:" + port + "/healthz"
}
