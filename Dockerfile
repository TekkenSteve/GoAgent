# Step 1: Modules caching
FROM golang:1.26-alpine3.23 AS modules

COPY go.mod go.sum /modules/

WORKDIR /modules

RUN go mod download

# Step 2: Builder
FROM golang:1.26-alpine3.23 AS builder

ARG TARGET=app

COPY --from=modules /go/pkg /go/pkg
COPY . /app

WORKDIR /app

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -o /bin/app ./cmd/${TARGET}

# Step 3: Builder — the prober, and the paths the runtime user must own.
FROM builder AS runtime-assets

# The artifact store's directory belongs to the runtime user, because scratch
# has no way to create it and a non-root process cannot create it at /var/lib.
RUN mkdir -p /runtime/var/lib/goagent/artifacts && \
    chown -R 65532:65532 /runtime/var/lib/goagent

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -o /bin/healthcheck ./cmd/healthcheck

# Step 4: Final
FROM scratch

COPY --from=runtime-assets /app/config /config
COPY --from=runtime-assets /app/migrations /migrations
COPY --from=runtime-assets /bin/app /app
COPY --from=runtime-assets /bin/healthcheck /healthcheck
COPY --from=runtime-assets /runtime/var/lib/goagent /var/lib/goagent
COPY --from=runtime-assets /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Non-root, by uid: scratch has no /etc/passwd, so a user name would be
# decoration. 65532 is the conventional "nonroot" uid in distroless images.
USER 65532:65532

# The image carries no shell and no curl, so the probe is our own binary.
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/healthcheck"]

CMD ["/app"]
