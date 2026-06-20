# Step 1: Modules caching
FROM golang:1.26-alpine3.23 AS modules

COPY go.mod go.sum /modules/

WORKDIR /modules

RUN go mod download

# Step 2: Tests
FROM golang:1.26-alpine3.23

COPY --from=modules /go/pkg /go/pkg
COPY . /app

WORKDIR /app

RUN go env -w CGO_ENABLED=0 \
    && go env -w GOOS=linux  \
    && go env -w GOARCH=amd64

ENV GO_TEST_FLAGS="-v"
ENV GO_TEST_PACKAGES="./integration-test/..."

CMD ["sh", "-c", "go test ${GO_TEST_FLAGS} ${GO_TEST_PACKAGES}"]
