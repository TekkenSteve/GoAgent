FROM golang:1.26-alpine3.23 AS modules

COPY go.mod go.sum /modules/

WORKDIR /modules

RUN go mod download

FROM golang:1.26-alpine3.23

COPY --from=modules /go/pkg /go/pkg
COPY . /app

WORKDIR /app

RUN go env -w CGO_ENABLED=0 \
    && go env -w GOOS=linux  \
    && go env -w GOARCH=amd64

CMD ["go", "run", "./integration-test/mock-agent-backends"]
