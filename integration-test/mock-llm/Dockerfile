FROM golang:1.26-alpine3.23 AS builder

WORKDIR /app
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=off go build -o /mock-llm .

FROM scratch
COPY --from=builder /mock-llm /mock-llm
EXPOSE 8080
CMD ["/mock-llm"]
