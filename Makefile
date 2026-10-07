ifneq ($(wildcard .env),)
include .env
export
else
$(warning WARNING: .env file not found! Using .env.example)
include .env.example
export
endif

GO ?= go

BASE_STACK = docker compose -f docker-compose.dev.yml
INTEGRATION_TEST_STACK = $(BASE_STACK) -f docker-compose-integration-test.yml
ALL_STACK = $(INTEGRATION_TEST_STACK)

# HELP =================================================================================================================
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help

help: ## Display this help screen
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

dev-secrets: ### create .env with freshly generated development credentials (run once)
	@if [ -f .env ]; then \
		echo ".env already exists; leaving it alone"; \
	else \
		cp .env.example .env; \
		{ \
			echo "CENTRIFUGO_API_KEY=$$(openssl rand -hex 16)"; \
			echo "CENTRIFUGO_ADMIN_PASSWORD=$$(openssl rand -hex 16)"; \
			echo "CENTRIFUGO_ADMIN_SECRET=$$(openssl rand -hex 16)"; \
		} >> .env; \
		echo "created .env with generated development credentials"; \
	fi
.PHONY: dev-secrets

compose-up: ### Run docker compose (without backend and reverse proxy)
	$(BASE_STACK) up --build -d db redis centrifugo temporal temporal-create-namespace && docker compose logs -f
.PHONY: compose-up

compose-up-all: ### Run docker compose (with backend and reverse proxy)
	$(BASE_STACK) up --build -d
.PHONY: compose-up-all

compose-up-integration-test: ### Run docker compose with integration test
	exit_code=0; \
	trap '$(ALL_STACK) down --remove-orphans' EXIT; \
	$(INTEGRATION_TEST_STACK) up --build --exit-code-from integration-test integration-test || exit_code=$$?; \
	exit $$exit_code
.PHONY: compose-up-integration-test

compose-up-mixed-backend-integration-test: ### Run docker compose mixed AgentOS RunPlan backend test
	exit_code=0; \
	trap '$(ALL_STACK) down --remove-orphans' EXIT; \
	GO_TEST_FLAGS="-v -run TestHTTPAgentOSRunPlanMixedBackendsV1 -count=1" \
	GO_TEST_PACKAGES="./integration-test" \
	$(INTEGRATION_TEST_STACK) up --build --exit-code-from integration-test integration-test || exit_code=$$?; \
	exit $$exit_code
.PHONY: compose-up-mixed-backend-integration-test

compose-down: ### Down docker compose
	$(ALL_STACK) down --remove-orphans
.PHONY: compose-down

swag-v1: ### swag init
	$(GO) tool swag init --dir internal/controller/restapi,internal/controller/restapi/v1/request,internal/controller/restapi/v1/response,agentos \
		-g router.go --output docs --parseInternal --parseDependency
.PHONY: swag-v1

deps: ### deps tidy + verify
	$(GO) mod tidy && $(GO) mod verify
.PHONY: deps

deps-audit: ### check dependencies vulnerabilities
	$(GO) tool govulncheck ./...
.PHONY: deps-audit

fix-diff: ### Show code changes by `go fix`
	$(GO) fix -diff ./...
.PHONY: fix-diff

format: ### Run code formatter
	$(GO) fix ./...
	$(GO) tool gofumpt -l -w .
	$(GO) tool gci write . --skip-generated -s standard -s default
.PHONY: format

run: deps swag-v1 ### swag run for API v1
	$(GO) mod download && \
	CGO_ENABLED=0 $(GO) run -tags migrate ./cmd/app
.PHONY: run

docker-rm-volume: ### remove docker volume
	docker volume rm go-clean-template_pg-data
.PHONY: docker-rm-volume

linter-golangci: ### check by golangci linter
	$(GO) tool golangci-lint run --timeout=15m
.PHONY: linter-golangci

linter-hadolint: ### check by hadolint linter
	git ls-files --exclude='Dockerfile*' --ignored | xargs hadolint
.PHONY: linter-hadolint

linter-dotenv: ### check by dotenv linter
	dotenv-linter
.PHONY: linter-dotenv

check-workflow-determinism: ### prevent forbidden non-determinism in workflow code
	./scripts/agentfw/check_workflow_determinism.sh .
.PHONY: check-workflow-determinism

check-import-boundary: ### enforce public AgentOS import boundaries
	./scripts/check_import_boundary.sh
.PHONY: check-import-boundary

agentos-plan-schema: ### generate AgentOS RunPlan JSON Schema
	$(GO) run ./cmd/agentos-plan schema --kind run-plan --out docs/schemas/run_plan.schema.json
	$(GO) run ./cmd/agentos-plan schema --kind plan-delta --out docs/schemas/plan_delta.schema.json
	$(GO) run ./cmd/agentos-plan schema --kind capability-catalog --out docs/schemas/capability_catalog.schema.json
	$(GO) run ./cmd/agentos-plan schema --kind artifact-schema-catalog --out docs/schemas/artifact_schema_catalog.schema.json
.PHONY: agentos-plan-schema

check-agentos-plan-schema: agentos-plan-schema ### verify committed AgentOS RunPlan schemas are current
	git diff --exit-code -- docs/schemas/run_plan.schema.json docs/schemas/plan_delta.schema.json docs/schemas/capability_catalog.schema.json docs/schemas/artifact_schema_catalog.schema.json
.PHONY: check-agentos-plan-schema

agentfw-load-suite: ### run reproducible agent framework load/soak suites
	./scripts/agentfw/run_load_suites.sh
.PHONY: agentfw-load-suite

agentfw-security-suite: ### run agent framework security validation suites
	./scripts/agentfw/run_security_suites.sh
.PHONY: agentfw-security-suite

test: ### run test
	$(GO) test -v -race -covermode atomic -coverprofile=coverage.txt \
		./agentos/... ./internal/... ./pkg/...
.PHONY: test

integration-test: ### run integration-test
	$(GO) clean -testcache && $(GO) test -v ./integration-test/...
.PHONY: integration-test

postgres-integration-test: ### run Postgres-backed AgentOS persistence integration tests
	@test -n "$(GOAGENT_POSTGRES_TEST_URL)" || (echo "GOAGENT_POSTGRES_TEST_URL is required" >&2; exit 1)
	$(GO) clean -testcache && $(GO) test -tags postgres_integration -v ./internal/repo/persistent
.PHONY: postgres-integration-test

mock: ### run mockgen
	$(GO) tool mockgen -source ./internal/repo/contracts.go -package usecase_test > ./internal/usecase/mocks_repo_test.go
	$(GO) tool mockgen -source ./internal/usecase/contracts.go -package usecase_test > ./internal/usecase/mocks_usecase_test.go
.PHONY: mock

sqlc: ### generate type-safe SQL bindings from queries/*.sql
	$(GO) tool sqlc generate
.PHONY: sqlc

sqlc-check: ### fail when generated SQL bindings are stale
	$(GO) tool sqlc generate
	@git diff --exit-code -- internal/repo/persistent/sqlcgen || (echo "sqlcgen is stale: run 'make sqlc' and commit the result" >&2; exit 1)
.PHONY: sqlc-check

migrate-create:  ### create new migration
	migrate create -ext sql -dir migrations '$(word 2,$(MAKECMDGOALS))'
.PHONY: migrate-create

migrate-up: ### migration up
	migrate -path migrations -database '$(PG_URL)?sslmode=disable' up
.PHONY: migrate-up

# The drill needs a database it may empty, so it reads its own variable rather
# than the .env PG_URL a developer points at their working data. It falls back
# to PG_URL for local convenience. A URL may carry its own parameters.
MIGRATION_DRILL_URL ?= $(PG_URL)
MIGRATION_DRILL_DSN = $(if $(findstring ?,$(MIGRATION_DRILL_URL)),$(MIGRATION_DRILL_URL),$(MIGRATION_DRILL_URL)?sslmode=disable)

migration-drill: ### roll every migration back and re-apply it (MIGRATION_DRILL_URL)
	@test -n "$(MIGRATION_DRILL_URL)" || (echo "MIGRATION_DRILL_URL is required" >&2; exit 1)
	$(GO) run -tags postgres github.com/golang-migrate/migrate/v4/cmd/migrate -path migrations -database '$(MIGRATION_DRILL_DSN)' up
	$(GO) run -tags postgres github.com/golang-migrate/migrate/v4/cmd/migrate -path migrations -database '$(MIGRATION_DRILL_DSN)' down -all
	$(GO) run -tags postgres github.com/golang-migrate/migrate/v4/cmd/migrate -path migrations -database '$(MIGRATION_DRILL_DSN)' up
.PHONY: migration-drill

bin-deps: ### install tools
	$(GO) install tool
	$(GO) install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate
.PHONY: bin-deps

pre-commit: swag-v1 mock sqlc-check format linter-golangci test ### run pre-commit
.PHONY: pre-commit
