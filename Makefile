# Portrait Engine — developer task runner.
# Recipes use tabs (required by make).

COMPOSE       ?= docker compose
COMPOSE_FILE  ?= deploy/docker-compose.yml
# DSN used by the compose-internal migrate service (postgres reachable by name).
MIGRATE_DSN   ?= postgres://portrait:portrait@postgres:5432/portrait?sslmode=disable

GO            ?= go
BUILD_DIR     ?= bin

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: dev
dev: ## Start the full stack (api, worker, postgres, nats, minio) via Docker Compose
	$(COMPOSE) -f $(COMPOSE_FILE) up --build

.PHONY: down
down: ## Stop and remove the Docker Compose stack
	$(COMPOSE) -f $(COMPOSE_FILE) down -v

.PHONY: build
build: ## Build both binaries into ./bin
	$(GO) build -o $(BUILD_DIR)/api  ./cmd/api
	$(GO) build -o $(BUILD_DIR)/worker ./cmd/worker

.PHONY: fmt
fmt: ## Format code with gofmt
	gofmt -w -s .

.PHONY: fmt-check
fmt-check: ## Fail if any file is not gofmt'd
	@out="$$(gofmt -l -s .)"; if [ -n "$$out" ]; then echo "unformatted files:"; echo "$$out"; exit 1; fi

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: test
test: ## Run unit tests
	$(GO) test ./...

.PHONY: test-race
test-race: ## Run tests with the race detector
	$(GO) test -race ./...

.PHONY: test-integration
test-integration: ## Run integration tests (requires Docker; uses testcontainers)
	# Ryuk (the testcontainers reaper) is flaky on some Docker setups; tests
	# clean up their own containers via t.Cleanup/Terminate, so disable it.
	TESTCONTAINERS_RYUK_DISABLED=true $(GO) test -tags=integration -race -count=1 -timeout=600s ./...

.PHONY: lint
lint: ## Run golangci-lint (install: https://golangci-lint.run)
	golangci-lint run ./...

.PHONY: tidy
tidy: ## Tidy go.mod / go.sum
	$(GO) mod tidy

.PHONY: migrate-up
migrate-up: ## Apply all database migrations (runs the compose migrate service)
	$(COMPOSE) -f $(COMPOSE_FILE) run --rm migrate \
		-path=/migrations -database "$(MIGRATE_DSN)" up

.PHONY: migrate-down
migrate-down: ## Roll back the last database migration
	$(COMPOSE) -f $(COMPOSE_FILE) run --rm migrate \
		-path=/migrations -database "$(MIGRATE_DSN)" down 1

.PHONY: docker-build
docker-build: ## Build the application Docker image
	docker build -t portrait-engine:local .

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BUILD_DIR)
