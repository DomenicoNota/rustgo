ifeq ($(OS),Windows_NT)
POWERSHELL ?= powershell
else
POWERSHELL ?= pwsh
endif

.DEFAULT_GOAL := help
.PHONY: help up up-ui down status logs demo fmt test lint build docker-build e2e verify-fast verify-full ci

help: ## Show the supported root commands.
	@$(POWERSHELL) -NoProfile -File scripts/help.ps1

up: ## Build and run the core stack in the foreground; Ctrl+C stops it.
	docker compose up --build

up-ui: ## Build and run the core stack plus React explorer in the foreground.
	docker compose --profile ui up --build

down: ## Stop the stack while preserving the PostgreSQL volume.
	docker compose --profile ui down --remove-orphans

status: ## Show current Compose service and health state.
	docker compose --profile ui ps

logs: ## Follow bounded application logs for the core pipeline.
	docker compose logs --tail 100 --follow api worker agent

demo: ## Start the core stack, wait for readiness, and run the deterministic demo.
	docker compose up --build -d
	$(POWERSHELL) -NoProfile -File scripts/smoke.ps1
	$(POWERSHELL) -NoProfile -File scripts/demo.ps1

fmt: ## Format Go, Rust, and frontend source files.
	cd backend && gofmt -w .
	cd agent && cargo fmt
	cd dashboard && npm ci && npm exec -- biome format --write src

test: ## Run component tests, including Go race detection.
	cd backend && go test -race -count=1 ./...
	cd agent && cargo test --locked
	cd dashboard && npm ci && npm run test

lint: ## Run formatting checks, static analysis, and dependency audit.
	$(POWERSHELL) -NoProfile -File scripts/check-go-format.ps1
	cd backend && go vet ./...
	cd agent && cargo fmt --check && cargo clippy --locked --all-targets --all-features -- -D warnings
	cd dashboard && npm ci && npm audit --audit-level=high && npm run format:check && npm run lint && npm run typecheck

build: ## Build every application component.
	cd backend && go build ./...
	cd agent && cargo build --locked
	cd dashboard && npm ci && npm run build

docker-build: ## Validate and build all core Compose images.
	docker compose config --quiet
	docker compose build

verify-fast: ## Run the deterministic checks that do not require live infrastructure.
	$(POWERSHELL) -NoProfile -File scripts/verify-fast.ps1

verify-full: ## Run fast checks plus real PostgreSQL/Kafka and agent-to-query verification.
	$(POWERSHELL) -NoProfile -File scripts/verify-full.ps1

e2e: verify-full ## Alias for the complete infrastructure-backed verification gate.

ci: e2e ## Run the same complete gate used by CI.
