# Run `make help` to list targets. `make up` starts the whole stack locally.
APP        := gpu-telemetry
IMAGE_REPO ?= $(APP)
IMAGE_TAG  ?= local
STREAMERS  ?= 2
COLLECTORS ?= 3
SWAG       := github.com/swaggo/swag/cmd/swag@v1.16.4

# Resolve the docker CLI from PATH, falling back to the Docker Desktop install location
# so `make up` works even when the shell PATH is not set up (common right after install).
DOCKER := $(shell command -v docker || echo $(HOME)/.docker/bin/docker)

.PHONY: help build test coverage openapi lint up demo scale down logs docker-build kind-load helm-install helm-uninstall

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  %-15s %s\n", $$1, $$2}'

build: ## Build all binaries into bin/
	go build -o bin/ ./cmd/...

test: ## Run unit and system tests with the race detector
	go test -race ./...

# Cover the packages that hold logic, exercised by their own tests plus the e2e system test.
# Scoping to packages that have tests keeps this working on minimal toolchains (no covdata tool).
COVER_PKGS := ./internal/api ./internal/collector ./internal/mq ./internal/platform ./internal/store ./internal/streamer ./internal/telemetry
coverage: ## Run tests and report coverage (HTML report in coverage.html)
	go test -race -count=1 -coverpkg=$(shell echo $(COVER_PKGS) | tr ' ' ',') -coverprofile=coverage.raw $(COVER_PKGS) ./test/e2e
	@# Each test binary reports every -coverpkg block; keep the highest count per block.
	@awk 'NR==1 {print; next} {k = $$1 " " $$2; if (!(k in c) || $$3 > c[k]) c[k] = $$3} END {for (k in c) print k, c[k]}' coverage.raw > coverage.out && rm coverage.raw
	go tool cover -func=coverage.out | tail -1
	go tool cover -html=coverage.out -o coverage.html
	@echo "HTML report: coverage.html"

openapi: ## Regenerate the OpenAPI spec (docs/swagger.json, docs/swagger.yaml) from code annotations
	go run $(SWAG) init -g cmd/api/main.go -o docs --parseInternal

lint: ## Vet the code
	go vet ./...

.env: ## Generate a random DB password for Docker Compose (git-ignored)
	@umask 077 && echo "POSTGRES_PASSWORD=$$(openssl rand -hex 24)" > .env

up: .env ## Build and start the stack with Docker Compose
	$(DOCKER) compose up --build -d --scale streamer=$(STREAMERS) --scale collector=$(COLLECTORS)
	@echo "API:     http://localhost:8080/api/v1/gpus"
	@echo "Swagger: http://localhost:8080/swagger/index.html"

demo: up ## One command for first-time users: start the stack and verify it end-to-end
	@echo "Verifying the pipeline (this waits for telemetry to flow)..."
	@./scripts/smoke_test.sh

scale: ## Rescale a running stack, e.g. make scale STREAMERS=1 COLLECTORS=5
	$(DOCKER) compose up -d --no-recreate --scale streamer=$(STREAMERS) --scale collector=$(COLLECTORS)

down: ## Stop the stack and delete its volumes
	$(DOCKER) compose down -v

logs: ## Follow logs from all services
	$(DOCKER) compose logs -f

docker-build: ## Build the container image
	$(DOCKER) build -t $(IMAGE_REPO):$(IMAGE_TAG) .

kind-load: docker-build ## Load the image into a local kind cluster
	kind load docker-image $(IMAGE_REPO):$(IMAGE_TAG)

helm-install: ## Install or upgrade the Helm release on the current kube context
	helm upgrade --install $(APP) deploy/helm/$(APP) --set image.repository=$(IMAGE_REPO) --set image.tag=$(IMAGE_TAG) \
		--set streamer.replicas=$(STREAMERS) --set collector.replicas=$(COLLECTORS)

helm-uninstall: ## Remove the Helm release
	helm uninstall $(APP)
