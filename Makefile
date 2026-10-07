.PHONY: up down deps test race vet fmt integration integration-race migrate-up migrate-down load

MIGRATE_DATABASE_URL ?= postgres://jungle:jungle-1234@localhost:5432/jungle_test?sslmode=disable

up:            ## full stack: postgres, keycloak, localstack, migrations, 3 app instances
	docker compose up --build -d

down:          ## stop and remove containers and volumes
	docker compose down -v

deps:          ## only the dependencies used by the integration tests
	docker compose up -d --wait postgres keycloak localstack

test:          ## unit tests (no infrastructure needed)
	go test ./...

race:          ## unit tests with the race detector (requires cgo/gcc)
	go test -race ./...

vet:
	go vet ./... && go vet -tags integration ./test/...

fmt:
	gofmt -l -w .

integration: deps  ## integration, multi-instance and failure tests against real containers
	go test -tags integration -count=1 -timeout 20m ./test/integration/

integration-race: deps
	go test -race -tags integration -count=1 -timeout 20m ./test/integration/

migrate-up:
	MIGRATE_DATABASE_URL=$(MIGRATE_DATABASE_URL) go run ./cmd/migrate up

migrate-down:
	MIGRATE_DATABASE_URL=$(MIGRATE_DATABASE_URL) go run ./cmd/migrate down 1

load:          ## load test against the 3 compose instances
	go run ./cmd/loadtest -duration 30s -concurrency 32 -wallets 50
