.PHONY: build test test-integration run up down chaos

build:
	go build -o bin/forgeflow ./cmd/forgeflow

test:
	go test -race ./...

# Needs the compose Postgres: docker compose up -d postgres (host port 5433)
test-integration:
	FORGEFLOW_TEST_DATABASE_URL=postgres://forgeflow:forgeflow@localhost:5433/forgeflow?sslmode=disable \
		go test -race -count=1 ./...

run: build
	./bin/forgeflow

up:
	docker compose up --build -d

down:
	docker compose down -v

# Kill a worker mid-flight and watch its runs get recovered on the dashboard.
chaos:
	docker compose kill worker-2
	@echo "worker-2 killed; watch http://localhost:8080 - its runs will be recovered"
