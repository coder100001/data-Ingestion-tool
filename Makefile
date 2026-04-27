.PHONY: build clean test run docker-build docker-run help

# Variables
BINARY_NAME=data-ingestion-tool
DOCKER_IMAGE=data-ingestion-tool:latest
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE=$(shell date -u '+%Y-%m-%d_%H:%M:%S')

# Go build flags
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(BUILD_DATE)"

# Default target
all: build

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	go build $(LDFLAGS) -o bin/$(BINARY_NAME) ./cmd/ingester

# Build for multiple platforms
build-all:
	@echo "Building for multiple platforms..."
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-linux-amd64 ./cmd/ingester
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-darwin-amd64 ./cmd/ingester
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-darwin-arm64 ./cmd/ingester
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-windows-amd64.exe ./cmd/ingester

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf bin/
	rm -f coverage.out coverage.html

# Run the application
run: build
	./bin/$(BINARY_NAME) -config=config.yaml

# Run with reset flag
run-reset: build
	./bin/$(BINARY_NAME) -config=config.yaml -reset

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Run linter
lint:
	@echo "Running linter..."
	golangci-lint run ./...

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE) .

# Run Docker container
docker-run:
	@echo "Running Docker container..."
	docker run -it --rm \
		-v $(PWD)/config.yaml:/app/config.yaml \
		-v $(PWD)/data-lake:/app/data-lake \
		-v $(PWD)/metadata:/app/metadata \
		-v $(PWD)/logs:/app/logs \
		$(DOCKER_IMAGE)

# Docker Compose up
docker-compose-up:
	@echo "Starting with Docker Compose..."
	docker-compose up -d

# Docker Compose down
docker-compose-down:
	@echo "Stopping Docker Compose..."
	docker-compose down

# Generate mocks for testing
mocks:
	@echo "Generating mocks..."
	go generate ./...

# Run integration tests
integration-test:
	@echo "Running integration tests..."
	go test -v -tags=integration ./tests/...

# Create necessary directories
setup:
	@echo "Creating directories..."
	mkdir -p data-lake metadata logs

# Show help
help:
	@echo "Available targets:"
	@echo "  build           - Build the binary"
	@echo "  build-all       - Build for multiple platforms"
	@echo "  test            - Run unit tests"
	@echo "  test-coverage   - Run tests with coverage report"
	@echo "  clean           - Clean build artifacts"
	@echo "  run             - Build and run the application"
	@echo "  run-reset       - Run with checkpoint reset"
	@echo "  deps            - Download dependencies"
	@echo "  fmt             - Format code"
	@echo "  lint            - Run linter"
	@echo "  docker-build    - Build Docker image"
	@echo "  docker-run      - Run Docker container"
	@echo "  docker-compose-up   - Start with Docker Compose"
	@echo "  docker-compose-down - Stop Docker Compose"
	@echo "  setup           - Create necessary directories"
	@echo "  help            - Show this help message"
