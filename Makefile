################### Helpers ######################

## help: print this help message
.PHONY: help
help:
	@echo 'Usage:'
	@sed -n 's/^//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'

.PHONY: confirm
confirm:
	@echo -n "Are you sure? [y/N] " && read ans && [ $${ans:-N} = y ]

################### Dev Dependencies ######################
.PHONY: dev-deps
dev-deps:
	@echo 'Installing development dependencies...'
	@echo 'Installing protoc compiler and plugins...'
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo 'Installing linters and tools...'
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install honnef.co/go/tools/cmd/staticcheck@latest
	@go install golang.org/x/tools/cmd/goimports@latest
	@go install github.com/vektra/mockery/v2@latest
	@echo 'Checking protoc installation...'
	@if ! command -v protoc >/dev/null 2>&1; then \
		echo "protoc is not installed. Please install it manually:"; \
		exit 1; \
	fi

################### Dev ######################
.PHONY: proto-compile
proto-compile:
	@echo 'Generating protobuf code...'
	protoc proto/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

################### Clean ######################
.PHONY: clean
clean:
	@echo 'Cleaning up...'
	@go clean
	@rm -f gafka
	@rm -rf test-topic-*

################### Lint ######################
.PHONY: lint
lint:
	@echo 'Running linters...'
	@echo 'Running staticcheck...'
	staticcheck ./...
	@echo 'Running golangci-lint...'
	golangci-lint run ./...

################### QA ######################
.PHONY: audit
audit: proto-compile clean lint
	@echo 'Formatting code...'
	go fmt ./...
	@echo 'Organizing imports...'
	goimports -w .
	@echo 'Vetting code...'
	go vet ./...
	@echo 'Running tests'
	go test -race -vet=off ./... -timeout 30s

.PHONY: test
test: proto-compile clean
	@echo 'Running tests'
	go test -race -vet=off ./... -timeout 30s

## tidy and verify dependencies
.PHONY: vendor
vendor:
	@echo 'Tidying and verifying module dependencies...'
	go mod tidy
	go mod verify

################### Build ######################
current_time = $(shell date -Iseconds)
git_desc = $(shell git describe --always --dirty --tags --long)
linker_flags = '-s -X main.buildTime=${current_time} -X main.version=${git_desc}'

## build: build the application
.PHONY: build
build: clean proto-compile vendor audit
	@echo 'Building gafka...'
	CGO_ENABLED=0 go build -ldflags=${linker_flags} -o=./gafka ./cmd/main.go

## quick build without tests
.PHONY: build/quick
build/quick: clean proto-compile
	@echo 'Quick building gafka...'
	CGO_ENABLED=0 go build -ldflags=${linker_flags} -o=./gafka ./cmd/main.go

################### Run ######################
.PHONY: run
run: build
	@echo 'Running gafka...'
	./gafka