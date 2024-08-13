
################### Helpers ######################

## help: print this help message
.PHONY: help
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'

.PHONY: confirm
confirm:
	@echo -n "Are you sure? [y/N] " && read ans && [ $${ans:-N} = y ]


################### Dev ######################



################### QA ######################
.PHONY: audit
audit:
	@echo 'Formatting code...'
	go fmt ./...
	@echo 'Vetting code...'
	go vet ./...
	# staticcheck ./...
	@echo 'Running tests'
	go test -race -vet=off ./...  -timeout 30s

## tidy and verify dependencies
.PHONY: tidy
vendor:
	@echo 'Tidying and verifying module dependencies...'
	go mod tidy
	go mod verify

################### Build ######################
current_time = $(shell date -Iseconds)
git_desc = $(shell git describe --always --dirty --tags --long)
linker_flags = '-s -X main.buildTime=${current_time}  -X main.version=${git_desc}'

## build: build the application
.PHONY: build/bins
build/bins:
	@echo 'Building cmd/api'
	CGO_ENABLED=0 go build -ldflags=${linker_flags} -o=./gafka ./cmd/main.go

################### Docker ######################
