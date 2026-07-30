SHELL := /usr/bin/env bash

GO ?= go
DOCKER ?= docker
BIN_DIR ?= bin
IMAGE ?= kubebrain
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GO_VERSION := $(shell $(GO) env GOVERSION)
GO_OS_ARCH := $(shell $(GO) env GOOS)/$(shell $(GO) env GOARCH)
VERSION_PACKAGE := github.com/kubewharf/kubebrain/cmd/version
COMMON_LDFLAGS := -s -w \
	-X $(VERSION_PACKAGE).Version=$(VERSION) \
	-X $(VERSION_PACKAGE).GitSHA=$(GIT_SHA) \
	-X $(VERSION_PACKAGE).GoVersion=$(GO_VERSION) \
	-X $(VERSION_PACKAGE).GoOsArch=$(GO_OS_ARCH) \
	-X $(VERSION_PACKAGE).Date=$(BUILD_DATE)

.DEFAULT_GOAL := help

.PHONY: help build tikv badger test test-race coverage format lint vuln docker-build clean

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z_0-9-]+:.*?## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: tikv ## Build the default TiKV-backed binary

tikv: ## Build the TiKV-backed binary
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -tags tikv -ldflags "$(COMMON_LDFLAGS) -X $(VERSION_PACKAGE).Storage=TiKV" -o $(BIN_DIR)/kube-brain ./cmd

badger: ## Build the Badger-backed binary
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -tags badger -ldflags "$(COMMON_LDFLAGS) -X $(VERSION_PACKAGE).Storage=Badger" -o $(BIN_DIR)/kube-brain ./cmd

test: ## Run unit tests
	$(GO) test ./...

test-race: ## Run the race detector
	$(GO) test -race ./...

coverage: ## Write a package coverage report
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...

format: ## Format Go source files
	gofmt -w -s $$(find . -name '*.go' -not -path './.git/*' -not -path './pkg/metrics/mock/mock_metrics.go')

lint: ## Run golangci-lint
	golangci-lint run

vuln: ## Scan reachable Go code for known vulnerabilities
	govulncheck ./...

docker-build: ## Build the default production container image
	$(DOCKER) build -f build/kubebrain.Dockerfile --build-arg STORAGE=tikv -t $(IMAGE):local .

clean: ## Remove generated build artifacts
	rm -rf $(BIN_DIR) coverage.out
