# Build and test entry points. See docs/DEVELOPMENT.md.
#
# Go is optional on the host: when `go` is not on PATH, every target runs inside the
# golang:1.27 image (--network host so `make run` serves on 127.0.0.1 and tool downloads work).

BIN        := bin/notifier
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
FUZZTIME   ?= 30s
STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.8.1
GOVULNCHECK := golang.org/x/vuln/cmd/govulncheck@latest

DOCKER_GO := docker run --rm -u $(shell id -u):$(shell id -g) -e HOME=/tmp \
	-e GOCACHE=/src/.cache/go-build -e GOMODCACHE=/src/.cache/gomod \
	-v $(CURDIR):/src -w /src --network host golang:1.27 go
GO ?= $(if $(shell command -v go 2>/dev/null),go,$(DOCKER_GO))

LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build test test-race test-integration fuzz lint vet vuln check fmt run docker demo demo-down fixtures clean help

all: build

## build: static binary in bin/
build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/notifier

## test: unit tests
test:
	$(GO) test ./...

## test-race: unit tests under the race detector (needs cgo, hence the Debian image)
test-race:
	CGO_ENABLED=1 $(GO) test -race -count=1 ./...

## test-integration: tests behind the `integration` build tag (needs AMQP_URL; skips without it)
test-integration:
	$(GO) test -tags integration -count=1 ./...

## fuzz: run every fuzz target for FUZZTIME (today: FuzzParse)
fuzz:
	@pkgs=$$(grep -rl '^func Fuzz' --include='*_test.go' --exclude-dir=.cache --exclude-dir=bin \
	  cmd internal tools 2>/dev/null | xargs -r -n1 dirname | sort -u); \
	if [ -z "$$pkgs" ]; then echo "no fuzz targets yet (Phase 4)"; exit 0; fi; \
	for p in $$pkgs; do \
	  for f in $$(grep -ho '^func Fuzz[A-Za-z0-9_]*' $$p/*_test.go | sed 's/^func //'); do \
	    echo "== $$f in $$p"; \
	    $(GO) test -run '^$$' -fuzz "^$$f$$" -fuzztime $(FUZZTIME) $$p || exit 1; \
	  done; \
	done

## lint: staticcheck (pinned)
lint:
	$(GO) run $(STATICCHECK) ./...

## vet: go vet
vet:
	$(GO) vet ./...

## vuln: govulncheck
vuln:
	$(GO) run $(GOVULNCHECK) ./...

## check: vet + lint + tests
check: vet lint test

## fmt: gofmt every package
fmt:
	$(GO) fmt ./...

## run: build and run with a demo configuration (mock notifier, no network needed)
run: build
	AMQP_URL="$${AMQP_URL:-amqp://guest:guest@localhost:5672/}" \
	NOTIFIER="$${NOTIFIER:-mock}" \
	MAIL_FROM="$${MAIL_FROM:-notifier@example.org}" \
	MAIL_TO="$${MAIL_TO:-admin@example.org}" \
	./$(BIN)

## demo: the full stack (broker + Mailpit + notifier); then: go run ./tools/publish
demo:
	docker compose -f deploy/compose.yaml up --build -d
	@echo "RabbitMQ  http://localhost:15672 (guest/guest)"
	@echo "Mailpit   http://localhost:8025"
	@echo "publish:  $(GO) run ./tools/publish"

## demo-down: stop the demo stack and remove its volumes
demo-down:
	docker compose -f deploy/compose.yaml down -v

## docker: build the container image
docker:
	docker build -f deploy/Dockerfile -t vm-notifier:$(VERSION) .

## fixtures: regenerate testdata/notifications (deterministic)
fixtures:
	$(GO) run tools/fixtures/main.go

## clean: remove build output and caches (the module cache is read-only, hence the chmod)
clean:
	chmod -R u+w .cache 2>/dev/null || true
	rm -rf bin .cache

## help: list targets
help:
	@grep -hE '^## ' $(MAKEFILE_LIST) | sed 's/^## //' | sort | awk -F': ' '{printf "  %-18s %s\n", $$1, $$2}'
