.PHONY: build run test fmt lint clean docker

APP      := via-realtime-gateway
CMD      := ./cmd/gateway
OUT      := ./bin/$(APP)
VERSION  ?= $(shell git describe --tags --always 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X main.version=$(VERSION)

## build: compile the gateway binary
build:
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o $(OUT) $(CMD)

## run: build and run locally
run: build
	$(OUT)

## test: run all tests
test:
	go test -race -count=1 ./...

## fmt: format all Go files
fmt:
	go fmt ./...
	goimports -w . 2>/dev/null || true

## lint: run static analysis
lint:
	golangci-lint run ./...

## clean: remove build artefacts
clean:
	rm -rf bin/ server

## docker: build production Docker image
docker:
	docker build -f ../deployment/docker/gateway.Dockerfile -t $(APP):$(VERSION) ..

## help: show this help
help:
	@grep -E '^## ' Makefile | sed 's/## //' | column -t -s ':'
