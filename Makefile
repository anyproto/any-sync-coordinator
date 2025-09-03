.PHONY: build test deps build-dev
SHELL=/bin/bash
export GOPRIVATE=github.com/anyproto
export PATH:=$(CURDIR)/deps:$(PATH)
export CGO_ENABLED:=1
BUILD_GOOS:=$(shell go env GOOS)
BUILD_GOARCH:=$(shell go env GOARCH)

ifeq ($(CGO_ENABLED), 0)
	TAGS:=-tags nographviz
else
	TAGS:=
endif

build:
	@$(eval FLAGS := $$(shell PATH=$(PATH) govvv -flags -pkg github.com/anyproto/any-sync/app))
	GOOS=$(BUILD_GOOS) GOARCH=$(BUILD_GOARCH) go build $(TAGS) -v -o bin/any-sync-coordinator -ldflags "$(FLAGS) -X github.com/anyproto/any-sync/app.AppName=any-sync-coordinator" github.com/anyproto/any-sync-coordinator/cmd/coordinator
	GOOS=$(BUILD_GOOS) GOARCH=$(BUILD_GOARCH) go build $(TAGS) -v -o bin/any-sync-confapply -ldflags "$(FLAGS)" github.com/anyproto/any-sync-coordinator/cmd/confapply

test:
	go test ./... --cover $(TAGS)

deps:
	go mod download
	go build -o deps github.com/ahmetb/govvv
	go build -o deps go.uber.org/mock/mockgen

mocks:
	echo 'Generating mocks...'
	go generate ./...

