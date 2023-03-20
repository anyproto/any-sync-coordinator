.PHONY: build test deps build-dev
export GOPRIVATE=github.com/anytypeio
export PATH:=deps:$(PATH)
export CGO_ENABLED:=1
export GOOS:=linux
export GOARCH:=amd64

ifeq ($(CGO_ENABLED), 0)
	TAGS:=-tags nographviz
else
	TAGS:=
endif

build:
	@$(eval FLAGS := $$(shell PATH=$(PATH) govvv -flags -pkg github.com/anytypeio/any-sync/app))
	go build $(TAGS) -v -o bin/any-sync-coordinator -ldflags "$(FLAGS)" github.com/anytypeio/any-sync-coordinator/cmd

test:
	go test ./... --cover $(TAGS)

deps:
	go mod download
	go build -o deps github.com/ahmetb/govvv
