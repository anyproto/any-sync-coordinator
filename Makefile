.PHONY: build test deps build-dev
export GOPRIVATE=github.com/anytypeio
export PATH:=deps:$(PATH)

build:
	@$(eval FLAGS := $$(shell PATH=$(PATH) govvv -flags -pkg github.com/anytypeio/any-sync/app))
	go build -v -o bin/any-sync-coordinator -ldflags "$(FLAGS)" github.com/anytypeio/any-sync-coordinator/cmd

test:
	go test ./... --cover

deps:
	go mod download
	go build -o deps github.com/ahmetb/govvv
