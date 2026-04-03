ZAI_PKG := github.com/zaibyte/zaipkg

LDFLAGS += -X "$(ZAI_PKG)/version.ReleaseVersion=$(shell git describe --tags --dirty)"
LDFLAGS += -X "$(ZAI_PKG)/version.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(ZAI_PKG)/version.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     uniq | sed -e "s/^\./github.com\/zaibyte\/zbuf/")

all: test build

build:
	go build -ldflags '$(LDFLAGS)' -o bin/zbuf cmd/zbuf-server/main.go
	go build -o bin/extperf tools/extperf/main.go
test:
    # testing...
	go test -tags memfs_test -race -cover $(TEST_PKGS)

tidy:
	@echo "go mod tidy"
	go mod tidy
	git diff --quiet

clean:
	rm -rf bin/*

.PHONY: all tidy clean
