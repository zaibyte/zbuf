GOVER_MAJOR := $(shell go version | sed -E -e "s/.*go([0-9]+)[.]([0-9]+).*/\1/")
GOVER_MINOR := $(shell go version | sed -E -e "s/.*go([0-9]+)[.]([0-9]+).*/\2/")
GO115 := $(shell [ $(GOVER_MAJOR) -gt 1 ] || [ $(GOVER_MAJOR) -eq 1 ] && [ $(GOVER_MINOR) -ge 15 ]; echo $$?)
ifeq ($(GO115), 1)
    $(error "go below 1.15 does not support")
endif

ZAI_PKG := g.tesamc.com/IT/zaipkg

LDFLAGS += -X "$(ZAI_PKG)/version.ReleaseVersion=$(shell git describe --tags --dirty)"
LDFLAGS += -X "$(ZAI_PKG)/version.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(ZAI_PKG)/version.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     uniq | sed -e "s/^\./g.tesamc.com\/IT\/zbuf/")

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
