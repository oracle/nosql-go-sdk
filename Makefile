#
# Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

ifneq ($(shell which go > /dev/null 2>&1; echo $$?),0)
$(error cannot find `go` in PATH)
endif

GO=go
GIT=git
ZIP=zip
# The version should be consistent with that specified in nosqldb/internal/sdkutil/version.go
version ?= 1.2.0

ROOT := $(shell pwd)
BIN := $(ROOT)/bin
SRC := $(ROOT)/nosqldb
EXAMPLE_SRC := $(ROOT)/examples
EXAMPLE_BIN := $(BIN)/examples

testcases ?=
options ?=
examples := basic delete index

GOTEST := $(GO) test -v -run "$(testcases)" $(options)

.PHONY: all build test cloudsim-test onprem-test clean build-examples release $(examples) help

all: build

# compile all packages
build:
	cd $(SRC) && $(GO) build -v ./...

# run tests
test:
	cd $(SRC) && $(GOTEST) ./...

# run tests against cloudsim
cloudsim-test:
	cd $(SRC) && $(GOTEST) -tags "cloud" ./... -args testConfig=$(ROOT)/internal/test/cloudsim_config.json

# run tests against onpremise
onprem-test:
	cd $(SRC) && $(GOTEST) -tags "onprem" ./... -args testConfig=$(ROOT)/internal/test/onprem_config.json

# clean
clean:
	cd $(SRC) && $(GO) clean -v ./...

# compile examples
build-examples: $(examples)

$(examples): %: $(wildcard $(EXAMPLE_SRC)/%/*.go) | $(EXAMPLE_BIN)
	cd $(EXAMPLE_SRC)/$* && $(GO) build -v -o $(EXAMPLE_BIN)/$@ .

$(EXAMPLE_BIN):
	mkdir -p $@

# package sources into a zip file
release:
	$(GIT) ls-tree --full-tree -r --name-only HEAD | $(ZIP) -r nosql-go-sdk-$(version).zip -@

help:
	@echo "Usages: make <target>"
	@echo ""
	@echo "Available targets are:"
	@echo ""
	@echo "build          : compile all packages"
	@echo "test           : run all tests"
	@echo "cloudsim-test  : run cloudsim tests"
	@echo "onprem-test    : run onprem tests"
	@echo "build-examples : compile examples"
	@echo "release        : package source codes into a zip file"
	@echo "help           : print help messages"
