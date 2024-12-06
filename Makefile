SHELL := /bin/bash
.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))
VERSION := $$(grep '^version' pyproject.toml | sed 's%version = "\(.*\)"%\1%')
APP_NAME := $$(grep '^name' pyproject.toml | head -1 | sed 's%name = "\(.*\)"%\1%')

.DEFAULT_GOAL := help

version: ## display version and exit
	@echo $(VERSION)

dev: ## setup development environment
	$(shell echo $$SHELL) ./setup.sh

test: ## run unit tests
	@echo "Running tests..."
	@rye run pytest

lint: ## run linting
	@echo "Running linting tools..."
	@rye run ruff check --fix --select I src/$(APP_NAME) tests
	@rye run pydoclint --config=pyproject.toml src tests
	@rye run interrogate -vv src/$(APP_NAME) tests

type-check: ## run mypy and check types
	@echo "Running type checks..."
	@rye run mypy --install-types --non-interactive src/$(APP_NAME)

format: ## run formatting
	@echo "Running formatting tools..."
	@rye run ruff format src/$(APP_NAME) tests

dep-check: ## check for outdated dependencies
	@echo "Running dependencies checks..."
	@rye run deptry . --known-first-party $(APP_NAME)

build: ## build distributions
	@echo "Building distributions..."
	@rye build

check: lint format test type-check dep-check ## run all checks

help: ## This is help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
