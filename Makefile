SHELL := /bin/bash
.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))
VERSION := $$(grep '^version' pyproject.toml | sed 's%version = "\(.*\)"%\1%')
APP_NAME := $$(grep '^name' pyproject.toml | sed 's%name = "\(.*\)"%\1%')

.DEFAULT_GOAL := help

version: ## display version and exit
	@echo $(VERSION)

dev: ## setup development environment
	./setup.sh

test: ## run unit tests
	@rye run pytest

lint: ## run linting and formatting tools
	@rye run ruff check src/$(APP_NAME)
	@rye run pydoclint --config=pyproject.toml src
	@rye run pydoclint --config=pyproject.toml --skip-checking-short-docstrings=true tests

check-types: ## run mypy and check types
	@rye run python -m mypy --install-types --non-interactive src/$(APP_NAME)

format: ## run formatting
	@rye run python -m ruff check --fix src/$(APP_NAME) tests

dep-check: ## check for outdated dependencies
	@rye run deptry . --known-first-party $(APP_NAME)

check: lint test check-types ## run all checks

help: ## This is help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)