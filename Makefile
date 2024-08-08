SHELL := /bin/bash
.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))
VERSION := $$(grep '^version' pyproject.toml | sed 's%version = "\(.*\)"%\1%')

.DEFAULT_GOAL := help

version: ## display version and exit
	@echo $(VERSION)

dev: ## setup development environment
	./setup.sh

test: ## run unit tests
	@rye run pytest

lint: ## run linting and formatting tools
	@rye run ruff check src/gentroutils
	@rye run pydoclint --config=pyproject.toml src
	@rye run pydoclint --config=pyproject.toml --skip-checking-short-docstrings=true tests


help: ## This is help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)