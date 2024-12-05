#!/usr/bin/env make
include ./makefile_resources/Makehelp
include ./makefile_resources/bootstrap.mk
include ./.env

# Targets

## Check required bins
check: _header
	$(foreach bin,$(REQUIRED_BINS),\
		$(if $(shell command -v $(bin) 2> /dev/null),$(info ✅ Found $(bin).),$(error ❌ Please install $(bin).)))
.PHONY: check

## Checks all the required env variables
env: check
	@ $(foreach variable,$(REQUIRED_VARIABLES), $(if $($(variable)),$(info ✅ $(variable) is set),$(error ❌ $(variable) is not set)))
.PHONY: env

## Create Python Virtual Environment, Activates it and Installs Python Packages
venv: 
	@ ( \
       python -m venv .venv; \
	   source .venv/bin/activate; \
       pip install -r requirements.txt; \
    )
.PHONY: venv

## Creates Adventure Works Database Tables & Schemas & Seeds Data in Postgres DB
postgres-bootstrap:
	@ ( \
		cd postgres/adventure_works; \
		PGPASSWORD=$(DATABASE_PASSWORD) psql -U $(DATABASE_USER) -p $(DATABASE_PORT) -d postgres -h $(DATABASE_HOST) -f sql/install.sql; \
	)
.PHONY: postgres_create

## Create header
_header:
	@ printf "${GREEN}$$HEADER\n${RESET}"