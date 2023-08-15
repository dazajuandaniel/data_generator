#!/usr/bin/env make
include ./resources/Makehelp
include ./resources/bootstrap.mk
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

## Creates Adventure Works Database
db_create:
	@ PGPASSWORD=mysecretpassword psql -U $(PGUSER) -p $(PGPORT) -d $(PGDATABASE) -h $(PGHOST) -c 'CREATE DATABASE $(ADVENTUREWORKSNAME)'
.PHONY: db_create

## Creates Adventure Works Database Tables & Schemas & Seeds Data
db_bootstrap:
	@ PGPASSWORD=mysecretpassword psql -U $(PGUSER) -p $(PGPORT) -d $(ADVENTUREWORKSNAME) -h $(PGHOST) -f sql/install.sql
.PHONY: db_bootstrap

## Generates Data for Table
db_generate:
	@ cd src && python generate_data.py $(table_name) $(num_rows)
.PHONY: db_generate

## Create header
_header:
	@ printf "${GREEN}$$HEADER\n${RESET}"