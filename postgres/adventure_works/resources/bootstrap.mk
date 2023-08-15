#!/usr/bin/env make

.DEFAULT_GOAL := help

# COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RED    := $(shell tput -Txterm setaf 1)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

TARGET_MAX_CHAR_NUM := 20

# Define header
define HEADER
endef
export HEADER

# Required tools
REQUIRED_BINS := docker docker-compose make psql
EVAL := FALSE

#Open Docker, only if is not running
run_docker:
	@ $(if $(shell command docker stats --no-stream 2> /dev/null), echo "✅ Docker is running.", echo "❌ Please run Docker." && exit 1)
.PHONY: run_docker

# Required Env variables
REQUIRED_VARIABLES := PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD