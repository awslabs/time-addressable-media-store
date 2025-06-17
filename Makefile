SHELL := /bin/bash

.PHONY: help api-spec fetch-tams-repo clean test test-unit test-functional test-acceptance test-all lint format cfn-lint cfn-nag cfn-format build deploy package validate local-api local-invoke

TAMS_REPO_PATH=https://github.com/bbc/tams
STACK_NAME ?= tams
OUTPUT_DIR ?= .aws-sam

ifndef TAMS_TAG
TAMS_SPEC_ZIP_URL := $(TAMS_REPO_PATH)/archive/refs/heads/main.zip
TAMS_SPEC_ZIP_FILE := api/build/tams-repo-main.zip
TAMS_SPEC_ZIP_PATH := tams-main
else
TAMS_SPEC_ZIP_URL := $(TAMS_REPO_PATH)/archive/refs/tags/$(TAMS_TAG).zip
TAMS_SPEC_ZIP_FILE := api/build/tams-repo-$(TAMS_TAG).zip
TAMS_SPEC_ZIP_PATH := tams-$(TAMS_TAG)
endif

# Default target
.DEFAULT_GOAL := help

help:
	@echo "Available targets:"
	@echo "  api-spec           - Generate API schema from TAMS repository"
	@echo "  fetch-tams-repo    - Fetch TAMS repository as a zip file"
	@echo "  build              - Build the SAM application"
	@echo "  package            - Package the SAM application"
	@echo "  deploy             - Deploy the SAM application (use STACK_NAME)"
	@echo "  validate           - Validate the SAM template"
	@echo "  local-api          - Run API Gateway locally"
	@echo "  local-invoke       - Invoke a Lambda function locally"
	@echo "  test-unit          - Run unit tests"
	@echo "  test-functional    - Run functional tests"
	@echo "  test-acceptance    - Run acceptance tests"
	@echo "  test-all           - Run all tests"
	@echo "  test               - Alias for test-all"
	@echo "  lint               - Run all linting checks"
	@echo "  format             - Format code"
	@echo "  cfn-lint           - Run CloudFormation linting"
	@echo "  cfn-nag            - Run CloudFormation security checks"
	@echo "  cfn-format         - Format CloudFormation templates"
	@echo "  clean              - Clean build artifacts"
	@echo ""
	@echo "Options:"
	@echo "  TAMS_TAG           - Specify a tag for api-spec (e.g., make api-spec TAMS_TAG=v1.0.0)"
	@echo "  STACK_NAME         - CloudFormation stack name (default: tams)"
	@echo "  FUNCTION_NAME      - Lambda function name for local-invoke"
	@echo "  EVENT_FILE         - Event file for local-invoke"

api/build:
	mkdir -p api/build

api/build/tams-repo-%.zip: | api/build
	wget -O "$@" "$(TAMS_SPEC_ZIP_URL)"

api/build/tams: $(TAMS_SPEC_ZIP_FILE) | api/build
	unzip -o "$(TAMS_SPEC_ZIP_FILE)" "$(TAMS_SPEC_ZIP_PATH)/api/*" -d api/build
	@# Delete spec directory if it already exists, so it can be replaced in the following step
	if [ -d "$@" ]; then \
		rm -rf "$@"; \
	fi
	mv "api/build/$(TAMS_SPEC_ZIP_PATH)" "$@"

api-spec: api/build/tams
	@echo "Generating API schema..."
	poetry run python ./api/build/generate_spec.py
	poetry run datamodel-codegen --input ./api/build/openapi.yaml --input-file-type openapi --output ./layers/utils/schema.py --output-model-type pydantic_v2.BaseModel --target-python-version 3.13 --use-schema-description --use-double-quotes
	rm -rf ./api/build/tams

build: api-spec
	@echo "Building SAM application..."
	sam build --use-container

package: build
	@echo "Packaging SAM application..."
	sam package --output-template-file packaged.yaml --s3-bucket "$(BUCKET_NAME)"

deploy: build
	@echo "Deploying SAM application..."
	sam deploy --stack-name "$(STACK_NAME)" --capabilities CAPABILITY_IAM --guided

validate:
	@echo "Validating SAM template..."
	sam validate

local-api: build
	@echo "Starting local API Gateway..."
	sam local start-api

local-invoke: build
	@if [ -z "$(FUNCTION_NAME)" ]; then \
		echo "Error: FUNCTION_NAME is required"; \
		exit 1; \
	fi
	@if [ -z "$(EVENT_FILE)" ]; then \
		echo "Error: EVENT_FILE is required"; \
		exit 1; \
	fi
	@echo "Invoking $(FUNCTION_NAME) locally..."
	sam local invoke "$(FUNCTION_NAME)" -e "$(EVENT_FILE)"

test-unit:
	@echo "Running unit tests..."
	poetry run pytest tests/unit -v

test-functional:
	@echo "Running functional tests..."
	poetry run pytest tests/functional -v

test-acceptance:
	@echo "Running acceptance tests..."
	poetry run pytest tests/acceptance -v

test-all: test-unit test-functional test-acceptance
	@echo "All tests completed"

test: test-all

cfn-lint:
	@echo "Running CloudFormation linting..."
	cfn-lint template.yaml ./templates/*.yaml

cfn-nag:
	@echo "Running CloudFormation security checks..."
	cfn_nag_scan --input-path template.yaml
	cfn_nag_scan --input-path ./templates

cfn-format:
	@echo "Formatting CloudFormation templates..."
	rain fmt template.yaml
	find ./templates -name "*.yaml" -exec rain fmt {} \;

format:
	@echo "Formatting code..."
	poetry run black .
	poetry run isort --profile black .

lint-pylint:
	@echo "Running pylint..."
	poetry run pylint --errors-only --disable=E0401 functions/ layers/

lint-bandit:
	@echo "Running bandit..."
	poetry run bandit -c pyproject.toml -r .

lint: cfn-lint cfn-nag lint-pylint lint-bandit
	@echo "Running Python linting checks..."

clean:
	@echo "Cleaning build artifacts..."
	rm -rf \
		"$(OUTPUT_DIR)" \
		.aws-sam \
		packaged.yaml \
		./api/build/tams \
		./api/build/tams-repo-*.zip \
		./api/build/openapi.yaml \
		;
	find . \
	\( -type d \
		\( -name "__pycache__" -o -name ".pytest_cache" \) -exec rm -rf {} + \) \
	-o \
		\( -type f -name "*.pyc" -delete \) \
	;
