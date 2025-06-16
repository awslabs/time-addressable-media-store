SHELL := /bin/bash

.PHONY: help api-spec clean test test-unit test-functional test-acceptance test-all lint format cfn-lint cfn-nag cfn-format build deploy package validate local-api local-invoke

TAMS_REPO_PATH=https://github.com/bbc/tams
STACK_NAME ?= tams
AWS_REGION ?= eu-west-1
PROFILE ?= default
OUTPUT_DIR ?= .aws-sam

# Default target
.DEFAULT_GOAL := help

help:
	@echo "Available targets:"
	@echo "  api-spec           - Generate API schema from TAMS repository"
	@echo "  build              - Build the SAM application"
	@echo "  package            - Package the SAM application"
	@echo "  deploy             - Deploy the SAM application (use STACK_NAME, AWS_REGION, PROFILE)"
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
	@echo "  AWS_REGION         - AWS region (default: us-east-1)"
	@echo "  PROFILE            - AWS profile (default: default)"
	@echo "  FUNCTION_NAME      - Lambda function name for local-invoke"
	@echo "  EVENT_FILE         - Event file for local-invoke"

api-spec:
	@if [ -z "$(TAMS_TAG)" ]; then \
		echo "Downloading main branch..."; \
		wget -O ./api/build/tams-repo.zip $(TAMS_REPO_PATH)/archive/refs/heads/main.zip; \
		unzip ./api/build/tams-repo.zip tams-main/api/* -d api/build; \
		rm -f ./api/build/tams-repo.zip; \
		mv ./api/build/tams-main/ ./api/build/tams; \
	else \
		echo "Downloading tag $(TAMS_TAG)..."; \
		wget -O ./api/build/tams-repo.zip $(TAMS_REPO_PATH)/archive/refs/tags/$(TAMS_TAG).zip; \
		unzip ./api/build/tams-repo.zip tams-$(TAMS_TAG)/api/* -d api/build; \
		rm -f ./api/build/tams-repo.zip; \
		mv ./api/build/tams-$(TAMS_TAG)/ ./api/build/tams; \
	fi
	@echo "Generating API schema..."
	poetry run python ./api/build/generate_spec.py
	poetry run datamodel-codegen --input ./api/build/openapi.yaml --input-file-type openapi --output ./layers/utils/schema.py --output-model-type pydantic_v2.BaseModel --target-python-version 3.13 --use-schema-description --use-double-quotes
	rm -rf ./api/build/tams

build: api-spec
	@echo "Building SAM application..."
	sam build --use-container

package: build
	@echo "Packaging SAM application..."
	sam package --output-template-file packaged.yaml --s3-bucket $(BUCKET_NAME)

deploy: build
	@echo "Deploying SAM application..."
	sam deploy --stack-name $(STACK_NAME) --region $(AWS_REGION) --profile $(PROFILE) --capabilities CAPABILITY_IAM --guided

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
	sam local invoke $(FUNCTION_NAME) -e $(EVENT_FILE)

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
	cfn-lint *template.yaml

cfn-nag:
	@echo "Running CloudFormation security checks..."
	cfn_nag_scan --input-path template.yaml

cfn-format:
	@echo "Formatting CloudFormation templates..."
	rain fmt *template.yaml

format:
	@echo "Formatting code..."
	poetry run black .
	poetry run isort --profile black .

lint: cfn-lint cfn-nag
	@echo "Running Python linting checks..."
	poetry run pylint --errors-only --disable=E0401 functions/ layers/
	poetry run bandit -c pyproject.toml -r .

clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(OUTPUT_DIR)
	rm -rf .aws-sam
	rm -rf packaged.yaml
	rm -rf ./api/build/tams
	rm -f ./api/build/tams-repo.zip
	rm -f ./api/build/openapi.yaml
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
