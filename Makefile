SHELL := /bin/bash

.PHONY: build-apispec


TAMS_REPO_PATH=https://github.com/bbc/tams

build-api-spec-main:
	wget -O ./api/build/tams-repo.zip $(TAMS_REPO_PATH)/archive/refs/heads/main.zip
	unzip ./api/build/tams-repo.zip tams-main/api/* -d api/build
	rm -f ./api/build/tams-repo.zip
	mv ./api/build/tams-main/ ./api/build/tams
#	rm -rf ./api/build/tams

build-api-spec-tag:
	wget -O ./api/build/tams-repo.zip $(TAMS_REPO_PATH)/archive/refs/tags/$(TAMS_TAG).zip
	unzip ./api/build/tams-repo.zip tams-$(TAMS_TAG)/api/* -d api/build
	rm -f ./api/build/tams-repo.zip
	mv ./api/build/tams-$(TAMS_TAG)/ ./api/build/tams
#	rm -rf ./api/build/tams

build-api-schema:
	python ./api/build/generate_spec.py
	datamodel-codegen --input ./api/openapi.yaml --input-file-type openapi --output ./layers/utils/schema.py --output-model-type pydantic_v2.BaseModel --target-python-version 3.13 --use-schema-description --use-double-quotes
