build-UtilsLayer:
	mkdir -p "$(ARTIFACTS_DIR)/python"
	cp utils.py "$(ARTIFACTS_DIR)/python"
	cp constants.py "$(ARTIFACTS_DIR)/python"
	python -m pip install -r requirements.txt -t "$(ARTIFACTS_DIR)/python"
	python -m pip install datamodel-code-generator pyyaml
	datamodel-codegen --input openapi.yaml --input-file-type openapi --output "$(ARTIFACTS_DIR)/python/schema.py" --output-model-type pydantic_v2.BaseModel
	python build_params.py
	cp params.py "$(ARTIFACTS_DIR)/python"
