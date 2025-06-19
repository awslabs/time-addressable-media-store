#!/usr/bin/env python
import json
import re
import sys
from collections import OrderedDict

import yaml

type_map = {
    "string": "str",
    "integer": "int",
    "number": "float",
    "boolean": "bool",
    "array": "list",
    "object": "dict",
}


def spec_lookup(spec, keys):
    value = spec
    for k in keys:
        value = value[k]
    return value


def ordered_load(content, Loader=yaml.SafeLoader, object_pairs_hook=OrderedDict):
    """Function to read YAML string into an OrderedDict"""

    class OrderedLoader(Loader):
        pass

    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))

    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping
    )
    return yaml.load(content, OrderedLoader)  # nosec B506


def str_presenter(dumper, data):
    """Function to ensure yaml | symbols are used for multi line strings when dumped"""
    if len(data.split("\n")) > 1:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


# Add representers for str and OrderedDict to yaml
yaml.add_representer(str, str_presenter)
yaml.add_representer(
    OrderedDict,
    lambda dumper, data: dumper.represent_mapping(
        "tag:yaml.org,2002:map", data.items()
    ),
)


def process_openapi_spec(spec):
    """A function to find and replace external schema reference to embedded schema components, returns the number of replacements made so thatb it can be called until complete"""
    external_schemas = set()

    def replace_external_ref(d):
        for k, v in d.copy().items():
            if k == "$ref" and v.startswith("schemas/") and v.endswith(".json"):
                schema_name = re.search(
                    r"schemas\/(?P<schema>.*)\.json", v
                ).groupdict()["schema"]
                safe_name = re.sub(r"[\W]+", "", schema_name.lower())
                external_schemas.add((v, safe_name))
                d[k] = f"#/components/schemas/{safe_name}"
            elif isinstance(v, dict):
                replace_external_ref(v)
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        replace_external_ref(item)

    replace_external_ref(spec)
    for schema in sorted(external_schemas):
        with open(f"{sys.path[0]}/tams/api/{schema[0]}", "r", encoding="utf-8") as sf:
            raw_content = sf.read()
            raw_content = re.sub(
                r"\"\$ref\": \"(?P<file>.*\.json)\"",
                '"$ref": "schemas/\\1"',
                raw_content,
            )
            schema_dict = json.loads(raw_content, object_pairs_hook=OrderedDict)
        spec["components"]["schemas"][schema[1]] = schema_dict
    return len(external_schemas)


def parse_essence_parameters(spec):
    essence_params = {}
    for component in spec["components"]:
        for schema in spec["components"][component]:
            if "allOf" in spec["components"][component][schema]:
                for item in spec["components"][component][schema]["allOf"]:
                    if "properties" in item:
                        if "essence_parameters" in item["properties"]:
                            for prop in item["properties"]["essence_parameters"][
                                "properties"
                            ]:
                                essence_params[prop] = type_map[
                                    item["properties"]["essence_parameters"][
                                        "properties"
                                    ][prop]["type"]
                                ]
    return essence_params


def main():
    # Read the spec from YAML file into OrderedDict to preserve easy to read structure when written back.
    with open(
        f"{sys.path[0]}/tams/api/TimeAddressableMediaStore.yaml", "r", encoding="utf-8"
    ) as f:
        openapi_spec = ordered_load(
            f.read(), Loader=yaml.SafeLoader, object_pairs_hook=OrderedDict
        )
    # Embed external schemas into the spec
    replacements = process_openapi_spec(openapi_spec)
    # Keep processing until no external schemas are found
    while replacements > 0:
        replacements = process_openapi_spec(openapi_spec)

    # Write completed API Spec out
    spec_yaml_string = yaml.dump(openapi_spec, width=1000, default_flow_style=False)
    with open("./api/build/openapi.yaml", "w", encoding="utf-8") as f:
        f.write(spec_yaml_string)

    essence_params = parse_essence_parameters(openapi_spec)

    with open("./layers/utils/params.py", "w", encoding="utf-8") as fw:
        fw.write(
            f"essence_params = {json.dumps(essence_params, indent=4, sort_keys=True)}\n"
        )


if __name__ == "__main__":
    main()
