import json
from collections import defaultdict

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


def parse_params(file):
    with open(file, "r", encoding="utf-8") as fr:
        openapi_spec = yaml.safe_load(fr.read())
    query = defaultdict(lambda: defaultdict(dict))
    for path in openapi_spec["paths"]:
        for method in openapi_spec["paths"][path]:
            if method != "parameters":
                if "parameters" in openapi_spec["paths"][path][method]:
                    for parameter in openapi_spec["paths"][path][method]["parameters"]:
                        if "$ref" in parameter:
                            parameter = spec_lookup(
                                openapi_spec, parameter["$ref"].split("/")[1:]
                            )
                        if parameter["in"] == "query":
                            if "$ref" in parameter["schema"]:
                                parameter["schema"] = spec_lookup(
                                    openapi_spec,
                                    parameter["schema"]["$ref"].split("/")[1:],
                                )
                            query[path][method.upper()][parameter["name"]] = type_map[
                                parameter["schema"]["type"]
                            ]
    essence = {}
    for component in openapi_spec["components"]:
        for schema in openapi_spec["components"][component]:
            if "allOf" in openapi_spec["components"][component][schema]:
                for item in openapi_spec["components"][component][schema]["allOf"]:
                    if "properties" in item:
                        if "essence_parameters" in item["properties"]:
                            for prop in item["properties"]["essence_parameters"][
                                "properties"
                            ]:
                                essence[prop] = type_map[
                                    item["properties"]["essence_parameters"][
                                        "properties"
                                    ][prop]["type"]
                                ]
    return query, essence


if __name__ == "__main__":
    query_params, essence_params = parse_params("openapi.yaml")
    with open("params.py", "w", encoding="utf-8") as fw:
        fw.write(f"query_params = {json.dumps(query_params, indent=4)}\n\n")
        fw.write(f"essence_params = {json.dumps(essence_params, indent=4)}\n")
