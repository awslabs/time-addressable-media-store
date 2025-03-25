#!/usr/bin/env python
import json
import re
import sys
from collections import OrderedDict, defaultdict

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


def get_event_name_suffix(value):
    """Simple function to create valid Cloudformation Serverless event name"""
    dash_path = re.sub(r"[{}_]", "", value).replace("/", "-").strip("-")
    if dash_path == "":
        return "Root"
    return "".join(v.title() for v in dash_path.split("-"))


def add_securitySchemes(spec):
    spec["components"]["securitySchemes"] = OrderedDict(
        {
            "Authorizor": {
                "type": "apiKey",
                "name": "Authorization",
                "in": "header",
                "x-amazon-apigateway-authtype": "cognito_user_pools",
                "x-amazon-apigateway-authorizer": {
                    "type": "cognito_user_pools",
                    "providerARNs": [
                        {
                            "Fn::Sub": "arn:${AWS::Partition}:cognito-idp:${AWS::Region}:${AWS::AccountId}:userpool/${UserPool}"
                        }
                    ],
                },
            }
        }
    )


def parse_query_parameters(spec):
    # Loop path methods and add API Gateway integration
    query_params = defaultdict(lambda: defaultdict(dict))
    for path in spec["paths"]:
        for method in spec["paths"][path]:
            if method != "parameters":
                # Parse Parameters out into separate dict
                if "parameters" in spec["paths"][path][method]:
                    for parameter in spec["paths"][path][method]["parameters"]:
                        if "$ref" in parameter:
                            parameter = spec_lookup(
                                spec, parameter["$ref"].split("/")[1:]
                            )
                        if parameter["in"] == "query":
                            if "$ref" in parameter["schema"]:
                                parameter["schema"] = spec_lookup(
                                    spec,
                                    parameter["schema"]["$ref"].split("/")[1:],
                                )
                            query_params[path][method.upper()][parameter["name"]] = (
                                type_map[parameter["schema"]["type"]]
                            )
    return query_params


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


def set_root_get_mock(spec):
    spec["paths"]["/"]["get"]["x-amazon-apigateway-integration"] = OrderedDict(
        {
            "type": "mock",
            "requestTemplates": {"application/json": '{\n  "statusCode": 200\n}\n'},
            "responses": {
                "default": OrderedDict(
                    {
                        "statusCode": "200",
                        "responseTemplates": {
                            "application/json": '[\n  "service",\n  "flows",\n  "sources",\n  "flow-delete-requests"\n]\n'
                        },
                    }
                )
            },
            "passthroughBehavior": "when_no_templates",
        }
    )


def set_root_any_mock(spec):
    spec["paths"]["/"]["x-amazon-apigateway-any-method"] = OrderedDict(
        {
            "responses": {
                "404": {"description": "The specified path or method does not exist."}
            },
            "security": [
                {
                    "Authorizor": [
                        {"Fn::FindInMap": ["OAuth", "Scopes", "CognitoAdmin"]},
                        {"Fn::FindInMap": ["OAuth", "Scopes", "Head"]},
                        {"Fn::FindInMap": ["OAuth", "Scopes", "Get"]},
                        {"Fn::FindInMap": ["OAuth", "Scopes", "Put"]},
                        {"Fn::FindInMap": ["OAuth", "Scopes", "Post"]},
                        {"Fn::FindInMap": ["OAuth", "Scopes", "Delete"]},
                    ]
                }
            ],
            "x-amazon-apigateway-integration": OrderedDict(
                {
                    "type": "mock",
                    "requestTemplates": {"application/json": '{"statusCode": 404}'},
                    "responses": {
                        "default": OrderedDict(
                            {
                                "statusCode": "404",
                                "responseTemplates": {"application/json": {}},
                            }
                        )
                    },
                    "passthroughBehavior": "when_no_templates",
                }
            ),
        }
    )


def set_proxy_any_mock(spec):
    spec["paths"]["/{proxy+}"] = OrderedDict(
        {
            "x-amazon-apigateway-any-method": {
                "parameters": [
                    OrderedDict(
                        {
                            "name": "proxy",
                            "in": "path",
                            "required": True,
                            "schema": {"type": "string"},
                        }
                    )
                ],
                "responses": {
                    "404": {
                        "description": "The specified path or method does not exist."
                    }
                },
                "security": [
                    {
                        "Authorizor": [
                            {"Fn::FindInMap": ["OAuth", "Scopes", "CognitoAdmin"]},
                            {"Fn::FindInMap": ["OAuth", "Scopes", "Head"]},
                            {"Fn::FindInMap": ["OAuth", "Scopes", "Get"]},
                            {"Fn::FindInMap": ["OAuth", "Scopes", "Put"]},
                            {"Fn::FindInMap": ["OAuth", "Scopes", "Post"]},
                            {"Fn::FindInMap": ["OAuth", "Scopes", "Delete"]},
                        ]
                    }
                ],
                "x-amazon-apigateway-integration": OrderedDict(
                    {
                        "type": "mock",
                        "requestTemplates": {"application/json": '{"statusCode": 404}'},
                        "responses": {
                            "default": OrderedDict(
                                {
                                    "statusCode": "404",
                                    "responseTemplates": {"application/json": {}},
                                }
                            )
                        },
                        "passthroughBehavior": "when_no_templates",
                    }
                ),
            }
        }
    )


def main():
    # Read the spec from YAML file into OrderedDict to preserve easy to read structure when written back.
    with open(
        f"{sys.path[0]}/tams/api/TimeAddressableMediaStore.yaml", "r", encoding="utf-8"
    ) as f:
        openapi_spec = ordered_load(
            f.read(), Loader=yaml.SafeLoader, object_pairs_hook=OrderedDict
        )
    # Update spec to be 3.0.1 (source is 3.1.0 and not supported by API Gateway)
    openapi_spec["openapi"] = "3.0.1"
    openapi_spec["info"].pop("contact")
    openapi_spec["info"].pop("license")
    # Remove sections that are not needed for API Gateway
    for key in ["webhooks", "servers", "security"]:
        openapi_spec.pop(key)
    add_securitySchemes(openapi_spec)

    # Embed external schemas into the spec
    replacements = process_openapi_spec(openapi_spec)
    # Keep processing until no external schemas are found
    while replacements > 0:
        replacements = process_openapi_spec(openapi_spec)

    # Loop path methods and add API Gateway integration
    for path in openapi_spec["paths"]:
        for method in openapi_spec["paths"][path]:
            if method != "parameters":
                openapi_spec["paths"][path][method]["security"] = [
                    {
                        "Authorizor": [
                            {"Fn::FindInMap": ["OAuth", "Scopes", "CognitoAdmin"]},
                            {"Fn::FindInMap": ["OAuth", "Scopes", method.capitalize()]},
                        ]
                    }
                ]
                function_resource = (
                    f'{path.split("/")[1].title().replace("-", "")}Function'
                )
                if path == "/":
                    function_resource = "ServiceFunction"
                elif "/segments" in path:
                    function_resource = "FlowSegmentsFunction"
                if (
                    method == "head"
                    and len(openapi_spec["paths"][path][method]["responses"].keys())
                    == 1
                ):
                    openapi_spec["paths"][path][method][
                        "x-amazon-apigateway-integration"
                    ] = OrderedDict(
                        {
                            "type": "mock",
                            "requestTemplates": {
                                "application/json": '{"statusCode": 200}'
                            },
                            "responses": {
                                "default": OrderedDict(
                                    {
                                        "statusCode": "200",
                                        "responseTemplates": {"application/json": {}},
                                    }
                                )
                            },
                            "passthroughBehavior": "when_no_templates",
                        }
                    )
                else:
                    openapi_spec["paths"][path][method][
                        "x-amazon-apigateway-integration"
                    ] = OrderedDict(
                        {
                            "type": "aws_proxy",
                            "httpMethod": "POST",
                            "uri": {
                                "Fn::Sub": f"arn:${{AWS::Partition}}:apigateway:${{AWS::Region}}:lambda:path/2015-03-31/functions/${{{function_resource}.Arn}}/invocations"
                            },
                            "passthroughBehavior": "when_no_templates",
                        }
                    )
                # event_name = method + get_event_name_suffix(path)

                if "requestBody" in openapi_spec["paths"][path][method]:
                    if "content" in openapi_spec["paths"][path][method]["requestBody"]:
                        if (
                            "application/json"
                            in openapi_spec["paths"][path][method]["requestBody"][
                                "content"
                            ]
                        ):
                            if (
                                "example"
                                in openapi_spec["paths"][path][method]["requestBody"][
                                    "content"
                                ]["application/json"]
                            ):
                                openapi_spec["paths"][path][method]["requestBody"][
                                    "content"
                                ]["application/json"].pop("example")
                            if (
                                "examples"
                                in openapi_spec["paths"][path][method]["requestBody"][
                                    "content"
                                ]["application/json"]
                            ):
                                openapi_spec["paths"][path][method]["requestBody"][
                                    "content"
                                ]["application/json"].pop("examples")

                for code, response in openapi_spec["paths"][path][method][
                    "responses"
                ].items():
                    if "content" in response:
                        if (
                            "application/json"
                            in openapi_spec["paths"][path][method]["responses"][code][
                                "content"
                            ]
                        ):
                            if (
                                "example"
                                in openapi_spec["paths"][path][method]["responses"][
                                    code
                                ]["content"]["application/json"]
                            ):
                                openapi_spec["paths"][path][method]["responses"][code][
                                    "content"
                                ]["application/json"].pop("example")
                            if (
                                "examples"
                                in openapi_spec["paths"][path][method]["responses"][
                                    code
                                ]["content"]["application/json"]
                            ):
                                openapi_spec["paths"][path][method]["responses"][code][
                                    "content"
                                ]["application/json"].pop("examples")

    set_root_get_mock(openapi_spec)
    set_root_any_mock(openapi_spec)
    set_proxy_any_mock(openapi_spec)

    # Convert final spec to string for final manipulation
    spec_yaml_string = yaml.dump(openapi_spec, width=1000, default_flow_style=False)
    # Add comments to remove variable names from parameters as this is not supported by API Gateway
    spec_yaml_string = spec_yaml_string.replace(".{name}", ".name #{name}")

    # Write completed API Spec out
    with open("./api/openapi.yaml", "w", encoding="utf-8") as f:
        f.write(spec_yaml_string)

    query_params = parse_query_parameters(openapi_spec)
    essence_params = parse_essence_parameters(openapi_spec)

    with open("./layers/utils/params.py", "w", encoding="utf-8") as fw:
        fw.write(f"query_params = {json.dumps(query_params, indent=4)}\n\n")
        fw.write(f"essence_params = {json.dumps(essence_params, indent=4)}\n")


if __name__ == "__main__":
    main()
