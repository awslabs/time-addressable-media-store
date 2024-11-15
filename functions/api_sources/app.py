import json
import os
from datetime import datetime
from http import HTTPStatus

import boto3
import constants
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.event_handler import (
    APIGatewayRestResolver,
    CORSConfig,
    Response,
    content_types,
)
from aws_lambda_powertools.event_handler.exceptions import (
    BadRequestError,
    NotFoundError,
)
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.parser import parse
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from schema import Source
from utils import (
    generate_link_url,
    get_clean_item,
    get_ddb_args,
    get_username,
    json_number,
    publish_event,
    validate_query_string,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(enable_validation=True, cors=CORSConfig())
metrics = Metrics(namespace="Powertools")

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE"])
record_type = "source"
event_bus = os.environ["EVENT_BUS"]


@app.route("/sources", method=["HEAD"])
@app.get("/sources")
@tracer.capture_method(capture_response=False)
def list_sources():
    parameters = app.current_event.query_string_parameters
    if not validate_query_string(parameters, app.current_event.request_context):
        raise BadRequestError("Bad request. Invalid query options.")  # 400
    custom_headers = {}
    if parameters and "limit" in parameters:
        custom_headers["X-Paging-Limit"] = parameters["limit"]
    valid_parameters = [
        "format",
        "label",
        "limit",
        "page",
    ]
    args = get_ddb_args(parameters, valid_parameters, True, record_type)
    query = table.query(
        KeyConditionExpression=Key("record_type").eq(record_type), **args
    )
    items = query["Items"]
    while "LastEvaluatedKey" in query and len(items) < args["Limit"]:
        query = table.query(
            KeyConditionExpression=Key("record_type").eq(record_type),
            **args,
            ExclusiveStartKey=query["LastEvaluatedKey"],
        )
        items.extend(query["Items"])
    if "LastEvaluatedKey" in query:
        custom_headers["X-Paging-NextKey"] = query["LastEvaluatedKey"]["id"]
        custom_headers["Link"] = generate_link_url(
            app.current_event, query["LastEvaluatedKey"]["id"]
        )
    if app.current_event.request_context.http_method == "HEAD":
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            content_type=content_types.APPLICATION_JSON,
            body=None,
            headers=custom_headers,
        )
    schema_items = [
        get_clean_item(
            parse(event={**item, **get_collections(item["id"])}, model=Source)
        )
        for item in items
    ]
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=json.dumps(schema_items, default=json_number),
        headers=custom_headers,
    )


@app.route("/sources/<sourceId>", method=["HEAD"])
@app.get("/sources/<sourceId>")
@tracer.capture_method(capture_response=False)
def get_source_details(sourceId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": sourceId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested Source does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    item["Item"] = {**item["Item"], **get_collections(sourceId)}
    source: Source = parse(event=item["Item"], model=Source)
    return get_clean_item(source), HTTPStatus.OK.value  # 200


@app.route("/sources/<sourceId>/tags", method=["HEAD"])
@app.get("/sources/<sourceId>/tags")
@tracer.capture_method(capture_response=False)
def get_source_tags(sourceId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": sourceId}, ProjectionExpression="tags"
    )
    if "Item" not in item:
        raise NotFoundError("The requested Source does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    if "tags" not in item["Item"]:
        return json.dumps({}), HTTPStatus.OK.value  # 200
    return json.dumps(item["Item"]["tags"]), HTTPStatus.OK.value  # 200


@app.route("/sources/<sourceId>/tags/<name>", method=["HEAD"])
@app.get("/sources/<sourceId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def get_source_tag_value(sourceId: str, name: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": sourceId}, ProjectionExpression="tags"
    )
    if (
        "Item" not in item
        or "tags" not in item["Item"]
        or name not in item["Item"]["tags"]
    ):
        raise NotFoundError("The requested Source or tag does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return item["Item"]["tags"][name], HTTPStatus.OK.value  # 200


@app.put("/sources/<sourceId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def put_source_tag_value(sourceId: str, name: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": sourceId},
    )
    if "Item" not in item:
        raise NotFoundError(
            "The requested Source does not exist, or the tag name in the path is invalid."
        )  # 404
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid Source tag value.")  # 400
    source: Source = parse(event=item["Item"], model=Source)
    if source.tags is None:
        source.tags = {name: body}
    else:
        source.tags.root[name] = body
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    username = get_username(app.current_event.request_context)
    source.updated = now
    source.updated_by = username
    item_dict = get_clean_item(source)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [sourceId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/sources/<sourceId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def delete_source_tag(sourceId: str, name: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": sourceId},
    )
    if "Item" not in item:
        raise NotFoundError(
            "The requested Source ID or tag in the path is invalid."
        )  # 404
    source: Source = parse(event=item["Item"], model=Source)
    if source.tags is None or name not in source.tags.root:
        raise NotFoundError(
            "The requested Source ID or tag in the path is invalid."
        )  # 404
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    source.updated = now
    del source.tags.root[name]
    item_dict = get_clean_item(source)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [sourceId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.route("/sources/<sourceId>/description", method=["HEAD"])
@app.get("/sources/<sourceId>/description")
@tracer.capture_method(capture_response=False)
def get_source_description(sourceId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": sourceId},
        ProjectionExpression="description",
    )
    if "Item" not in item:
        raise NotFoundError("The requested Source does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    if "description" not in item["Item"]:
        return None, HTTPStatus.OK.value  # 200
    return item["Item"]["description"], HTTPStatus.OK.value  # 200


@app.put("/sources/<sourceId>/description")
@tracer.capture_method(capture_response=False)
def put_source_description(sourceId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": sourceId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested Source does not exist.")  # 404
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid Source description.")  # 400
    source: Source = parse(event=item["Item"], model=Source)
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    username = get_username(app.current_event.request_context)
    source.updated = now
    source.updated_by = username
    source.description = body
    item_dict = get_clean_item(source)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [sourceId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/sources/<sourceId>/description")
@tracer.capture_method(capture_response=False)
def delete_source_description(sourceId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": sourceId},
    )
    if "Item" not in item:
        raise NotFoundError("The Source ID in the path is invalid.")  # 404
    source: Source = parse(event=item["Item"], model=Source)
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    source.updated = now
    source.description = None
    item_dict = get_clean_item(source)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [sourceId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.route("/sources/<sourceId>/label", method=["HEAD"])
@app.get("/sources/<sourceId>/label")
@tracer.capture_method(capture_response=False)
def get_source_label(sourceId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": sourceId}, ProjectionExpression="label"
    )
    if "Item" not in item:
        raise NotFoundError(
            "The requested Source does not exist, or does not have a label set."
        )  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    if "label" not in item["Item"]:
        return None, HTTPStatus.OK.value  # 200
    return item["Item"]["label"], HTTPStatus.OK.value  # 200


@app.put("/sources/<sourceId>/label")
@tracer.capture_method(capture_response=False)
def put_source_label(sourceId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": sourceId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested Source does not exist.")  # 404
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid Source label.")  # 400
    source: Source = parse(event=item["Item"], model=Source)
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    username = get_username(app.current_event.request_context)
    source.updated = now
    source.updated_by = username
    source.label = body
    item_dict = get_clean_item(source)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [sourceId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/sources/<sourceId>/label")
@tracer.capture_method(capture_response=False)
def delete_source_label(sourceId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": sourceId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested Source ID in the path is invalid.")  # 404
    source: Source = parse(event=item["Item"], model=Source)
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    source.updated = now
    source.label = None
    item_dict = get_clean_item(source)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [sourceId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@logger.inject_lambda_context(
    log_event=True, correlation_id_path=correlation_paths.API_GATEWAY_REST
)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)


@tracer.capture_method(capture_response=False)
def get_collections(source_id):
    query = table.query(
        IndexName=f"{record_type}-id-index",
        KeyConditionExpression=Key(f"{record_type}_id").eq(source_id),
    )
    source_collection = []
    collected_by = []
    if "Items" in query:
        for flow_details in query["Items"]:
            if "flow_collection" in flow_details:
                for flow in flow_details["flow_collection"]:
                    item = table.get_item(
                        Key={"record_type": "flow", "id": flow["id"]},
                        ProjectionExpression=f"{record_type}_id",
                    )
                    if "Item" in item:
                        source_collection.append(
                            {
                                "id": item["Item"][f"{record_type}_id"],
                                "role": flow["role"],
                            }
                        )
            if "collected_by" in flow_details:
                for flow_id in flow_details["collected_by"]:
                    item = table.get_item(
                        Key={"record_type": "flow", "id": flow_id},
                        ProjectionExpression=f"{record_type}_id",
                    )
                    if "Item" in item:
                        collected_by.append(item["Item"][f"{record_type}_id"])
    # De-dup lists before returning them
    return {
        f"{record_type}_collection": [
            {"id": x[0], "role": x[1]}
            for x in list(set((x["id"], x["role"]) for x in source_collection))
        ],
        "collected_by": list(set(collected_by)),
    }
