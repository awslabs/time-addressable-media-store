import json
import os
import uuid
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
    ServiceError,
)
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.parser import parse
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from mediatimestamp.immutable import TimeRange
from schema import Deletionrequest, Flow, Flowstorage, Flowstoragepost, Source
from utils import (
    base_delete_request_dict,
    check_delete_source,
    generate_link_url,
    generate_presigned_url,
    get_clean_item,
    get_ddb_args,
    get_flow_timerange,
    get_model_by_id,
    get_username,
    json_number,
    publish_event,
    put_deletion_request,
    update_collected_by,
    update_flow_collection,
    validate_query_string,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(enable_validation=True, cors=CORSConfig())
metrics = Metrics(namespace="Powertools")

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE"])
segments_table = dynamodb.Table(os.environ["SEGMENTS_TABLE"])
record_type = "flow"
bucket = os.environ["BUCKET"]
del_queue = os.environ["DELETE_QUEUE_URL"]


@app.route("/flows", method=["HEAD"])
@app.get("/flows")
@tracer.capture_method(capture_response=False)
def get_flows():
    parameters = app.current_event.query_string_parameters
    if not validate_query_string(parameters, app.current_event.request_context):
        raise BadRequestError("Bad request. Invalid query options.")  # 400
    custom_headers = {}
    if parameters and "limit" in parameters:
        custom_headers["X-Paging-Limit"] = parameters["limit"]
    valid_parameters = [
        "codec",
        "format",
        "frame_height",
        "frame_width",
        "label",
        "limit",
        "page",
        "source_id",
        # "timerange",  # Handled as a special case below
    ]
    args = get_ddb_args(parameters, valid_parameters, True, record_type)
    query = table.query(
        KeyConditionExpression=Key("record_type").eq(record_type), **args
    )
    items = query["Items"]
    if parameters and "timerange" in parameters:
        timerange_filter = TimeRange.from_str(parameters["timerange"])
        if timerange_filter.is_empty():
            items = [
                item
                for item in items
                if TimeRange.from_str(
                    get_flow_timerange(segments_table, item["id"])
                ).is_empty()
            ]
        else:
            items = [
                item
                for item in items
                if not TimeRange.from_str(
                    get_flow_timerange(segments_table, item["id"])
                )
                .intersect_with(timerange_filter)
                .is_empty()
            ]
    while "LastEvaluatedKey" in query and len(items) < args["Limit"]:
        query = table.query(
            KeyConditionExpression=Key("record_type").eq(record_type),
            **args,
            ExclusiveStartKey=query["LastEvaluatedKey"],
        )
        if parameters and "timerange" in parameters:
            timerange_filter = TimeRange.from_str(parameters["timerange"])
            if timerange_filter.is_empty():
                items = [
                    item
                    for item in query["Items"]
                    if TimeRange.from_str(
                        get_flow_timerange(segments_table, item["id"])
                    ).is_empty()
                ]
            else:
                query["Items"] = [
                    item
                    for item in query["Items"]
                    if not TimeRange.from_str(
                        get_flow_timerange(segments_table, item["id"])
                    )
                    .intersect_with(timerange_filter)
                    .is_empty()
                ]
        items.extend(query["Items"])
    # Remove record_type and timerange field from results
    items = [
        {k: v for k, v in item.items() if k not in ["record_type"]} for item in items
    ]
    if "LastEvaluatedKey" in query:
        custom_headers["X-Paging-NextKey"] = query["LastEvaluatedKey"]["id"]
        custom_headers["Link"] = generate_link_url(
            app.current_event, query["LastEvaluatedKey"]["id"]
        )
    if app.current_event.request_context.http_method == "HEAD":
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            body=None,
            headers=custom_headers,
        )
    schema_items = [get_clean_item(parse(event=item, model=Flow)) for item in items]
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=json.dumps(schema_items, default=json_number),
        headers=custom_headers,
    )


@app.route("/flows/<flowId>", method=["HEAD"])
@app.get("/flows/<flowId>")
@tracer.capture_method(capture_response=False)
def get_flow_by_id(flowId: str):
    parameters = app.current_event.query_string_parameters
    if not validate_query_string(parameters, app.current_event.request_context):
        raise BadRequestError("Bad request. Invalid query options.")  # 400
    schema_item = None
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested flow does not exist.")  # 404
    if parameters and "include_timerange" in parameters:
        item["Item"]["timerange"] = get_flow_timerange(segments_table, flowId)
    # Update timerange if timerange parameter supplied
    if parameters and "timerange" in parameters and "include_timerange" in parameters:
        timerange_filter = TimeRange.from_str(parameters["timerange"])
        item["Item"]["timerange"] = str(
            timerange_filter.intersect_with(
                TimeRange.from_str(item["Item"]["timerange"])
            )
        )
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    schema_item = get_clean_item(parse(event=item["Item"], model=Flow))
    return json.dumps(schema_item, default=json_number), HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>")
@tracer.capture_method(capture_response=False)
def put_flow_by_id(flow: Flow, flowId: str):
    if flow.__root__.id != flowId:
        raise NotFoundError("The requested Flow ID in the path is invalid.")  # 404
    if flow.__root__.flow_collection:
        for collection in flow.__root__.flow_collection:
            collection_flow = get_model_by_id(table, record_type, collection.id)
            if collection_flow is None:
                raise BadRequestError(
                    "The supplied value for flow_collection references flowId(s) that do not exist"
                )  # 400
            update_collected_by(table, flowId, collection_flow, True)
    item = table.get_item(Key={"record_type": record_type, "id": flowId})
    existing_item = {}
    if "Item" in item:
        existing_item = item["Item"]
        if "read_only" in item["Item"] and item["Item"]["read_only"]:
            raise ServiceError(
                403,
                "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
            )  # 403
    request_item = get_clean_item(flow)
    # API spec states these fields should be ignored if given in a PUT request.
    for field in ["created", "metadata_updated", "collected_by"]:
        if field in request_item:
            del request_item[field]
    merged_item = {**existing_item, **request_item}
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    if "created" in merged_item:
        merged_item["metadata_updated"] = now
    else:
        merged_item["created"] = now
    # Set these if not supplied
    username = get_username(app.current_event.request_context)
    if "created_by" not in merged_item:
        merged_item["created_by"] = username
    if "updated_by" not in merged_item and existing_item != {}:
        merged_item["updated_by"] = username
    if not source_exists(merged_item["source_id"]):
        create_source(merged_item)
    put_item = table.put_item(Item={"record_type": record_type, **merged_item})
    publish_event(
        (
            f"{record_type}s/updated"
            if "Attributes" in put_item
            else f"{record_type}s/created"
        ),
        {record_type: merged_item},
        [flowId],
    )
    if existing_item != {}:
        return None, HTTPStatus.NO_CONTENT.value  # 204
    return json.dumps(merged_item), HTTPStatus.CREATED.value  # 201


@app.delete("/flows/<flowId>")
@tracer.capture_method(capture_response=False)
def delete_flow_by_id(flowId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId},
        ProjectionExpression="tags,read_only",
    )
    if "Item" not in item:
        raise NotFoundError("The requested Flow ID in the path is invalid.")  # 404
    if "read_only" in item["Item"] and item["Item"]["read_only"]:
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    # Get flow timerange, if timerange is empty delete flow sync, otherwise return a delete request
    flow_timerange = TimeRange.from_str(get_flow_timerange(segments_table, flowId))
    if flow_timerange.is_empty():
        delete_item = table.delete_item(
            Key={"record_type": record_type, "id": flowId},
            ReturnValues="ALL_OLD",
        )
        if "Attributes" in delete_item:
            publish_event(
                f"{record_type}s/deleted", {f"{record_type}_id": flowId}, [flowId]
            )
        flow: Flow = parse(event=delete_item["Attributes"], model=Flow)
        # Delete source if no longer referenced by any other flows
        check_delete_source(table, delete_item["Attributes"]["source_id"])
        # Update collections that either referenced this flow or were referenced by it
        if flow.__root__.flow_collection:
            for collection in flow.__root__.flow_collection:
                collection_flow = get_model_by_id(table, record_type, collection.id)
                update_collected_by(table, flowId, collection_flow, False)
        if flow.__root__.collected_by:
            for collected_by_id in flow.__root__.collected_by:
                update_flow_collection(table, flowId, collected_by_id)
        return None, HTTPStatus.NO_CONTENT.value  # 204
    # Create flow delete-request
    item_dict = {
        **base_delete_request_dict(flowId, app.current_event.request_context),
        "delete_flow": True,
        "timerange_to_delete": str(flow_timerange),
        "timerange_remaining": str(flow_timerange),
    }
    put_deletion_request(del_queue, table, item_dict)
    return Response(
        status_code=HTTPStatus.ACCEPTED.value,  # 202
        content_type=content_types.APPLICATION_JSON,
        body=json.dumps(get_clean_item(parse(event=item_dict, model=Deletionrequest))),
        headers={
            "Location": f'https://{app.current_event.request_context.domain_name}{app.current_event.request_context.path.split("/flows/")[0]}/flow-delete-requests/{item_dict["id"]}'
        },
    )


@app.route("/flows/<flowId>/tags", method=["HEAD"])
@app.get("/flows/<flowId>/tags")
@tracer.capture_method(capture_response=False)
def get_flow_tags(flowId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId}, ProjectionExpression="tags"
    )
    if "Item" not in item:
        raise NotFoundError("The requested flow does not exist.")  # 404
    if "tags" not in item["Item"]:
        return json.dumps([]), HTTPStatus.OK.value  # 200
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return json.dumps(item["Item"]["tags"]), HTTPStatus.OK.value  # 200


@app.route("/flows/<flowId>/tags/<name>", method=["HEAD"])
@app.get("/flows/<flowId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def get_flow_tag_value(flowId: str, name: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId}, ProjectionExpression="tags"
    )
    if (
        "Item" not in item
        or "tags" not in item["Item"]
        or name not in item["Item"]["tags"]
    ):
        raise NotFoundError("The requested flow or tag does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return item["Item"]["tags"][name], HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def put_flow_tag_value(flowId: str, name: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested flow does not exist.")  # 404
    flow: Flow = parse(event=item["Item"], model=Flow)
    if flow.__root__.read_only:
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid flow tag value.")  # 400
    if flow.__root__.tags is None:
        flow.__root__.tags = {name: body}
    else:
        flow.__root__.tags.__root__[name] = body
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    username = get_username(app.current_event.request_context)
    flow.__root__.metadata_updated = now
    flow.__root__.updated_by = username
    item_dict = get_clean_item(flow)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def delete_flow_tag_value(flowId: str, name: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested flow ID in the path is invalid.")  # 404
    flow: Flow = parse(event=item["Item"], model=Flow)
    if flow.__root__.read_only:
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    if flow.__root__.tags is None or name not in flow.__root__.tags.__root__:
        raise NotFoundError("The requested flow ID in the path is invalid.")  # 404
    del flow.__root__.tags.__root__[name]
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    username = get_username(app.current_event.request_context)
    flow.__root__.metadata_updated = now
    flow.__root__.updated_by = username
    item_dict = get_clean_item(flow)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.route("/flows/<flowId>/description", method=["HEAD"])
@app.get("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def get_flow_description(flowId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId},
        ProjectionExpression="description",
    )
    if "Item" not in item:
        raise NotFoundError("The requested flow does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    if "description" not in item["Item"]:
        return None, HTTPStatus.OK.value  # 200
    return item["Item"]["description"], HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def put_flow_description(flowId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested flow does not exist.")  # 404
    flow: Flow = parse(event=item["Item"], model=Flow)
    if flow.__root__.read_only:
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid flow description.")  # 400
    flow.__root__.description = body
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    username = get_username(app.current_event.request_context)
    flow.__root__.metadata_updated = now
    flow.__root__.updated_by = username
    item_dict = get_clean_item(flow)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def delete_flow_description(flowId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested flow ID in the path is invalid.")  # 404
    flow: Flow = parse(event=item["Item"], model=Flow)
    if flow.__root__.read_only:
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    flow.__root__.description = None
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    username = get_username(app.current_event.request_context)
    flow.__root__.metadata_updated = now
    flow.__root__.updated_by = username
    item_dict = get_clean_item(flow)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.route("/flows/<flowId>/label", method=["HEAD"])
@app.get("/flows/<flowId>/label")
@tracer.capture_method(capture_response=False)
def get_flow_label(flowId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId}, ProjectionExpression="label"
    )
    if "Item" not in item:
        raise NotFoundError(
            "The requested Flow does not exist, or does not have a label set."
        )  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    if "label" not in item["Item"]:
        return None, HTTPStatus.OK.value  # 200
    return item["Item"]["label"], HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/label")
@tracer.capture_method(capture_response=False)
def put_flow_label(flowId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested Flow does not exist.")  # 404
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid flow label.")  # 400
    flow: Flow = parse(event=item["Item"], model=Flow)
    flow.__root__.label = body
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    username = get_username(app.current_event.request_context)
    flow.__root__.metadata_updated = now
    flow.__root__.updated_by = username
    item_dict = get_clean_item(flow)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event("sources/updated", {"source": item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/label")
@tracer.capture_method(capture_response=False)
def delete_flow_label(flowId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested flow ID in the path is invalid.")  # 404
    flow: Flow = parse(event=item["Item"], model=Flow)
    flow.__root__.label = None
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    username = get_username(app.current_event.request_context)
    flow.__root__.metadata_updated = now
    flow.__root__.updated_by = username
    item_dict = get_clean_item(flow)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.route("/flows/<flowId>/read_only", method=["HEAD"])
@app.get("/flows/<flowId>/read_only")
@tracer.capture_method(capture_response=False)
def get_flow_read_only(flowId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId}, ProjectionExpression="read_only"
    )
    if "Item" not in item:
        raise NotFoundError("The requested flow does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return item["Item"].get("read_only", False), HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/read_only")
@tracer.capture_method(capture_response=False)
def put_flow_read_only(flowId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": flowId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested flow does not exist.")  # 404
    flow: Flow = parse(event=item["Item"], model=Flow)
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, bool):
        raise BadRequestError(
            "Bad request. Invalid flow read_only value. Value must be boolean."
        )  # 400
    flow.__root__.read_only = body
    item_dict = get_clean_item(flow)
    table.put_item(Item={"record_type": record_type, **item_dict})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.post("/flows/<flowId>/storage")
@tracer.capture_method(capture_response=False)
def post_flow_storage_by_id(flow_storage_post: Flowstoragepost, flowId: str):
    if flow_storage_post.limit is None:
        flow_storage_post.limit = constants.DEFAULT_PUT_LIMIT
    item = table.get_item(Key={"record_type": record_type, "id": flowId})
    if "Item" not in item:
        raise NotFoundError("The requested flow does not exist.")  # 404
    flow: Flow = parse(event=item["Item"], model=Flow)
    if flow.__root__.read_only:
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    if flow.__root__.container is None:
        raise BadRequestError(
            "Bad request. Invalid flow storage request JSON or the flow 'container' is not set."
        )  # 400
    response = parse(event={}, model=Flowstorage)
    response.media_objects = [
        get_presigned_put(flow.__root__.container)
        for _ in range(flow_storage_post.limit)
    ]
    return get_clean_item(response), HTTPStatus.CREATED.value  # 201


@logger.inject_lambda_context(
    log_event=True, correlation_id_path=correlation_paths.API_GATEWAY_REST
)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)


@tracer.capture_method(capture_response=False)
def source_exists(source_id):
    item = table.get_item(
        Key={"record_type": "source", "id": source_id}, ProjectionExpression="id"
    )
    return "Item" in item


@tracer.capture_method(capture_response=False)
def create_source(flow_dict):
    source: Source = parse(event=flow_dict, model=Source)
    source.id = flow_dict["source_id"]
    dict_item = get_clean_item(source)
    table.put_item(Item={"record_type": "source", **dict_item})
    publish_event("sources/created", {"source": dict_item}, [source.id])


@tracer.capture_method(capture_response=False)
def get_presigned_put(content_type):
    object_id = str(uuid.uuid4())
    url = generate_presigned_url(
        "put_object",
        bucket,
        object_id,
        {"ContentType": content_type},
    )
    return {
        "object_id": object_id,
        "put_url": {
            "url": url,
            "content-type": content_type,
        },
    }
