import concurrent.futures
import json
import os
from http import HTTPStatus

import boto3
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
from boto3.dynamodb.conditions import And, Attr, Key
from mediatimestamp.immutable import TimeRange
from pydantic import ValidationError
from schema import Deletionrequest, Flow, Flowsegment, Uuid
from utils import (
    base_delete_request_dict,
    delete_flow_segments,
    generate_link_url,
    generate_presigned_url,
    get_clean_item,
    get_flow_timerange,
    get_key_and_args,
    json_number,
    publish_event,
    put_deletion_request,
    update_flow_segments_updated,
    validate_query_string,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(enable_validation=True, cors=CORSConfig())
metrics = Metrics(namespace="Powertools")


dynamodb = boto3.resource("dynamodb")
segments_table = dynamodb.Table(os.environ["SEGMENTS_TABLE"])
table = dynamodb.Table(os.environ["TABLE"])
bucket = os.environ["BUCKET"]
bucket_region = os.environ["BUCKET_REGION"]
s3_queue = os.environ["S3_QUEUE_URL"]
del_queue = os.environ["DELETE_QUEUE_URL"]


@app.route("/flows/<flowId>/segments", method=["HEAD"])
@app.get("/flows/<flowId>/segments")
@tracer.capture_method(capture_response=False)
def get_flow_segments_by_id(flowId: str):
    parameters = app.current_event.query_string_parameters
    if not validate_query_string(parameters, app.current_event.request_context):
        raise BadRequestError("Bad request. Invalid query options.")  # 400
    try:
        parse(event={"__root__": flowId}, model=Uuid)
    except ValidationError as ex:
        raise NotFoundError("The flow ID in the path is invalid.") from ex  # 404
    item = table.get_item(
        Key={"record_type": "flow", "id": flowId},
        ProjectionExpression="id",
    )
    if "Item" not in item:
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            content_type=content_types.APPLICATION_JSON,
            body=json.dumps([]),
        )
    valid_parameters = [
        "limit",
        # "object_id",  # Handled as a special case
        "page",
        # "reverse_order",  # Handled as a special case
        "timerange",
    ]
    args, reverse_order = get_key_and_args(flowId, parameters, valid_parameters)
    query = segments_table.query(**args)
    items = query["Items"]
    custom_headers = {}
    while "LastEvaluatedKey" in query and len(items) < args["Limit"]:
        query = segments_table.query(
            **args, ExclusiveStartKey=query["LastEvaluatedKey"]
        )
        items.extend(query["Items"])
    if "LastEvaluatedKey" in query:
        if len(items) == 0:
            custom_headers["X-Paging-Timerange"] = "()"
        elif len(items) == 1:
            custom_headers["X-Paging-Timerange"] = items[0]["timerange"]
        else:
            custom_headers["X-Paging-Timerange"] = str(
                TimeRange.from_str(items[0]["timerange"]).extend_to_encompass_timerange(
                    TimeRange.from_str(items[-1]["timerange"])
                )
            )
        custom_headers["X-Paging-NextKey"] = str(
            query["LastEvaluatedKey"]["timerange_end"]
        )
        custom_headers["Link"] = generate_link_url(
            app.current_event, str(query["LastEvaluatedKey"]["timerange_end"])
        )
    # Set Paging Limit header if paging limit being used is not the one specified
    if (
        parameters
        and "limit" in parameters
        and parameters["limit"] != str(args["Limit"])
    ):
        custom_headers["X-Paging-Limit"] = str(args["Limit"])
    custom_headers["X-Paging-Count"] = str(len(items))
    custom_headers["X-Paging-Reverse-Order"] = str(reverse_order)
    if app.current_event.request_context.http_method == "HEAD":
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            content_type=content_types.APPLICATION_JSON,
            body=None,
            headers=custom_headers,
        )
    schema_items = [
        get_clean_item(parse(event=item, model=Flowsegment)) for item in items
    ]
    presigned_urls = generate_urls_parallel(
        set(item["object_id"] for item in schema_items)
    )
    # Add url to items
    stage_variables = app.current_event.stage_variables
    for item in schema_items:
        item["get_urls"] = [
            *item.get("get_urls", []),
            {
                "label": f"aws.{bucket_region}:s3.presigned:{stage_variables.get("name", "example-store-name")}",
                "url": presigned_urls[item["object_id"]],
            },
        ]
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=json.dumps(schema_items, default=json_number),
        headers=custom_headers,
    )


@app.post("/flows/<flowId>/segments")
@tracer.capture_method(capture_response=False)
def post_flow_segments_by_id(flow_segment: Flowsegment, flowId: str):
    item = table.get_item(Key={"record_type": "flow", "id": flowId})
    if "Item" not in item:
        raise NotFoundError("The flow does not exist.")  # 404
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
    item_dict = get_clean_item(flow_segment)
    segment_timerange = TimeRange.from_str(item_dict["timerange"])
    if check_overlapping_segments(flowId, segment_timerange):
        raise BadRequestError(
            "Bad request. The timerange of the segment MUST NOT overlap any other segment in the same Flow."
        )  # 400
    item_dict["timerange_start"] = segment_timerange.start.to_nanosec() + (
        0 if segment_timerange.includes_start() else 1
    )
    item_dict["timerange_end"] = segment_timerange.end.to_nanosec() - (
        0 if segment_timerange.includes_end() else 1
    )
    put_item = segments_table.put_item(
        Item={**item_dict, "flow_id": flowId}, ReturnValues="ALL_OLD"
    )
    update_flow_segments_updated(flowId, table)
    # Determine return code
    if "Attributes" in put_item:
        return None, HTTPStatus.NO_CONTENT.value  # 204
    publish_event(
        "flows/segments_added",
        {"flow_id": flowId, "timerange": item_dict["timerange"]},
        [flowId],
    )
    return get_clean_item(flow_segment), HTTPStatus.CREATED.value  # 201


@app.delete("/flows/<flowId>/segments")
@tracer.capture_method(capture_response=False)
def delete_flow_segments_by_id(flowId: str):
    parameters = app.current_event.query_string_parameters
    if not validate_query_string(parameters, app.current_event.request_context):
        raise BadRequestError("Bad request. Invalid query options.")  # 400
    item = table.get_item(Key={"record_type": "flow", "id": flowId})
    if "Item" not in item:
        raise NotFoundError("The requested flow ID in the path is invalid.")  # 404
    flow: Flow = parse(event=item["Item"], model=Flow)
    if flow.__root__.read_only:
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    timerange_to_delete = TimeRange.from_str(get_flow_timerange(segments_table, flowId))
    if parameters and "timerange" in parameters:
        timerange_to_delete = TimeRange.from_str(parameters["timerange"])
    deletion_request_dict = None
    if parameters and "object_id" in parameters:
        valid_parameters = [
            # "object_id",  # Handled as a special case
            "timerange"
        ]
        # Not able to handle return of Delete Request as delete requests do not support "object_id" query parameter
        delete_flow_segments(
            table,
            segments_table,
            flowId,
            parameters,
            valid_parameters,
            timerange_to_delete,
            app.lambda_context,
            s3_queue,
            del_queue,
            None,
        )
        return None, HTTPStatus.NO_CONTENT.value  # 204
    deletion_request_dict = {
        **base_delete_request_dict(flowId, app.current_event.request_context),
        "delete_flow": False,
        "timerange_to_delete": str(timerange_to_delete),
        "timerange_remaining": str(timerange_to_delete),
    }
    put_deletion_request(del_queue, table, deletion_request_dict)
    if deletion_request_dict is not None:
        return Response(
            status_code=HTTPStatus.ACCEPTED.value,  # 202
            content_type=content_types.APPLICATION_JSON,
            body=json.dumps(
                get_clean_item(
                    parse(event=deletion_request_dict, model=Deletionrequest)
                )
            ),
            headers={
                "Location": f'https://{app.current_event.request_context.domain_name}{app.current_event.request_context.path.split("/flows/")[0]}/flow-delete-requests/{deletion_request_dict["id"]}'
            },
        )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@logger.inject_lambda_context(
    log_event=True, correlation_id_path=correlation_paths.API_GATEWAY_REST
)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)


@tracer.capture_method(capture_response=False)
def check_overlapping_segments(flow_id, segment_timerange):
    args = {
        "KeyConditionExpression": And(
            Key("flow_id").eq(flow_id),
            (
                Key("timerange_end").gte(segment_timerange.start.to_nanosec())
                if segment_timerange.includes_start()
                else Key("timerange_end").gt(segment_timerange.start.to_nanosec())
            ),
        ),
        "FilterExpression": (
            Attr("timerange_start").lte(segment_timerange.end.to_nanosec())
            if segment_timerange.includes_end()
            else Attr("timerange_start").lt(segment_timerange.end.to_nanosec())
        ),
    }
    query = segments_table.query(**args)
    items = query["Items"]
    while "LastEvaluatedKey" in query:
        query = segments_table.query(
            **args, ExclusiveStartKey=query["LastEvaluatedKey"]
        )
        items.extend(query["Items"])
    return len(items) > 0


@tracer.capture_method(capture_response=False)
def get_presigned_url(key):
    url = generate_presigned_url("get_object", bucket, key)
    return (key, url)


@tracer.capture_method(capture_response=False)
def generate_urls_parallel(keys):
    # Asynchronous call to pre-signed url API
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for object_id in keys:
            futures.append(executor.submit(get_presigned_url, object_id))
    # Build dict of returned urls
    urls = {}
    for future in futures:
        key, url = future.result()
        urls[key] = url
    return urls
