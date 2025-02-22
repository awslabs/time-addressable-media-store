import concurrent.futures
import json
import os
from http import HTTPStatus

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
from aws_lambda_powertools.event_handler.openapi.params import Path
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import And, Attr, Key
from dynamodb import (
    TimeRangeBoundary,
    delete_flow_segments,
    get_flow_timerange,
    get_key_and_args,
    get_timerange_expression,
    segments_table,
)
from mediatimestamp.immutable import TimeRange
from neptune import (
    check_node_exists,
    enhance_resources,
    merge_delete_request,
    query_node,
    update_flow_segments_updated,
)
from pydantic import ValidationError
from schema import Deletionrequest, Flowsegment, GetUrl, Uuid
from typing_extensions import Annotated
from utils import (
    base_delete_request_dict,
    check_object_exists,
    generate_link_url,
    generate_presigned_url,
    model_dump,
    publish_event,
    put_message,
    validate_query_string,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(enable_validation=True, cors=CORSConfig())
metrics = Metrics(namespace="Powertools")
bucket = os.environ["BUCKET"]
bucket_region = os.environ["BUCKET_REGION"]
s3_queue = os.environ["S3_QUEUE_URL"]
del_queue = os.environ["DELETE_QUEUE_URL"]

UUID_PATTERN = Uuid.model_fields["root"].metadata[0].pattern


@app.head("/flows/<flowId>/segments")
@app.get("/flows/<flowId>/segments")
@tracer.capture_method(capture_response=False)
def get_flow_segments_by_id(
    flowId: str,
):  # There is a special case here where 404 is defined for an invalid (bad format) flowId
    parameters = app.current_event.query_string_parameters
    if not validate_query_string(parameters, app.current_event.request_context):
        raise BadRequestError("Bad request. Invalid query options.")  # 400
    try:
        Uuid(root=flowId)
    except ValidationError as ex:
        raise NotFoundError("The flow ID in the path is invalid.") from ex  # 404
    if not check_node_exists("flow", flowId):
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            content_type=content_types.APPLICATION_JSON,
            body=json.dumps([]),
        )
    args = get_key_and_args(flowId, parameters)
    reverse_order = not args["ScanIndexForward"]
    query = segments_table.query(**args)
    items = query["Items"]
    custom_headers = {}
    while "LastEvaluatedKey" in query and len(items) < args["Limit"]:
        query = segments_table.query(
            **args, ExclusiveStartKey=query["LastEvaluatedKey"]
        )
        items.extend(query["Items"])
    match len(items):
        case 0:
            custom_headers["X-Paging-Timerange"] = "()"
        case 1:
            custom_headers["X-Paging-Timerange"] = items[0]["timerange"]
        case _:
            custom_headers["X-Paging-Timerange"] = str(
                TimeRange.from_str(items[0]["timerange"]).extend_to_encompass_timerange(
                    TimeRange.from_str(items[-1]["timerange"])
                )
            )
    if "LastEvaluatedKey" in query:
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
    schema_items = [Flowsegment(**item) for item in items]
    accept_get_urls = parameters.get("accept_get_urls", None)
    filter_object_urls(schema_items, accept_get_urls)
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=model_dump(schema_items),
        headers=custom_headers,
    )


@app.post("/flows/<flowId>/segments")
@tracer.capture_method(capture_response=False)
def post_flow_segments_by_id(
    flow_segment: Flowsegment,
    flowId: Annotated[str, Path(pattern=UUID_PATTERN)],
):
    try:
        item = query_node("flow", flowId)
    except ValueError as e:
        raise NotFoundError("The flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    if not item.get("container"):
        raise BadRequestError(
            "Bad request. Invalid flow storage request JSON or the flow 'container' is not set."
        )  # 400
    if not check_object_exists(bucket, flow_segment.object_id):
        raise BadRequestError(
            "Bad request. The object id provided for a segment MUST exist."
        )  # 400
    item_dict = model_dump(flow_segment)
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
    segments_table.put_item(
        Item={**item_dict, "flow_id": flowId}, ReturnValues="ALL_OLD"
    )
    update_flow_segments_updated(flowId)
    schema_item = Flowsegment(**item_dict)
    get_url = get_nonsigned_url(schema_item.object_id)
    schema_item.get_urls = (
        schema_item.get_urls.append(get_url) if schema_item.get_urls else [get_url]
    )
    publish_event(
        "flows/segments_added",
        {"flow_id": flowId, "segments": [model_dump(schema_item)]},
        enhance_resources(
            [
                f"tams:flow:{flowId}",
                f'tams:source:{item["source_id"]}',
                *set(
                    f"tams:flow-collected-by:{c_id}"
                    for c_id in item.get("collected_by", [])
                ),
            ]
        ),
    )
    return model_dump(flow_segment), HTTPStatus.CREATED.value  # 201


@app.delete("/flows/<flowId>/segments")
@tracer.capture_method(capture_response=False)
def delete_flow_segments_by_id(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    parameters = app.current_event.query_string_parameters
    if not validate_query_string(parameters, app.current_event.request_context):
        raise BadRequestError("Bad request. Invalid query options.")  # 400
    try:
        item = query_node("flow", flowId)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    timerange_to_delete = TimeRange.from_str(get_flow_timerange(flowId))
    if parameters and "timerange" in parameters:
        timerange_to_delete = TimeRange.from_str(parameters["timerange"])
    deletion_request_dict = None
    if parameters and "object_id" in parameters:
        # Not able to handle return of Delete Request as delete requests do not support "object_id" query parameter
        delete_flow_segments(
            flowId,
            parameters,
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
    put_message(del_queue, deletion_request_dict)
    merge_delete_request(deletion_request_dict)
    if deletion_request_dict is not None:
        return Response(
            status_code=HTTPStatus.ACCEPTED.value,  # 202
            content_type=content_types.APPLICATION_JSON,
            body=model_dump(Deletionrequest(**deletion_request_dict)),
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
            get_timerange_expression(Key, TimeRangeBoundary.END, segment_timerange),
        ),
        "FilterExpression": get_timerange_expression(
            Attr, TimeRangeBoundary.START, segment_timerange
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
def get_nonsigned_url(key):
    return GetUrl(
        label=f'aws.{bucket_region}:s3:{app.current_event.stage_variables.get("name", "example-store-name")}',
        url=f"https://{bucket}.s3.{bucket_region}.amazonaws.com/{key}",
    )


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


def filter_object_urls(schema_items: list, accept_get_urls: str) -> None:
    """Filter the object urls in the schema items based on the accept_get_urls parameter."""
    presigned_urls = {}
    # Get presigned URLs only if required
    if accept_get_urls is None or ":s3.presigned:" in accept_get_urls:
        presigned_urls = generate_urls_parallel(
            set(item.object_id for item in schema_items)
        )
    # Add url to items
    stage_variables = app.current_event.stage_variables
    for item in schema_items:
        if accept_get_urls == "":
            item.get_urls = None
        else:
            get_url = get_nonsigned_url(item.object_id)
            item.get_urls = (
                item.get_urls.append(get_url) if item.get_urls else [get_url]
            )
            if item.object_id in presigned_urls:
                presigned_get_url = GetUrl(
                    label=f'aws.{bucket_region}:s3.presigned:{stage_variables.get("name", "example-store-name")}',
                    url=presigned_urls[item.object_id],
                )
                item.get_urls.append(presigned_get_url)
        if accept_get_urls:
            get_url_labels = [
                label for label in accept_get_urls.split(",") if label.strip()
            ]
            item.get_urls = [
                get_url for get_url in item.get_urls if get_url.label in get_url_labels
            ]
