import json
import os
from http import HTTPStatus
from typing import List, Optional, Union

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
from aws_lambda_powertools.event_handler.openapi.exceptions import (
    RequestValidationError,
)
from aws_lambda_powertools.event_handler.openapi.params import Body, Path, Query
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
    validate_object_id,
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
from schema import Deletionrequest, Flowsegment, Flowsegmentpost, Timerange, Uuid
from segment_get_urls import populate_get_urls
from typing_extensions import Annotated
from utils import (
    base_delete_request_dict,
    generate_failed_segment,
    generate_link_url,
    model_dump,
    publish_event,
    put_message,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(
    enable_validation=True, cors=CORSConfig(expose_headers=["*"])
)
metrics = Metrics()
s3_queue = os.environ["S3_QUEUE_URL"]
del_queue = os.environ["DELETE_QUEUE_URL"]

UUID_PATTERN = Uuid.model_fields["root"].metadata[0].pattern
TIMERANGE_PATTERN = Timerange.model_fields["root"].metadata[0].pattern


@app.head("/flows/<flowId>/segments")
@app.get("/flows/<flowId>/segments")
@tracer.capture_method(capture_response=False)
def get_flow_segments_by_id(
    flow_id: Annotated[
        str, Path(alias="flowId")
    ],  # There is a special case here where 404 is defined for an invalid (bad format) flowId
    param_object_id: Annotated[Optional[str], Query(alias="object_id")] = None,
    param_timerange: Annotated[
        Optional[str], Query(alias="timerange", pattern=TIMERANGE_PATTERN)
    ] = None,
    param_reverse_order: Annotated[Optional[bool], Query(alias="reverse_order")] = None,
    param_verbose_storage: Annotated[
        Optional[bool], Query(alias="verbose_storage")
    ] = None,
    param_accept_get_urls: Annotated[
        Optional[str], Query(alias="accept_get_urls", pattern=r"^([^,]+(,[^,]+)*)?$")
    ] = None,
    param_accept_storage_ids: Annotated[
        Optional[str],
        Query(
            alias="accept_storage_ids",
            pattern=r"^([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})(,[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})*$",
        ),
    ] = None,
    param_presigned: Annotated[Optional[bool], Query(alias="presigned")] = None,
    param_page: Annotated[Optional[str], Query(alias="page")] = None,
    param_limit: Annotated[Optional[int], Query(alias="limit", gt=0)] = None,
):
    try:
        Uuid(root=flow_id)
    except ValidationError as ex:
        raise NotFoundError("The flow ID in the path is invalid.") from ex  # 404
    if not check_node_exists("flow", flow_id):
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            content_type=content_types.APPLICATION_JSON,
            body=json.dumps([]),
        )
    args = get_key_and_args(
        flow_id,
        {
            "reverse_order": param_reverse_order,
            "limit": param_limit,
            "page": param_page,
            "timerange": param_timerange,
            "object_id": param_object_id,
        },
    )
    reverse_order = not args["ScanIndexForward"]
    query = segments_table.query(**args)
    items = query["Items"]
    custom_headers = {}
    while "LastEvaluatedKey" in query and len(items) < args["Limit"]:
        args["ExclusiveStartKey"] = query["LastEvaluatedKey"]
        query = segments_table.query(**args)
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
    if "LastEvaluatedKey" in query or param_limit != args["Limit"]:
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
    populate_get_urls(
        items,
        param_accept_get_urls,
        param_verbose_storage,
        param_accept_storage_ids,
        param_presigned,
    )
    schema_items = [Flowsegment(**item) for item in items]
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=model_dump(schema_items),
        headers=custom_headers,
    )


@app.post("/flows/<flowId>/segments")
@tracer.capture_method(capture_response=False)
def post_flow_segments_by_id(
    flow_segment: Annotated[Union[Flowsegmentpost, List[Flowsegmentpost]], Body()],
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node("flow", flow_id)
    except ValueError as e:
        raise NotFoundError("The flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    if not item.get("container"):
        raise BadRequestError("Bad request. The flow 'container' is not set.")  # 400
    if isinstance(flow_segment, List):
        failed_segments = []
        for segment in flow_segment:
            segment_result = process_single_segment(item, segment)
            if segment_result:
                failed_segments.append(segment_result)
        if len(failed_segments) == 0:
            return None, HTTPStatus.CREATED.value  # 201
        else:
            return model_dump(failed_segments), HTTPStatus.OK.value  # 200
    # Handle single segment requests separately to retain backwards compatibility with previous version of API where list of segments not available.
    else:
        segment_result = process_single_segment(item, flow_segment)
        if segment_result:
            raise BadRequestError(segment_result.error.summary)  # 400
        else:
            return None, HTTPStatus.CREATED.value  # 201


@app.delete("/flows/<flowId>/segments")
@tracer.capture_method(capture_response=False)
def delete_flow_segments_by_id(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
    param_timerange: Annotated[
        Optional[str], Query(alias="timerange", pattern=TIMERANGE_PATTERN)
    ] = None,
    param_object_id: Annotated[Optional[str], Query(alias="object_id")] = None,
):
    try:
        item = query_node("flow", flow_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    timerange_to_delete = TimeRange.from_str(get_flow_timerange(flow_id))
    if param_timerange:
        timerange_to_delete = TimeRange.from_str(param_timerange)
    deletion_request_dict = None
    if param_object_id:
        # Not able to handle return of Delete Request as delete requests do not support "object_id" query parameter
        delete_flow_segments(
            flow_id,
            {
                "timerange": param_timerange,
                "object_id": param_object_id,
            },
            timerange_to_delete,
            app.lambda_context,
            s3_queue,
            del_queue,
            None,
        )
        return None, HTTPStatus.NO_CONTENT.value  # 204
    deletion_request_dict = {
        **base_delete_request_dict(flow_id, app.current_event.request_context),
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


@app.exception_handler(RequestValidationError)
def handle_validation_error(ex: RequestValidationError):
    raise BadRequestError(ex.errors())  # 400


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
        args["ExclusiveStartKey"] = query["LastEvaluatedKey"]
        query = segments_table.query(**args)
        items.extend(query["Items"])
    return len(items) > 0


@tracer.capture_method(capture_response=False)
def process_single_segment(flow: dict, flow_segment: Flowsegmentpost) -> None:
    """Process a single flow segment POST request"""
    item_dict = model_dump(flow_segment)
    valid_object_id, storage_id, validation_message = validate_object_id(
        flow_segment, flow["id"]
    )
    if not valid_object_id:
        return generate_failed_segment(
            flow_segment.object_id,
            item_dict["timerange"],
            validation_message,
        )
    segment_timerange = TimeRange.from_str(item_dict["timerange"])
    if check_overlapping_segments(flow["id"], segment_timerange):
        return generate_failed_segment(
            flow_segment.object_id,
            item_dict["timerange"],
            "Bad request. The timerange of the segment MUST NOT overlap any other segment in the same Flow.",
        )
    item_dict["timerange_start"] = segment_timerange.start.to_nanosec() + (
        0 if segment_timerange.includes_start() else 1
    )
    item_dict["timerange_end"] = segment_timerange.end.to_nanosec() - (
        0 if segment_timerange.includes_end() else 1
    )
    if storage_id:
        item_dict["storage_ids"] = [storage_id]
    segments_table.put_item(
        Item={**item_dict, "flow_id": flow["id"]}, ReturnValues="ALL_OLD"
    )
    update_flow_segments_updated(flow["id"])
    publish_event(
        "flows/segments_added",
        {"flow_id": flow["id"], "segments": [item_dict]},
        enhance_resources(
            [
                f'tams:flow:{flow["id"]}',
                f'tams:source:{flow["source_id"]}',
                *set(
                    f"tams:flow-collected-by:{c_id}"
                    for c_id in flow.get("collected_by", [])
                ),
            ]
        ),
    )
