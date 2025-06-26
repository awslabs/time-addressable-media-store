import base64
import json
import os
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Type

import boto3

# pylint: disable=no-member
import constants
from aws_lambda_powertools import Tracer
from aws_lambda_powertools.event_handler.exceptions import BadRequestError
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import And, Attr, Key
from botocore.exceptions import ClientError
from mediatimestamp.immutable import TimeRange
from neptune import (
    enhance_resources,
    merge_delete_request,
    update_flow_segments_updated,
)
from utils import pop_outliers, publish_event, put_message, put_message_batches

tracer = Tracer()

dynamodb = boto3.resource("dynamodb")
service_table = dynamodb.Table(os.environ.get("SERVICE_TABLE", ""))
segments_table = dynamodb.Table(os.environ.get("SEGMENTS_TABLE", ""))
storage_table = dynamodb.Table(os.environ.get("STORAGE_TABLE", ""))
webhooks_table = dynamodb.Table(os.environ.get("WEBHOOKS_TABLE", ""))


class TimeRangeBoundary(Enum):
    START = "start"
    END = "end"


@tracer.capture_method(capture_response=False)
def delete_segment_items(items: list[dict], object_ids: set[str]) -> dict | None:
    """Loop supplied items and delete, early return on error, append to object_ids supplied on success"""
    delete_error = None
    for item in items:
        key = {
            "flow_id": item["flow_id"],
            "timerange_end": item["timerange_end"],
        }
        try:
            delete_item = segments_table.delete_item(
                Key=key,
                ReturnValues="ALL_OLD",
            )
            if "Attributes" in delete_item:
                object_ids.add(item["object_id"])
                publish_event(
                    "flows/segments_deleted",
                    {"flow_id": item["flow_id"], "timerange": item["timerange"]},
                    enhance_resources([f'tams:flow:{item["flow_id"]}']),
                )
        except ClientError as e:
            delete_error = {
                "type": e.response["Error"]["Code"],
                "summary": e.response["Error"]["Message"],
                "traceback": [
                    f"Delete Segment Key: {json.dumps(key, default=str)}",
                    json.dumps(e.response["ResponseMetadata"], default=str),
                ],
                "time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            break
    return delete_error


@tracer.capture_method(capture_response=False)
def get_flow_timerange(flow_id: str) -> str:
    """Get the timerange for a specified flow"""
    first_segment = segments_table.query(
        KeyConditionExpression=Key("flow_id").eq(flow_id),
        Limit=1,
        ScanIndexForward=True,
        Select="SPECIFIC_ATTRIBUTES",
        ProjectionExpression="timerange",
    )["Items"]
    last_segment = segments_table.query(
        KeyConditionExpression=Key("flow_id").eq(flow_id),
        Limit=1,
        ScanIndexForward=False,
        Select="SPECIFIC_ATTRIBUTES",
        ProjectionExpression="timerange",
    )["Items"]
    if len(first_segment) > 0 and len(last_segment) > 0:
        return str(
            TimeRange.from_str(
                first_segment[0]["timerange"]
            ).extend_to_encompass_timerange(
                TimeRange.from_str(last_segment[0]["timerange"])
            )
        )
    if len(first_segment) > 0:
        return first_segment[0]["timerange"]
    if len(last_segment) > 0:
        return last_segment[0]["timerange"]
    return "()"


@tracer.capture_method(capture_response=False)
def get_timerange_expression(
    expression_type: Type[Attr] | Type[Key],
    boundary: TimeRangeBoundary,
    filter_value: TimeRange,
):
    """Returns a DynamoDB expression, for Key or Filter, to add the specfied timerange condition."""
    other_boundary = (
        TimeRangeBoundary.START
        if boundary == TimeRangeBoundary.END
        else TimeRangeBoundary.END
    )
    include_method = getattr(filter_value, f"includes_{other_boundary.value}")
    nanosec_value = getattr(filter_value, other_boundary.value).to_nanosec()
    match boundary:
        case TimeRangeBoundary.START:
            if include_method():
                return expression_type(f"timerange_{boundary.value}").lte(nanosec_value)
            else:
                return expression_type(f"timerange_{boundary.value}").lt(nanosec_value)
        case TimeRangeBoundary.END:
            if include_method():
                return expression_type(f"timerange_{boundary.value}").gte(nanosec_value)
            else:
                return expression_type(f"timerange_{boundary.value}").gt(nanosec_value)


@tracer.capture_method(capture_response=False)
def get_key_and_args(flow_id: str, parameters: dict) -> dict:
    """Generate key expression and args for a dynamodb query operation"""
    args = {
        "KeyConditionExpression": Key("flow_id").eq(flow_id),
        "ScanIndexForward": not parameters.get("reverse_order", False),
        "Limit": constants.DEFAULT_PAGE_LIMIT,
    }
    # Pagination query string parameters
    if parameters.get("limit"):
        args["Limit"] = min(parameters["limit"], constants.MAX_PAGE_LIMIT)
    if parameters.get("page"):
        args["ExclusiveStartKey"] = {
            "flow_id": flow_id,
            "timerange_end": int(parameters["page"]),
        }
    # Parse timerange filter out of parameters
    timerange_filter = (
        TimeRange.from_str(parameters["timerange"])
        if parameters.get("timerange")
        else None
    )
    # Ignore timerange filter if it is eternity
    if timerange_filter == TimeRange.eternity():
        timerange_filter = None
    # Update Key Expression
    if parameters.get("object_id"):
        args["IndexName"] = "object-id-index"
        args["KeyConditionExpression"] = And(
            args["KeyConditionExpression"],
            Key("object_id").eq(parameters["object_id"]),
        )
    elif timerange_filter:
        if timerange_filter.start:
            args["KeyConditionExpression"] = And(
                args["KeyConditionExpression"],
                get_timerange_expression(Key, TimeRangeBoundary.END, timerange_filter),
            )
        else:
            # Get the end of the first overlapping segment and use that to set the key filter when a start timerange is not specified.
            exact_timerange_end = get_exact_timerange_end(
                flow_id, timerange_filter.end.to_nanosec()
            )
            args["KeyConditionExpression"] = And(
                args["KeyConditionExpression"],
                Key("timerange_end").lte(exact_timerange_end),
            )
    # Build Filter expression
    if timerange_filter:
        if timerange_filter.start and parameters.get("object_id"):
            args["FilterExpression"] = get_timerange_expression(
                Attr, TimeRangeBoundary.END, timerange_filter
            )
        if timerange_filter.start and timerange_filter.end:
            args["FilterExpression"] = get_timerange_expression(
                Attr, TimeRangeBoundary.START, timerange_filter
            )
    return args


@tracer.capture_method(capture_response=False)
def delete_flow_segments(
    flow_id: str,
    parameters: None | dict,
    timerange_to_delete: TimeRange,
    context: LambdaContext,
    s3_queue: str,
    del_queue: str,
    item_dict: dict | None = None,
):
    """Performs the logic to delete flow segments exits gracefully if within 5 seconds of Lambda timeout"""
    delete_error = None
    args = get_key_and_args(flow_id, parameters)
    args["Limit"] = constants.DELETE_BATCH_SIZE
    query = segments_table.query(**args)
    object_ids = set()
    # Pop first and/or last item in array if they are not entirely covered by the deletion timerange
    query["Items"] = pop_outliers(timerange_to_delete, query["Items"])
    if len(query["Items"]) > 0:
        delete_error = delete_segment_items(
            query["Items"],
            object_ids,
        )
        update_flow_segments_updated(flow_id)
    # Continue with deletes if no errors, more records available and more than specified milliseconds remain of runtime
    while (
        delete_error is None
        and "LastEvaluatedKey" in query
        and context.get_remaining_time_in_millis() > constants.LAMBDA_TIME_REMAINING
    ):
        args["ExclusiveStartKey"] = query["LastEvaluatedKey"]
        query = segments_table.query(**args)
        # Pop first and/or last item in array if they are not entirely covered by the deletion timerange
        query["Items"] = pop_outliers(timerange_to_delete, query["Items"])
        if len(query["Items"]) > 0:
            delete_error = delete_segment_items(
                query["Items"],
                object_ids,
            )
            update_flow_segments_updated(flow_id)
    # Add affected object_ids to the SQS queue for potential S3 cleanup
    if len(object_ids) > 0:
        put_message_batches(s3_queue, list(object_ids))
    if item_dict is None:
        # item_dict only None when called from object_id related segment delete. This method does not support delete requests
        return
    # Update DDB record with error if error encountered, no SQS publish to prevent further processing
    if delete_error:
        merge_delete_request(
            {
                **item_dict,
                "status": "error",
                "error": delete_error,
            }
        )
        return
    # Update DDB record with done, no SQS publish as no further processing required.
    if "LastEvaluatedKey" not in query:
        merge_delete_request(
            {
                **item_dict,
                "status": "done",
                "timerange_remaining": "()",
            }
        )
        return
    last_timerange = TimeRange.from_str(query["Items"][-1]["timerange"])
    timerange_remaining = timerange_to_delete.intersect_with(
        last_timerange.timerange_after()
    )
    item_dict["timerange_remaining"] = str(timerange_remaining)
    item_dict["updated"] = datetime.now().strftime(constants.DATETIME_FORMAT)
    put_message(del_queue, item_dict)
    merge_delete_request(item_dict)


@tracer.capture_method(capture_response=False)
def get_exact_timerange_end(flow_id: str, timerange_end: int) -> int:
    """Get the exact timerange end of a segment the overlaps with the specified timerange end value"""
    items = segments_table.query(
        KeyConditionExpression=And(
            Key("flow_id").eq(flow_id), Key("timerange_end").gte(timerange_end)
        ),
        Limit=1,
        ScanIndexForward=True,
        Select="SPECIFIC_ATTRIBUTES",
        ProjectionExpression="timerange_end",
    )
    if items["Items"]:
        if items["Items"][0]["timerange_end"] != timerange_end:
            return items["Items"][0]["timerange_end"]
    return timerange_end


@tracer.capture_method(capture_response=False)
def validate_object_id(object_id: str, flow_id: str) -> tuple[bool, list]:
    """Check supplied object_id can be used for the supplied flow_id, returns storage_ids if valid"""
    query = storage_table.query(KeyConditionExpression=Key("id").eq(object_id))
    items = query["Items"]
    if len(items) == 0:
        # No matching object_id found so must be invalid
        return False, []
    object_item = items[0]
    if object_item["flow_id"] == flow_id:
        if object_item.get("expire_at"):
            # expire_at exists so this is first time use and needs updating to prevent TTL deletion
            storage_table.update_item(
                Key={
                    "id": object_id,
                    "flow_id": flow_id,
                },
                AttributeUpdates={"expire_at": {"Action": "DELETE"}},
            )
        # flow_id matches so is a valida object_id
        return True, object_item.get("storage_ids", [])
    if object_item.get("expire_at") is None:
        # object_id already used therefore can be re-used by any flow_id
        return True, object_item.get("storage_ids", [])
    # All other options are invalid
    return False, []


@tracer.capture_method(capture_response=False)
def delete_flow_storage_record(object_id: str) -> None:
    """Delete the DDB record associated with the supplied object_id"""
    query = storage_table.query(KeyConditionExpression=Key("id").eq(object_id))
    for item in query["Items"]:
        storage_table.delete_item(
            Key={
                "id": item["id"],
                "flow_id": item["flow_id"],
            },
        )


@tracer.capture_method(capture_response=False)
def get_object_id_query_kwargs(object_id: str, parameters: dict) -> dict:
    """Generate key expression and args for a dynamodb query operation"""
    kwargs = {
        "IndexName": "object-id-index",
        "ProjectionExpression": "flow_id",
        "KeyConditionExpression": Key("object_id").eq(object_id),
        "Limit": constants.DEFAULT_PAGE_LIMIT,
    }
    # Pagination query string parameters
    if parameters.get("limit"):
        kwargs["Limit"] = min(parameters["limit"], constants.MAX_PAGE_LIMIT)
    if parameters.get("page"):
        try:
            decoded_page = base64.b64decode(parameters["page"]).decode("utf-8")
            exclusive_start_key = json.loads(decoded_page)
        except UnicodeDecodeError as ex:
            raise BadRequestError("Invalid page parameter value") from ex  # 400
        except json.decoder.JSONDecodeError as ex:
            raise BadRequestError("Invalid page parameter value") from ex  # 400
        if any(
            f not in exclusive_start_key
            for f in ["flow_id", "object_id", "timerange_end"]
        ):
            raise BadRequestError("Invalid page parameter value")  # 400
        if exclusive_start_key["object_id"] != object_id:
            raise BadRequestError("Invalid page parameter value")  # 400
        kwargs["ExclusiveStartKey"] = exclusive_start_key
    return kwargs


@lru_cache()
@tracer.capture_method(capture_response=False)
def get_store_name() -> str:
    get_item = service_table.get_item(Key={"record_type": "service", "id": "1"})
    if get_item["Item"].get("name") is None:
        return "tams"
    return get_item["Item"]["name"]


@tracer.capture_method(capture_response=False)
def get_storage_backend_dict(item, store_name) -> dict:
    return {
        **item,
        "storage_id": item["id"],
        "label": f'aws.{item["region"]}:s3:{store_name}',
    }


@lru_cache()
@tracer.capture_method(capture_response=False)
def get_default_storage_backend() -> dict:
    query = service_table.query(
        KeyConditionExpression=Key("record_type").eq("storage-backend"),
        FilterExpression=Attr("default_storage").eq(True),
    )
    items = query["Items"]
    if len(items) == 0:
        raise BadRequestError("No default storage backend found")  # 404
    return dict(get_storage_backend_dict(items[0], get_store_name()))


@lru_cache()
@tracer.capture_method(capture_response=False)
def get_storage_backend(storage_id: str) -> dict:
    get_item = service_table.get_item(
        Key={"record_type": "storage-backend", "id": storage_id}
    )
    if not get_item.get("Item"):
        raise BadRequestError("Invalid storage backend identifier")  # 404
    return dict(get_storage_backend_dict(get_item["Item"], get_store_name()))
