import json
import os
from datetime import datetime
from enum import Enum
from typing import Type

import boto3

# pylint: disable=no-member
import constants
from aws_lambda_powertools import Tracer
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
segments_table = dynamodb.Table(os.environ["SEGMENTS_TABLE"])


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
def get_key_and_args(flow_id: str, parameters: None | dict) -> dict | bool:
    """Generate key expression and args for a dynamodb query operation"""
    args = {
        "KeyConditionExpression": Key("flow_id").eq(flow_id),
        "ScanIndexForward": True,
        "Limit": constants.DEFAULT_PAGE_LIMIT,
    }
    if parameters is None:
        return args
    reverse_order = parameters.get("reverse_order", "false").lower() == "true"
    args["ScanIndexForward"] = not reverse_order
    # Pagination query string parameters
    if "limit" in parameters:
        args["Limit"] = min(int(parameters["limit"]), constants.MAX_PAGE_LIMIT)
    if "page" in parameters:
        args["ExclusiveStartKey"] = {
            "flow_id": flow_id,
            "timerange_end": int(parameters["page"]),
        }
    # Parse timerange filter out of parameters
    timerange_filter = (
        TimeRange.from_str(parameters["timerange"])
        if "timerange" in parameters
        else None
    )
    # Ignore timerange filter if it is eternity
    if timerange_filter == TimeRange.eternity():
        timerange_filter = None
    # Update Key Expression
    if "object_id" in parameters:
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
        if timerange_filter.start and "object_id" in parameters:
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
