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
from mediatimestamp.immutable import TimeRange, Timestamp
from neptune import (
    enhance_resources,
    merge_delete_request,
    update_flow_segments_updated,
)
from schema import Flowsegmentpost
from utils import (
    calculate_object_timerange,
    pop_outliers,
    publish_event,
    put_message,
    put_message_batches,
)

tracer = Tracer()

dynamodb = boto3.resource("dynamodb")
service_table = dynamodb.Table(os.environ.get("SERVICE_TABLE", ""))
segments_table = dynamodb.Table(os.environ.get("SEGMENTS_TABLE", ""))
storage_table = dynamodb.Table(os.environ.get("STORAGE_TABLE", ""))


class TimeRangeBoundary(Enum):
    START = "start"
    END = "end"


@tracer.capture_method(capture_response=False)
def delete_segment_items(
    items: list[dict], object_ids: set[str], resources: list | None = None
) -> dict | None:
    """Loop supplied items and delete, early return on error, append to object_ids supplied on success.

    `resources` may hold the pre-resolved flows/segments_deleted event resources
    (source, collected-by, etc.). This is required when the Flow is being
    deleted: by the time its segments are removed the Flow (and its
    source/collection edges) no longer exists, so enhance_resources could not
    resolve them here - which would defeat source_ids / source_collected_by_ids
    / flow_collected_by_ids webhook filtering. When not supplied (segment-only
    deletion, Flow still exists) the resources are resolved live per flow_id.
    """
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
                object_ids.add((item["object_id"], tuple(item.get("storage_ids", []))))
                if item.get("init_object_id"):
                    object_ids.add(
                        (
                            item["init_object_id"],
                            tuple(item.get("init_storage_ids", [])),
                        )
                    )
                publish_event(
                    "flows/segments_deleted",
                    {"flow_id": item["flow_id"], "timerange": item["timerange"]},
                    (
                        resources
                        if resources is not None
                        else enhance_resources([f"tams:flow:{item['flow_id']}"])
                    ),
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
) -> Attr | Key:
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
        # Query the object-id-index, whose sort key is flow_id (not
        # timerange_end), so a timerange filter cannot be applied to the key and
        # is instead applied entirely as a FilterExpression below.
        args["IndexName"] = "object-id-index"
        args["KeyConditionExpression"] = And(
            args["KeyConditionExpression"],
            Key("object_id").eq(parameters["object_id"]),
        )
        filter_conditions = []
        if timerange_filter:
            if timerange_filter.start:
                filter_conditions.append(
                    get_timerange_expression(
                        Attr, TimeRangeBoundary.END, timerange_filter
                    )
                )
            if timerange_filter.end:
                filter_conditions.append(
                    get_timerange_expression(
                        Attr, TimeRangeBoundary.START, timerange_filter
                    )
                )
        if len(filter_conditions) == 2:
            args["FilterExpression"] = And(*filter_conditions)
        elif filter_conditions:
            args["FilterExpression"] = filter_conditions[0]
    elif timerange_filter:
        if timerange_filter.start and timerange_filter.end:
            # Bound the sort key on BOTH sides so pagination cannot scan past the
            # requested range. Segments in a Flow never overlap, so the first
            # segment with timerange_end >= the filter end is the only one above
            # the range that could still overlap it; every later segment starts
            # after the filter end and would be dropped by the FilterExpression
            # anyway. BETWEEN is inclusive, so fold the start boundary's
            # inclusivity into the lower bound the same way timerange_start is
            # stored on write (see process_single_segment).
            lower = timerange_filter.start.to_nanosec() + (
                0 if timerange_filter.includes_start() else 1
            )
            upper = get_exact_timerange_end(flow_id, timerange_filter.end.to_nanosec())
            # A non-empty range always has lower <= upper. An empty range (e.g.
            # "()" or "[5:0_5:0)") normalises to start == end yet is not eternity,
            # so it reaches here with lower > upper. DynamoDB rejects BETWEEN when
            # lower > upper with a ValidationException, so clamp upper up to lower:
            # the retained FilterExpression drops every candidate anyway, leaving
            # the empty result an empty range must produce.
            upper = max(upper, lower)
            args["KeyConditionExpression"] = And(
                args["KeyConditionExpression"],
                Key("timerange_end").between(lower, upper),
            )
            # Retain the end filter to drop the boundary segment when it starts
            # after the requested range (i.e. does not actually overlap it).
            args["FilterExpression"] = get_timerange_expression(
                Attr, TimeRangeBoundary.START, timerange_filter
            )
        elif timerange_filter.start:
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
    resources: list | None = None,
) -> None:
    """Performs the logic to delete flow segments exits gracefully if within 5 seconds of Lambda timeout.

    `resources` is forwarded to delete_segment_items for the
    flows/segments_deleted events; see that function for why it is required when
    the Flow is being deleted.
    """
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
            resources,
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
                resources,
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
    if query["Items"]:
        resume_after = TimeRange.from_str(
            query["Items"][-1]["timerange"]
        ).timerange_after()
    else:
        # The final scanned page held no deletable items (all filtered out or
        # popped as partial-overlap outliers) yet more records remain. Resume
        # from just after the last scanned key so the next invocation does not
        # re-scan this same tail. timerange_end is stored inclusive, so the next
        # unscanned segment starts at cursor + 1 nanosecond.
        resume_after = TimeRange.from_start(
            Timestamp.from_nanosec(int(query["LastEvaluatedKey"]["timerange_end"]) + 1)
        )
    timerange_remaining = timerange_to_delete.intersect_with(resume_after)
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
def validate_object_id(segment: Flowsegmentpost, flow_id: str) -> dict:
    """Validate object_id can be used with flow_id.

    Performs NO writes. Any storage mutations required on success are returned
    in the `claim` list for the caller to apply via commit_object_claim, and
    only after all validation (including overlap and init_segments consistency
    checks) has passed, so that a rejected request never claims an Object.
    """
    get_item = storage_table.get_item(Key={"id": segment.object_id})
    storage_item = get_item.get("Item")
    # Handle case where object_id doesn't exist
    if storage_item is None:
        if not segment.get_urls:
            # No matching object_id and no get_urls supplied so invalid
            return {
                "valid": False,
                "storage_id": None,
                "object_timerange": None,
                "message": "Bad request. The object id does not exist and no get_urls supplied.",
            }
        labels = [get_url.label for get_url in segment.get_urls]
        # get_urls supplied with duplicate labels
        if len(labels) != len(set(labels)):
            return {
                "valid": False,
                "storage_id": None,
                "object_timerange": None,
                "message": "Bad request. All label value in get_urls must be unique.",
            }
        # Storage record to create so the "first_referenced_by" field in the
        # objects endpoint reports correctly. Applied by the caller on success.
        object_timerange = calculate_object_timerange(segment)
        item = {
            "id": segment.object_id,
            "flow_id": flow_id,
            "timerange": object_timerange,
        }
        if segment.init_object_id:
            item["init_object_id"] = segment.init_object_id
        # No matching object_id and get_urls supplied so this is valid
        return {
            "valid": True,
            "storage_id": None,
            "init_object_id": segment.init_object_id,
            "init_storage_id": None,
            "object_timerange": object_timerange,
            "message": None,
            "claim": [{"op": "put", "item": item}],
        }
    # object_id exists - get_urls not allowed when called from segments endpoint
    if segment.get_urls:
        return {
            "valid": False,
            "storage_id": None,
            "object_timerange": None,
            "message": "Bad request. An unused object id is required when supplying get_urls.",
        }
    # Reject using an init object as a media object
    if storage_item.get("is_init_object"):
        return {
            "valid": False,
            "storage_id": None,
            "object_timerange": None,
            "message": "Bad request. An initialisation segment Object cannot be used as a media segment Object.",
        }
    is_first_time_use = storage_item.get("expire_at") is not None
    flow_id_matches = storage_item["flow_id"] == flow_id
    stored_timerange = storage_item.get("timerange")
    claim = []
    # First time use object_id must be used on flow_id it was created with
    if is_first_time_use and not flow_id_matches:
        return {
            "valid": False,
            "storage_id": None,
            "object_timerange": None,
            "message": "Bad request. The object id is not valid to be used for the flow id supplied.",
        }
    # Queue removal of expiration on first use with matching flow_id
    if is_first_time_use and flow_id_matches:
        object_timerange = calculate_object_timerange(segment)
        update_expr = "REMOVE expire_at SET timerange = :timerange"
        expr_values = {":timerange": object_timerange}
        if segment.init_object_id:
            update_expr += ", init_object_id = :init_object_id"
            expr_values[":init_object_id"] = segment.init_object_id
        claim.append(
            {
                "op": "update",
                "key": {"id": segment.object_id},
                "UpdateExpression": update_expr,
                "ExpressionAttributeValues": expr_values,
            }
        )
        stored_timerange = object_timerange
    # object_timerange must not be specified on object re-use
    if not is_first_time_use and segment.object_timerange:
        return {
            "valid": False,
            "storage_id": None,
            "object_timerange": None,
            "message": "Bad request. The object_timerange should not be specified when Media Objects are re-used.",
        }
    # init_object_id must not change on object re-use
    if not is_first_time_use and segment.init_object_id is not None:
        stored_init_object_id = storage_item.get("init_object_id")
        if (
            stored_init_object_id is not None
            and stored_init_object_id != segment.init_object_id
        ):
            return {
                "valid": False,
                "storage_id": None,
                "object_timerange": None,
                "message": "Bad request. The init_object_id must not change when Media Objects are re-used.",
            }
    init_storage_id = None
    effective_init_object_id = segment.init_object_id
    # Validate init_object_id if provided
    if segment.init_object_id:
        init_item = storage_table.get_item(Key={"id": segment.init_object_id}).get(
            "Item"
        )
        if init_item is None:
            return {
                "valid": False,
                "storage_id": None,
                "object_timerange": None,
                "message": "Bad request. The init_object_id does not exist.",
            }
        if init_item.get("is_init_object"):
            init_storage_id = init_item.get("storage_id")
        elif init_item.get("expire_at"):
            # First time use as init object - check flow_id matches
            if init_item["flow_id"] != flow_id:
                return {
                    "valid": False,
                    "storage_id": None,
                    "object_timerange": None,
                    "message": "Bad request. The init_object_id is not valid to be used for the flow id supplied.",
                }
            claim.append(
                {
                    "op": "update",
                    "key": {"id": segment.init_object_id},
                    "UpdateExpression": "REMOVE expire_at SET is_init_object = :flag",
                    "ExpressionAttributeValues": {":flag": True},
                }
            )
            init_storage_id = init_item.get("storage_id")
        else:
            # No expire_at, no is_init_object → already used as media object
            return {
                "valid": False,
                "storage_id": None,
                "object_timerange": None,
                "message": "Bad request. A media segment Object cannot be used as an initialisation segment Object.",
            }
    elif not is_first_time_use and storage_item.get("init_object_id"):
        # Object re-use with init_object_id omitted (as recommended by the spec).
        # Recover the init reference from the stored Media Object so the new
        # Segment still reports init_object on read.
        effective_init_object_id = storage_item["init_object_id"]
        init_item = storage_table.get_item(Key={"id": effective_init_object_id}).get(
            "Item"
        )
        if init_item:
            init_storage_id = init_item.get("storage_id")
    # Valid: either flow_id matches or object_id is reusable
    return {
        "valid": True,
        "storage_id": storage_item.get("storage_id"),
        "init_object_id": effective_init_object_id,
        "init_storage_id": init_storage_id,
        "object_timerange": stored_timerange,
        "message": None,
        "claim": claim,
    }


@tracer.capture_method(capture_response=False)
def commit_object_claim(claim: list | None) -> None:
    """Apply the storage writes returned by validate_object_id.

    Called only after all validation has passed, so a rejected request never
    mutates (claims) an Object.
    """
    for write in claim or []:
        if write["op"] == "put":
            storage_table.put_item(Item=write["item"])
        elif write["op"] == "update":
            storage_table.update_item(
                Key=write["key"],
                UpdateExpression=write["UpdateExpression"],
                ExpressionAttributeValues=write["ExpressionAttributeValues"],
            )


@tracer.capture_method(capture_response=False)
def delete_flow_storage_record(object_id: str, storage_id: str | None = None) -> None:
    """Remove storage_id from object's DDB record, or delete the record entirely if no segments reference it"""
    object_id_refs = segments_table.query(
        IndexName="object-id-index",
        KeyConditionExpression=Key("object_id").eq(object_id),
        Select="COUNT",
    )
    init_object_id_refs = segments_table.query(
        IndexName="init-object-id-index",
        KeyConditionExpression=Key("init_object_id").eq(object_id),
        Select="COUNT",
    )
    if object_id_refs["Count"] == 0 and init_object_id_refs["Count"] == 0:
        storage_table.delete_item(
            Key={"id": object_id},
        )
        return
    try:
        storage_table.update_item(
            Key={"id": object_id},
            UpdateExpression="REMOVE storage_id",
            ConditionExpression="storage_id = :storage_id",
            ExpressionAttributeValues={":storage_id": storage_id},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # existing storage_id does not exist or doesn't match, no action required.
            return
        raise


@tracer.capture_method(capture_response=False)
def decode_and_validate_page(
    page: str, object_id: str, key_name: str = "object_id"
) -> dict:
    """Decode and validate base64 encoded pagination key"""
    try:
        decoded_page = base64.b64decode(page).decode("utf-8")
        exclusive_start_key = json.loads(decoded_page)
    except Exception as ex:
        raise BadRequestError("Invalid page parameter value") from ex
    if any(
        f not in exclusive_start_key for f in ["flow_id", key_name, "timerange_end"]
    ):
        raise BadRequestError("Invalid page parameter value")
    if exclusive_start_key[key_name] != object_id:
        raise BadRequestError("Invalid page parameter value")
    return exclusive_start_key


@tracer.capture_method(capture_response=False)
def query_segments_by_object_id(
    object_id: str,
    projection: str | None = None,
    limit: int | None = None,
    page: str | None = None,
    fetch_all: bool = False,
) -> tuple[list, dict | None, int | None]:
    """Query segments by object_id using object-id-index"""
    kwargs = {
        "IndexName": "object-id-index",
        "KeyConditionExpression": Key("object_id").eq(object_id),
    }
    if projection:
        kwargs["ProjectionExpression"] = projection
    if not fetch_all:
        kwargs["Limit"] = (
            min(limit, constants.MAX_PAGE_LIMIT)
            if limit
            else constants.DEFAULT_PAGE_LIMIT
        )
        if page:
            kwargs["ExclusiveStartKey"] = decode_and_validate_page(page, object_id)

    query = segments_table.query(**kwargs)
    items = query["Items"]
    while "LastEvaluatedKey" in query and (fetch_all or len(items) < kwargs["Limit"]):
        kwargs["ExclusiveStartKey"] = query["LastEvaluatedKey"]
        query = segments_table.query(**kwargs)
        items.extend(query["Items"])
    return items, query.get("LastEvaluatedKey"), kwargs.get("Limit")


@tracer.capture_method(capture_response=False)
def query_segments_by_init_object_id(
    init_object_id: str,
    projection: str | None = None,
    limit: int | None = None,
    page: str | None = None,
    fetch_all: bool = False,
) -> tuple[list, dict | None, int | None]:
    """Query segments by init_object_id using init-object-id-index"""
    kwargs = {
        "IndexName": "init-object-id-index",
        "KeyConditionExpression": Key("init_object_id").eq(init_object_id),
    }
    if projection:
        kwargs["ProjectionExpression"] = projection
    if not fetch_all:
        kwargs["Limit"] = (
            min(limit, constants.MAX_PAGE_LIMIT)
            if limit
            else constants.DEFAULT_PAGE_LIMIT
        )
        if page:
            kwargs["ExclusiveStartKey"] = decode_and_validate_page(
                page, init_object_id, key_name="init_object_id"
            )
    query = segments_table.query(**kwargs)
    items = query["Items"]
    while "LastEvaluatedKey" in query and (fetch_all or len(items) < kwargs["Limit"]):
        kwargs["ExclusiveStartKey"] = query["LastEvaluatedKey"]
        query = segments_table.query(**kwargs)
        items.extend(query["Items"])
    return items, query.get("LastEvaluatedKey"), kwargs.get("Limit")


@tracer.capture_method(capture_response=False)
def page_targets_init_index(page: str) -> bool:
    """Return True if a pagination token belongs to the init-object-id-index.

    A malformed token returns False so the caller's primary (media) query path
    raises the canonical 'Invalid page parameter value' error.
    """
    try:
        decoded = json.loads(base64.b64decode(page).decode("utf-8"))
    except ValueError, TypeError:
        return False
    return "init_object_id" in decoded


@lru_cache()
@tracer.capture_method(capture_response=False)
def get_store_name() -> str:
    """Get the store name from service configuration, defaults to 'tams'."""
    get_item = service_table.get_item(
        Key={"record_type": "service", "id": constants.SERVICE_INFO_ID}
    )
    if not get_item.get("Item"):
        return "tams"
    if get_item["Item"].get("name") is None:
        return "tams"
    return get_item["Item"]["name"]


@tracer.capture_method(capture_response=False)
def get_storage_backend_dict(item: dict, store_name: str) -> dict:
    """Transform storage backend item into standardized dictionary format with label."""
    return {
        "storage_id": item["id"],
        "label": f"aws.{item['region']}:s3:{store_name}",
        **item,
    }


@lru_cache()
@tracer.capture_method(capture_response=False)
def get_default_storage_backend() -> dict:
    """Retrieve the default storage backend configuration from service table."""
    query = service_table.query(
        KeyConditionExpression=Key("record_type").eq("storage-backend"),
        FilterExpression=Attr("default_storage").eq(True),
    )
    items = query["Items"]
    if len(items) == 0:
        raise BadRequestError("No default storage backend found")  # 404
    return get_storage_backend_dict(items[0], get_store_name())


@lru_cache()
@tracer.capture_method(capture_response=False)
def get_storage_backend(storage_id: str) -> dict:
    """Retrieve specific storage backend configuration by storage_id."""
    get_item = service_table.get_item(
        Key={"record_type": "storage-backend", "id": storage_id}
    )
    if not get_item.get("Item"):
        raise BadRequestError("Invalid storage backend identifier")  # 404
    return get_storage_backend_dict(get_item["Item"], get_store_name())


@lru_cache()
@tracer.capture_method(capture_response=False)
def list_storage_backends() -> list[dict]:
    """Retrieve all storage backend items from service table."""
    args = {"KeyConditionExpression": Key("record_type").eq("storage-backend")}
    query = service_table.query(**args)
    items = query["Items"]
    while "LastEvaluatedKey" in query:
        args["ExclusiveStartKey"] = query["LastEvaluatedKey"]
        query = service_table.query(**args)
        items.extend(query["Items"])
    store_name = get_store_name()
    return [get_storage_backend_dict(item, store_name) for item in items]


@tracer.capture_method(capture_response=False)
def append_to_segment_list(item: dict, attribute: str, value: dict | str) -> None:
    """Append a value to a list attribute in a segment."""
    segments_table.update_item(
        Key={
            "flow_id": item["flow_id"],
            "timerange_end": item["timerange_end"],
        },
        UpdateExpression="SET #attr = list_append(if_not_exists(#attr, :empty_list), :new_value)",
        ExpressionAttributeNames={
            "#attr": attribute,
        },
        ExpressionAttributeValues={
            ":new_value": [value],
            ":empty_list": [],
        },
    )


@tracer.capture_method(capture_response=False)
def remove_storage_id_from_segment(
    item: dict, storage_id: str, attribute: str = "storage_ids"
) -> None:
    """Remove a storage_id from a segment's storage_ids list with optimistic locking.

    `attribute` selects which list to mutate ("storage_ids" for the Media
    Object, "init_storage_ids" for the init Object).
    """
    current_item = item
    for attempt in range(constants.DDB_MAX_RETRIES):
        try:
            filtered_ids = [sid for sid in current_item[attribute] if sid != storage_id]
            segments_table.update_item(
                Key={
                    "flow_id": current_item["flow_id"],
                    "timerange_end": current_item["timerange_end"],
                },
                UpdateExpression="SET #attr = :ids",
                ConditionExpression="#attr = :old_ids",
                ExpressionAttributeNames={"#attr": attribute},
                ExpressionAttributeValues={
                    ":ids": filtered_ids,
                    ":old_ids": current_item[attribute],
                },
            )
            break
        except ClientError as e:
            if (
                e.response["Error"]["Code"] == "ConditionalCheckFailedException"
                and attempt < constants.DDB_MAX_RETRIES - 1
            ):
                current_item = segments_table.get_item(
                    Key={
                        "flow_id": item["flow_id"],
                        "timerange_end": item["timerange_end"],
                    }
                )["Item"]
                continue
            raise


@tracer.capture_method(capture_response=False)
def remove_get_url_by_label_from_segment(
    item: dict, label: str, attribute: str = "get_urls"
) -> None:
    """Remove a get_url dictionary with matching label from a segment's get_urls
    list with optimistic locking.

    `attribute` selects which list to mutate ("get_urls" for the Media Object,
    "init_get_urls" for the init Object).
    """
    current_item = item
    for attempt in range(constants.DDB_MAX_RETRIES):
        try:
            filtered_urls = [
                url for url in current_item[attribute] if url.get("label") != label
            ]
            segments_table.update_item(
                Key={
                    "flow_id": current_item["flow_id"],
                    "timerange_end": current_item["timerange_end"],
                },
                UpdateExpression="SET #attr = :urls",
                ConditionExpression="#attr = :old_urls",
                ExpressionAttributeNames={"#attr": attribute},
                ExpressionAttributeValues={
                    ":urls": filtered_urls,
                    ":old_urls": current_item[attribute],
                },
            )
            break
        except ClientError as e:
            if (
                e.response["Error"]["Code"] == "ConditionalCheckFailedException"
                and attempt < constants.DDB_MAX_RETRIES - 1
            ):
                current_item = segments_table.get_item(
                    Key={
                        "flow_id": item["flow_id"],
                        "timerange_end": item["timerange_end"],
                    }
                )["Item"]
                continue
            raise
