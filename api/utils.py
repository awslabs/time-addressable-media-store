import json
import math
import os
import urllib.parse
import uuid
from datetime import datetime, timezone
from functools import reduce

# pylint: disable=no-name-in-module
from itertools import batched

import boto3
import constants
from aws_lambda_powertools import Tracer
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from aws_lambda_powertools.utilities.data_classes.api_gateway_proxy_event import (
    APIGatewayEventRequestContext,
)
from aws_lambda_powertools.utilities.parser import parse
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import And, Attr, Key
from botocore.config import Config
from botocore.exceptions import ClientError
from mediatimestamp.immutable import TimeRange
from params import essence_params, query_params
from schema import Flow, Source

tracer = Tracer()

events = boto3.client("events")
sqs = boto3.client("sqs")
s3 = boto3.client(
    "s3", config=Config(s3={"addressing_style": "virtual"})
)  # Addressing style is required to ensure pre-signed URLs work as soon as the bucket is created.


@tracer.capture_method(capture_response=False)
def base_delete_request_dict(
    flow_id: str, request_context: APIGatewayEventRequestContext
) -> dict:
    """returns a base delete request dict"""
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    return {
        "id": str(uuid.uuid4()),
        "created": now,
        "updated": now,
        "status": "created",
        "flow_id": flow_id,
        "created_by": get_username(request_context),
    }


@tracer.capture_method(capture_response=False)
def check_delete_source(
    table: "boto3.resources.factory.dynamodb.Table", source_id: str
) -> bool:
    """check if source is now not referenced and therefore to be deleted"""
    query = table.query(
        IndexName="source-id-index",
        KeyConditionExpression=Key("source_id").eq(source_id),
        Select="COUNT",
    )
    if query["Count"] == 0:
        table.delete_item(Key={"record_type": "source", "id": source_id})
        publish_event("sources/deleted", {"source_id": source_id}, [source_id])
        return True
    return False


@tracer.capture_method(capture_response=False)
def delete_flow_segments(
    table: "boto3.resources.factory.dynamodb.Table",
    segments_table: "boto3.resources.factory.dynamodb.Table",
    flow_id: str,
    parameters: None | dict,
    valid_parameters: list,
    timerange_to_delete: TimeRange,
    context: LambdaContext,
    s3_queue: str,
    del_queue: str,
    item_dict: dict | None = None,
):
    """Performs the logic to delete flow segments exits gracefully if within 5 seconds of Lambda timeout"""
    delete_error = None
    args, _ = get_key_and_args(flow_id, parameters, valid_parameters)
    query = segments_table.query(**args, Limit=constants.DELETE_BATCH_SIZE)
    object_ids = set()
    # Pop first and/or last item in array if they are not entirely covered by the deletion timerange
    pop_outliers(timerange_to_delete, query["Items"])
    if len(query["Items"]) > 0:
        delete_error = delete_segment_items(
            segments_table,
            query["Items"],
            object_ids,
        )
        update_flow_segments_updated(flow_id, table)
    # Continue with deletes if no errors, more records available and more than specified milliseconds remain of runtime
    while (
        delete_error is None
        and "LastEvaluatedKey" in query
        and context.get_remaining_time_in_millis() > constants.LAMBDA_TIME_REMAINING
    ):
        query = segments_table.query(
            **args,
            Limit=constants.DELETE_BATCH_SIZE,
            ExclusiveStartKey=query["LastEvaluatedKey"],
        )
        # Pop first and/or last item in array if they are not entirely covered by the deletion timerange
        pop_outliers(timerange_to_delete, query["Items"])
        if len(query["Items"]) > 0:
            delete_error = delete_segment_items(
                segments_table,
                query["Items"],
                object_ids,
            )
            update_flow_segments_updated(flow_id, table)
    # Add affected object_ids to the SQS queue for potential S3 cleanup
    if len(object_ids) > 0:
        for message in get_message_batches(list(object_ids)):
            sqs.send_message(
                QueueUrl=s3_queue,
                MessageBody=json.dumps(message),
            )
    if item_dict is None:
        # item_dict only None when called from object_id related segment delete. This method does not support delete requests
        return
    # Update DDB record with error if error encountered, no SQS publish to prevent further processing
    if delete_error:
        table.put_item(
            Item={
                "record_type": "delete-request",
                **item_dict,
                "status": "error",
                "error": delete_error,
            }
        )
        return
    # Update DDB record with done, no SQS publish as no further processing required.
    if "LastEvaluatedKey" not in query:
        table.put_item(
            Item={
                "record_type": "delete-request",
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
    put_deletion_request(del_queue, table, item_dict)


@tracer.capture_method(capture_response=False)
def delete_segment_items(
    segments_table: "boto3.resources.factory.dynamodb.Table",
    items: list[dict],
    object_ids: set[str],
) -> tuple[dict, None]:
    """loop supplied items and delete, early return on error, append to object_ids supplied on success"""
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
                    [item["flow_id"]],
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
def generate_link_url(current_event: APIGatewayProxyEvent, page_value: str) -> str:
    """Generates a link URL relative to the API Gateway request that calls it"""
    host = current_event.request_context.domain_name
    path = current_event.request_context.path
    query_string = (
        "&".join(
            f"{k}={v}"
            for k, v in current_event.query_string_parameters.items()
            if k != "page"
        )
        + "&"
        if current_event.query_string_parameters
        else ""
    )

    return f'<https://{host}{path}?{query_string}page={urllib.parse.quote_plus(page_value)}>; rel="next"'


@tracer.capture_method(capture_response=False)
# pylint: disable=W0102:dangerous-default-value
def generate_presigned_url(
    method: str, bucket: str, key: str, other_args: dict = {}
) -> str:
    """Generates an S3 pre-signed URL"""
    url = s3.generate_presigned_url(
        ClientMethod=method,
        Params={
            "Bucket": bucket,
            "Key": key,
            **other_args,
        },
        ExpiresIn=3600,
    )
    return url


@tracer.capture_method(capture_response=False)
def get_ddb_args(
    params: None | dict,
    valid_params: list,
    tags_supported: bool,
    record_type: None | str,
) -> dict:
    """Processes API Gateway query string parameters into args for a DynamoDB query operation"""
    if params is None:
        if "limit" in valid_params:
            return {"Limit": constants.DEFAULT_PAGE_LIMIT}
        return {}
    args = {}
    # Pagination query string parameters
    if "page" in valid_params and "page" in params:
        valid_params.remove("page")
        if record_type in ["source", "flow", "delete-request"]:
            args["ExclusiveStartKey"] = {
                "record_type": record_type,
                "id": params["page"],
            }
        else:
            # record_type field is used for flow_id when query is related to flow segments.
            args["ExclusiveStartKey"] = {
                "flow_id": record_type,
                "timerange_end": int(params["page"]),
            }
    if "limit" in valid_params:
        if "limit" in params:
            valid_params.remove("limit")
            args["Limit"] = (
                int(params["limit"])
                if int(params["limit"]) < constants.MAX_PAGE_LIMIT
                else constants.MAX_PAGE_LIMIT
            )
        else:
            args["Limit"] = (
                constants.DEFAULT_PAGE_LIMIT
            )  # Default limit if not supplied
    # Filter query string parameters
    filter_expressions = []
    if "timerange" in valid_params and "timerange" in params:
        valid_params.remove("timerange")
        timerange_filter = TimeRange.from_str(params["timerange"])
        if timerange_filter.start and "object_id" in params:
            filter_expressions.append(
                Attr("timerange_end").gte(timerange_filter.start.to_nanosec())
                if timerange_filter.includes_start()
                else Attr("timerange_end").gt(timerange_filter.start.to_nanosec())
            )
        if timerange_filter.end:
            filter_expressions.append(
                Attr("timerange_start").lte(timerange_filter.end.to_nanosec())
                if timerange_filter.includes_end()
                else Attr("timerange_start").lt(timerange_filter.end.to_nanosec())
            )
    for key in valid_params:
        if key in params:
            if key in essence_params:
                value = params[key]
                if essence_params[key] == "int":
                    value = int(value)
                elif essence_params[key] == "float":
                    value = float(value)
                elif essence_params[key] == "bool":
                    value = value.lower() == "true"
                filter_expressions.append(Attr(f"essence_parameters.{key}").eq(value))
            else:
                filter_expressions.append(Attr(key).eq(params[key]))
    # Handle tag query string parameters
    if tags_supported:
        for key in params.copy().keys():
            if key.startswith("tag."):
                filter_expressions.append(
                    Attr(f'tags.{key.split(".", 1)[1]}').eq(params[key])
                )
            elif key.startswith("tag_exists."):
                value = params[key].lower()
                if value in ["true", "false"]:
                    if value == "true":
                        filter_expressions.append(
                            Attr(f'tags.{key.split(".", 1)[1]}').exists()
                        )
                    else:
                        filter_expressions.append(
                            Attr(f'tags.{key.split(".", 1)[1]}').not_exists()
                        )
    if len(filter_expressions) > 0:
        args["FilterExpression"] = reduce(And, filter_expressions)
    return args


@tracer.capture_method(capture_response=False)
def get_flow_timerange(
    table: "boto3.resources.factory.dynamodb.Table", flow_id: str
) -> str:
    """Get the timerange for a specified flow"""
    first_segment = table.query(
        KeyConditionExpression=Key("flow_id").eq(flow_id),
        Limit=1,
        ScanIndexForward=True,
        Select="SPECIFIC_ATTRIBUTES",
        ProjectionExpression="timerange",
    )["Items"]
    last_segment = table.query(
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
def get_key_and_args(
    flow_id: str, parameters: None | dict, valid_parameters: list
) -> tuple[dict, bool]:
    """generate key expression and args for a dynamodb query operation. Additionally returns specfied order as boolean (reverse = True)"""
    # Build parameters for query or scan
    args = get_ddb_args(
        parameters, valid_parameters, False, flow_id
    )  # record_type used for flow_id is a special use case specific to flow segments
    timerange_filter = None
    if parameters and "timerange" in parameters:
        timerange_filter = TimeRange.from_str(parameters["timerange"])
    reverse_order = False
    if parameters and "reverse_order" in parameters:
        reverse_order = parameters["reverse_order"].lower() == "true"
    # Set query direction
    args["ScanIndexForward"] = not reverse_order
    # Set primary key condition expression
    key = Key("flow_id").eq(flow_id)
    if parameters and "object_id" in parameters:
        args["IndexName"] = "object-id-index"
        key = And(
            key,
            Key("object_id").eq(parameters["object_id"]),
        )
    elif timerange_filter and timerange_filter.start:
        key = And(
            key,
            (
                Key("timerange_end").gte(timerange_filter.start.to_nanosec())
                if timerange_filter.includes_end()
                else Key("timerange_end").gt(timerange_filter.start.to_nanosec())
            ),
        )
    args["KeyConditionExpression"] = key
    return args, reverse_order


@tracer.capture_method(capture_response=False)
def get_message_batches(items: list) -> list:
    """split a list of items into a list of batches all smaller than the defined maximum message size"""
    if len(items) == 0:
        return []
    batch_count = math.ceil(
        len(json.dumps(items, default=str)) / constants.MAX_MESSAGE_SIZE
    )
    batch_size = math.ceil(len(items) / batch_count)
    return list(batched(items, batch_size))


@tracer.capture_method(capture_response=False)
def get_model_by_id(
    table: "boto3.resources.factory.dynamodb.Table", record_type: str, record_id: str
) -> Flow | Source:
    """get record if exists else return None"""
    model_mapping = {"flow": Flow, "source": Source}
    item = table.get_item(Key={"record_type": record_type, "id": record_id})
    if "Item" not in item:
        return None
    return parse(event=item["Item"], model=model_mapping[record_type])


@tracer.capture_method(capture_response=False)
def get_username(request_context: APIGatewayEventRequestContext) -> str:
    """Dervive a suitable username from the API Gateway request details"""
    idp = boto3.client("cognito-idp")
    if "username" in request_context.authorizer.claims:
        user_pool = idp.describe_user_pool(UserPoolId=os.environ["USER_POOL_ID"])[
            "UserPool"
        ]
        if "UsernameAttributes" in user_pool:
            user_attributes = idp.admin_get_user(
                UserPoolId=os.environ["USER_POOL_ID"],
                Username=request_context.authorizer.claims["username"],
            )["UserAttributes"]
            user_attributes = {a["Name"]: a["Value"] for a in user_attributes}
            if "email" in user_pool["UsernameAttributes"]:
                return user_attributes["email"]
            if "phone_number" in user_pool["UsernameAttributes"]:
                return user_attributes["phone_number"]
        return request_context.authorizer.claims["username"]
    if "client_id" in request_context.authorizer.claims:
        user_pool_client = idp.describe_user_pool_client(
            UserPoolId=os.environ["USER_POOL_ID"],
            ClientId=request_context.authorizer.claims["client_id"],
        )
        return user_pool_client["UserPoolClient"]["ClientName"]
    return "NoAuth"


@tracer.capture_method(capture_response=False)
def json_number(x: any) -> float | int:
    """Returns a numeric value as the int of float based upon whether it contains a decimal point"""
    f = float(x)
    if f.is_integer():
        return int(f)
    return f


@tracer.capture_method(capture_response=False)
def model_dump(model, **kwargs):
    args = {"by_alias": True, "exclude_unset": True, "exclude_none": True, **kwargs}
    model_dict = model.model_dump(mode="json", **args)
    remove_null(model_dict)
    return model_dict


@tracer.capture_method(capture_response=False)
def model_dump_json(model, **kwargs):
    if isinstance(model, list):
        model_dict = [model_dump(m, **kwargs) for m in model]
    else:
        model_dict = model_dump(model, **kwargs)
    return json.dumps(model_dict)


@tracer.capture_method(capture_response=False)
def pop_outliers(timerange: TimeRange, items: list) -> None:
    """Remove ends of a list of Timerange items if they do not fully cover teh supplied Timerange"""
    if len(items) > 1:
        if not timerange.contains_subrange(TimeRange.from_str(items[-1]["timerange"])):
            items.pop(-1)
    if len(items) > 0:
        if not timerange.contains_subrange(TimeRange.from_str(items[0]["timerange"])):
            items.pop(0)


@tracer.capture_method(capture_response=False)
# pylint: disable=dangerous-default-value
def publish_event(detail_type: str, details: dict, resources: list = []) -> None:
    """publishes the supplied events to an EventBridge EventBus"""
    events.put_events(
        Entries=[
            {
                "Source": "tams.api",
                "EventBusName": os.environ["EVENT_BUS"],
                "DetailType": detail_type,
                "Time": datetime.now(),
                "Detail": json.dumps(details),
                "Resources": resources,
            }
        ],
    )


@tracer.capture_method(capture_response=False)
def put_deletion_request(
    queue: str, table: "boto3.resources.factory.dynamodb.Table", item: dict
) -> None:
    """publishs a message to SQS and inserts into dynamodb"""
    sqs.send_message(
        QueueUrl=queue,
        MessageBody=json.dumps(item),
    )
    table.put_item(Item={"record_type": "delete-request", **item})


@tracer.capture_method(capture_response=False)
def remove_null(d: any) -> None:
    """Removes null and other "empty" keys from a dict recursively"""
    if isinstance(d, list):
        for i in d:
            remove_null(i)
    elif isinstance(d, dict):
        for k, v in d.copy().items():
            if v is None or v == {} or v == []:
                d.pop(k)
            elif isinstance(v, str):
                for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f"):
                    try:
                        dt = datetime.strptime(v, fmt)
                        d[k] = dt.astimezone(timezone.utc).strftime(
                            constants.DATETIME_FORMAT
                        )
                    except ValueError:
                        pass
            else:
                remove_null(v)


@tracer.capture_method(capture_response=False)
def update_collected_by(
    table: "boto3.resources.factory.dynamodb.Table",
    flow_id: str,
    flow: Flow,
    add_to: bool,
) -> None:
    """update the collected by field on the specified flow"""
    if flow.root.collected_by:
        if add_to:
            flow.root.collected_by.append(flow_id)
            flow.root.collected_by = list(set(flow.root.collected_by))
        else:
            flow.root.collected_by = [c for c in flow.root.collected_by if c != flow_id]
    else:
        if add_to:
            flow.root.collected_by = [flow_id]
    item_dict = model_dump(flow)
    table.put_item(Item={"record_type": "flow", **item_dict})
    publish_event("flows/updated", {"flow": item_dict}, [flow_id])


@tracer.capture_method(capture_response=False)
def update_flow_collection(
    table: "boto3.resources.factory.dynamodb.Table", flow_id: str, collected_by_id: str
) -> None:
    """update the flow_collection on the specified flow"""
    item = table.get_item(Key={"record_type": "flow", "id": collected_by_id})
    if "Item" in item:
        flow: Flow = Flow(**item["Item"])
        if flow.root.flow_collection:
            flow.root.flow_collection = [
                collection
                for collection in flow.root.flow_collection
                if collection.id != flow_id
            ]
            item_dict = model_dump(flow)
            table.put_item(Item={"record_type": "flow", **item_dict})
            publish_event("flows/updated", {"flow": item_dict}, [flow_id])


@tracer.capture_method(capture_response=False)
def update_flow_segments_updated(
    flow_id: str, table: "boto3.resources.factory.dynamodb.Table"
) -> None:
    """Update the segments_updated field on the specified Flow"""
    record_type = "flow"
    item = table.get_item(Key={"record_type": record_type, "id": flow_id})
    if "Item" not in item:
        return
    flow: Flow = Flow(**item["Item"])
    now = datetime.now().strftime(constants.DATETIME_FORMAT)
    flow.root.segments_updated = now
    item_dict = model_dump(flow)
    try:
        table.put_item(
            Item={"record_type": record_type, **item_dict},
            ConditionExpression=And(
                Key("record_type").eq(record_type), Key("id").eq(flow_id)
            ),
        )
        publish_event("flows/updated", {"flow": item_dict}, [flow.root.id])
    except boto3.resource(
        "dynamodb"
    ).meta.client.exceptions.ConditionalCheckFailedException:
        pass


@tracer.capture_method(capture_response=False)
def validate_query_string(
    params: None | dict, request_context: APIGatewayEventRequestContext
) -> bool:
    """checks if supplied parameters are valid names for the path and method of the request"""
    if params is None:
        return True
    query_string_parameters_keys = query_params[request_context.resource_path][
        request_context.http_method
    ].keys()
    for key in params.keys():
        if key not in query_string_parameters_keys:
            if not key.startswith("tag.") and not key.startswith("tag_exists."):
                return False
    return True
