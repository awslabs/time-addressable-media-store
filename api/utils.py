# pylint: disable=too-many-lines
import json
import math
import os
import urllib.parse
import uuid
from datetime import datetime, timezone

# pylint: disable=no-name-in-module
from itertools import batched

import boto3
import constants
import cymple
from aws_lambda_powertools import Tracer
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from aws_lambda_powertools.utilities.data_classes.api_gateway_proxy_event import (
    APIGatewayEventRequestContext,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import And, Attr, Key
from botocore.exceptions import ClientError
from cymple import QueryBuilder
from mediatimestamp.immutable import TimeRange
from params import essence_params, query_params
from pydantic import BaseModel

tracer = Tracer()


events = boto3.client("events")
sqs = boto3.client("sqs")
s3 = boto3.client("s3")
neptune = boto3.client(
    "neptunedata",
    region_name=os.environ["AWS_REGION"],
    endpoint_url=f'https://{os.environ["NEPTUNE_ENDPOINT"]}:8182',
)
qb = QueryBuilder()


@tracer.capture_method(capture_response=False)
def base_delete_request_dict(
    flow_id: str, request_context: APIGatewayEventRequestContext
) -> dict:
    """Returns a base delete request dict"""
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
def check_delete_source(source_id: str) -> None:
    """Checks to see if the specified Source is not without representations and if so it deletes it from the Neptune Database"""
    query = (
        qb.match()
        .node(ref_name="source", labels="source", properties={"id": source_id})
        .where_literal("NOT exists((source)<-[:represents]-(:flow))")
        .return_literal("source.id AS source_id")
        .get()
    )
    results = neptune.execute_open_cypher_query(openCypherQuery=query)
    if len(results["results"]) != 0:
        query = (
            qb.match()
            .node(ref_name="source", labels="source", properties={"id": source_id})
            .detach_delete(ref_name="source")
            .return_literal("source.id AS source_id")
            .get()
        )
        neptune.execute_open_cypher_query(openCypherQuery=query)


@tracer.capture_method(capture_response=False)
def delete_flow_segments(
    segments_table: "boto3.resources.factory.dynamodb.Table",
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
    pop_outliers(timerange_to_delete, query["Items"])
    if len(query["Items"]) > 0:
        delete_error = delete_segment_items(
            segments_table,
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
        query = segments_table.query(
            **args,
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
            update_flow_segments_updated(flow_id)
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
    put_deletion_request(del_queue, item_dict)


@tracer.capture_method(capture_response=False)
def delete_segment_items(
    segments_table: "boto3.resources.factory.dynamodb.Table",
    items: list[dict],
    object_ids: set[str],
) -> tuple[dict, None]:
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
def get_key_and_args(flow_id: str, parameters: None | dict) -> tuple[dict, bool]:
    """Generate key expression and args for a dynamodb query operation"""
    args = {
        "KeyConditionExpression": Key("flow_id").eq(flow_id),
        "ScanIndexForward": True,
        "Limit": constants.DEFAULT_PAGE_LIMIT,
    }
    if parameters is None:
        return args
    args["ScanIndexForward"] = (
        not parameters.get("reverse_order", "false").lower() == "true"
    )
    # Pagination query string parameters
    if "limit" in parameters:
        args["Limit"] = (
            int(parameters["limit"])
            if int(parameters["limit"]) < constants.MAX_PAGE_LIMIT
            else constants.MAX_PAGE_LIMIT
        )
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
    # Update Key Expression
    if "object_id" in parameters:
        args["IndexName"] = "object-id-index"
        args["KeyConditionExpression"] = And(
            args["KeyConditionExpression"],
            Key("object_id").eq(parameters["object_id"]),
        )
    elif timerange_filter and timerange_filter.start:
        args["KeyConditionExpression"] = And(
            args["KeyConditionExpression"],
            (
                Key("timerange_end").gte(timerange_filter.start.to_nanosec())
                if timerange_filter.includes_end()
                else Key("timerange_end").gt(timerange_filter.start.to_nanosec())
            ),
        )
    # Build Filter expression
    if timerange_filter:
        if timerange_filter.start and "object_id" in parameters:
            args["FilterExpression"] = (
                Attr("timerange_end").gte(timerange_filter.start.to_nanosec())
                if timerange_filter.includes_start()
                else Attr("timerange_end").gt(timerange_filter.start.to_nanosec())
            )
        if timerange_filter.end:
            args["FilterExpression"] = (
                Attr("timerange_start").lte(timerange_filter.end.to_nanosec())
                if timerange_filter.includes_end()
                else Attr("timerange_start").lt(timerange_filter.end.to_nanosec())
            )
    return args


@tracer.capture_method(capture_response=False)
def get_message_batches(items: list) -> list:
    """Split a list of items into a list of batches all smaller than the defined maximum message size"""
    if len(items) == 0:
        return []
    batch_count = math.ceil(
        len(json.dumps(items, default=str)) / constants.MAX_MESSAGE_SIZE
    )
    batch_size = math.ceil(len(items) / batch_count)
    return list(batched(items, batch_size))


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
def model_dump(model: BaseModel, **kwargs: None | dict) -> dict:
    """Dumps a pydantic model to a dict, removing null and other "empty" keys"""
    args = {"by_alias": True, "exclude_unset": True, "exclude_none": True, **kwargs}
    model_dict = model.model_dump(mode="json", **args)
    remove_null(model_dict)
    return model_dict


@tracer.capture_method(capture_response=False)
def model_dump_json(model: BaseModel | list[BaseModel], **kwargs: None | dict):
    """Dumps a pydantic model to a json string, removing null and other "empty" keys"""
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
    """Publishes the supplied events to an EventBridge EventBus"""
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
def put_deletion_request(queue: str, item: dict) -> None:
    """Publishs a message to SQS and inserts into dynamodb"""
    sqs.send_message(
        QueueUrl=queue,
        MessageBody=json.dumps(item),
    )
    merge_delete_request(item.copy())


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
def update_flow_segments_updated(flow_id: str) -> None:
    """Update the segments_updated field on the specified Flow"""
    try:
        item_dict = set_node_property_base(
            "flow",
            flow_id,
            {
                "flow.segments_updated": datetime.now()
                .astimezone(timezone.utc)
                .strftime(constants.DATETIME_FORMAT)
            },
        )
        publish_event("flows/updated", {"flow": item_dict}, [flow_id])
    except ValueError:
        return


@tracer.capture_method(capture_response=False)
def validate_query_string(
    params: None | dict, request_context: APIGatewayEventRequestContext
) -> bool:
    """Checks if supplied parameters are valid names for the path and method of the request"""
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


@tracer.capture_method(capture_response=False)
def serialise_neptune_obj(obj: dict) -> None:
    """Modifies Neptune JSON response back to hierachical JSON structure required by API"""
    for p in list(obj.keys()):
        if p.startswith("SERIALISE_"):
            prop_value = obj.pop(p)
            obj[p[len("SERIALISE_") :]] = json.loads(prop_value)
        elif isinstance(obj[p], dict):
            serialise_neptune_obj(obj[p])


@tracer.capture_method(capture_response=False)
# pylint: disable=dangerous-default-value
def generate_match_query(
    record_type: str, properties: dict, set_dict: dict = None, where_literals: list = []
) -> cymple.builder.NodeAvailable:
    """Returns an Open Cypher Match query to return specified data"""
    match record_type:
        case "source":
            query = (
                qb.match()
                .node(
                    ref_name="f", labels="flow", properties=properties.get("flow", {})
                )
                .related_to(label="represents")
                .node(
                    ref_name=record_type,
                    labels="source",
                    properties=properties.get("source", {}),
                )
                .related_to(
                    ref_name="t",
                    label="has_tags",
                    properties=properties.get("tags", {}),
                )
                .node(labels="tags")
            )
            if set_dict:
                query = query.set(set_dict).with_("*")
            if len(where_literals) > 0:
                query = query.where_literal(" AND ".join(where_literals))
            query = (
                query.match_optional()
                .node(ref_name="f")
                .related_from(ref_name="c", label="collected_by")
                .node(labels="flow")
                .related_to(label="represents")
                .node(ref_name="sc", labels="source")
                .match_optional()
                .node(ref_name="f")
                .related_to(label="collected_by")
                .node(labels="flow")
                .related_to(label="represents")
                .node(ref_name="cb", labels="source")
            )
            return query
        case "flow":
            query = (
                qb.match()
                .node(
                    ref_name=record_type,
                    labels="flow",
                    properties=properties.get("flow", {}),
                )
                .related_to(label="represents")
                .node(
                    ref_name="s",
                    labels="source",
                    properties=properties.get("source", {}),
                )
                .match()
                .node(ref_name=record_type)
                .related_to(
                    ref_name="e",
                    label="has_essence_parameters",
                    properties=properties.get("essence_parameters", {}),
                )
                .node(labels="essence_parameters")
                .match()
                .node(ref_name=record_type)
                .related_to(
                    ref_name="t",
                    label="has_tags",
                    properties=properties.get("tags", {}),
                )
                .node(labels="tags")
            )
            if set_dict:
                query = query.set(set_dict).with_("*")
            if len(where_literals) > 0:
                query = query.where_literal(" AND ".join(where_literals))
            query = (
                query.match_optional()
                .node(ref_name=record_type)
                .related_from(ref_name="c", label="collected_by")
                .node(ref_name="fc", labels="flow")
                .match_optional()
                .node(ref_name=record_type)
                .related_to(label="collected_by")
                .node(ref_name="cb", labels="flow")
            )
            return query
        case "delete_request":
            query = (
                qb.match()
                .node(
                    ref_name=record_type,
                    labels="delete_request",
                    properties=properties.get("delete_request", {}),
                )
                .related_to(
                    ref_name="e",
                    label="has_error",
                    properties=properties.get("error", {}),
                )
                .node(labels="error")
            )
            if set_dict:
                query = query.set(set_dict).with_("*")
            if len(where_literals) > 0:
                query = query.where_literal(" AND ".join(where_literals))
            return query


@tracer.capture_method(capture_response=False)
def check_node_exists(record_type: str, record_id: str) -> bool:
    """Checks whether the specified Node exists in the Neptune Database"""
    query = (
        qb.match()
        .node(ref_name="n", labels=record_type, properties={"id": record_id})
        .return_literal("n.id")
        .get()
    )
    results = neptune.execute_open_cypher_query(openCypherQuery=query)
    return False if len(results["results"]) == 0 else True


@tracer.capture_method(capture_response=False)
def query_node_tags(record_type: str, record_id: str) -> dict:
    """Returns the TAMS Tags for the specified Node"""
    query = (
        qb.match()
        .node(labels=record_type, properties={"id": record_id})
        .related_to(ref_name="t", label="has_tags")
        .node(labels="tags")
        .return_literal("t {.*} AS tags")
        .get()
    )
    results = neptune.execute_open_cypher_query(openCypherQuery=query)
    if len(results["results"]) == 0:
        raise ValueError("No results returned from the database query.")
    else:
        return results["results"][0]["tags"]


@tracer.capture_method(capture_response=False)
def query_node_property(record_type: str, record_id: str, prop_name: str) -> any:
    """Returns the value of the specified Node property"""
    query = (
        qb.match()
        .node(ref_name="n", labels=record_type, properties={"id": record_id})
        .return_literal(f"n.{prop_name} AS property")
        .get()
    )
    results = neptune.execute_open_cypher_query(openCypherQuery=query)
    if len(results["results"]) == 0:
        raise ValueError("No results returned from the database query.")
    else:
        return results["results"][0]["property"]


@tracer.capture_method(capture_response=False)
def query_node(record_type: str, record_id: str) -> dict:
    """Returns the specified Node from the Neptune Database"""
    query = (
        generate_match_query(record_type, {record_type: {"id": record_id}})
        .return_literal(constants.RETURN_LITERAL[record_type])
        .get()
    )
    results = neptune.execute_open_cypher_query(openCypherQuery=query)
    for result in results["results"]:
        serialise_neptune_obj(result[record_type])
    if len(results["results"]) == 0:
        raise ValueError("No results returned from the database query.")
    else:
        return results["results"][0][record_type]


@tracer.capture_method(capture_response=False)
def query_sources(parameters: dict) -> tuple[list, int]:
    """Returns a list of the TAMS Sources from the Neptune Database"""
    page = int(parameters.pop("page", 0))
    limit = int(parameters.pop("limit", constants.DEFAULT_PAGE_LIMIT))
    if limit > constants.MAX_PAGE_LIMIT:
        limit = constants.MAX_PAGE_LIMIT
    tag_properties = {}
    where_literals = []
    for key in list(parameters.keys()):
        if key.startswith("tag."):
            tag_properties[key.split(".", 1)[-1]] = parameters.pop(key)
        elif key.startswith("tag_exists."):
            value = parameters.pop(key).lower()
            if value in ["true", "false"]:
                if value == "true":
                    where_literals.append(f"t.{key.split(".", 1)[-1]} IS NOT NULL")
                else:
                    where_literals.append(f"t.{key.split(".", 1)[-1]} IS NULL")
    query = generate_match_query(
        "source",
        {
            "source": parameters,
            "tags": tag_properties,
        },
        where_literals=where_literals,
    )
    query = (
        query.return_literal(constants.RETURN_LITERAL["source"])
        .order_by("source.id")
        .skip(page)
        .limit(limit)
        .get()
    )
    results = neptune.execute_open_cypher_query(openCypherQuery=query)
    for result in results["results"]:
        serialise_neptune_obj(result["source"])
    next_page = page + limit if len(results["results"]) == limit else None
    return [item["source"] for item in results["results"]], next_page


@tracer.capture_method(capture_response=False)
def query_flows(parameters: dict) -> tuple[list, int]:
    """Returns a list of the TAMS Flows from the Neptune Database"""
    parameters.pop("timerange", None)
    page = int(parameters.pop("page", 0))
    limit = int(parameters.pop("limit", constants.DEFAULT_PAGE_LIMIT))
    if limit > constants.MAX_PAGE_LIMIT:
        limit = constants.MAX_PAGE_LIMIT
    source_id = parameters.pop("source_id", None)
    source_properties = {"id": source_id} if source_id else {}
    essence_properties = {}
    tag_properties = {}
    where_literals = []
    for key in list(parameters.keys()):
        if key in essence_params:
            value = parameters.pop(key)
            if essence_params[key] == "int":
                value = int(value)
            elif essence_params[key] == "float":
                value = float(value)
            elif essence_params[key] == "bool":
                value = value.lower() == "true"
            essence_properties[key] = value
        elif key.startswith("tag."):
            tag_properties[key.split(".", 1)[-1]] = parameters.pop(key)
        elif key.startswith("tag_exists."):
            value = parameters.pop(key).lower()
            if value in ["true", "false"]:
                if value == "true":
                    where_literals.append(f"t.{key.split(".", 1)[-1]} IS NOT NULL")
                else:
                    where_literals.append(f"t.{key.split(".", 1)[-1]} IS NULL")
    query = generate_match_query(
        "flow",
        {
            "flow": parameters,
            "source": source_properties,
            "essence_parameters": essence_properties,
            "tags": tag_properties,
        },
        where_literals=where_literals,
    )
    query = (
        query.return_literal(constants.RETURN_LITERAL["flow"])
        .order_by("flow.id")
        .skip(page)
        .limit(limit)
        .get()
    )
    results = neptune.execute_open_cypher_query(openCypherQuery=query)
    for result in results["results"]:
        serialise_neptune_obj(result["flow"])
    next_page = page + limit if len(results["results"]) == limit else None
    return [item["flow"] for item in results["results"]], next_page


@tracer.capture_method(capture_response=False)
def query_delete_requests() -> list:
    """Returns a list of the TAMS Delete Request from the Neptune Database"""
    query = generate_match_query(
        "delete_request",
        {},
    )
    query = (
        query.return_literal(constants.RETURN_LITERAL["delete_request"])
        .order_by("delete_request.id")
        .get()
    )
    results = neptune.execute_open_cypher_query(openCypherQuery=query)
    for result in results["results"]:
        serialise_neptune_obj(result["delete_request"])
    return [item["delete_request"] for item in results["results"]]


@tracer.capture_method(capture_response=False)
def merge_source(source_dict: dict) -> None:
    """Perform an OpenCypher Merge operation on the supplied TAMS Source record"""
    tags = source_dict.pop("tags", {})
    query = (
        qb.merge()
        .node(
            ref_name="s",
            labels="source",
            properties={"id": source_dict.pop("id")},
        )
        .set(
            {
                f"s.{k}": (
                    json.dumps(v) if isinstance(v, list) or isinstance(v, dict) else v
                )
                for k, v in source_dict.items()
            }
        )
        .merge()
        .node(labels="tags", ref_name="t")
        .merge()
        .node(ref_name="s")
        .related_to(label="has_tags", properties=tags)
        .node(ref_name="t")
        .get()
    )
    neptune.execute_open_cypher_query(openCypherQuery=query)


@tracer.capture_method(capture_response=False)
def merge_flow(flow_dict: dict) -> None:
    """Perform an OpenCypher Merge operation on the supplied TAMS Flow record"""
    tags = flow_dict.pop("tags", {})
    essence_parameters = flow_dict.pop("essence_parameters", {})
    flow_collection = flow_dict.pop("flow_collection", [])
    essence_parameters_properties = {
        (f"SERIALISE_{k}" if isinstance(v, list) or isinstance(v, dict) else k): (
            json.dumps(v) if isinstance(v, list) or isinstance(v, dict) else v
        )
        for k, v in essence_parameters.items()
    }
    query = (
        qb.match()
        .node(
            ref_name="s", labels="source", properties={"id": flow_dict.pop("source_id")}
        )
        .merge()
        .node(ref_name="f", labels="flow", properties={"id": flow_dict.pop("id")})
        .set(
            {
                (
                    f"f.SERIALISE_{k}"
                    if isinstance(v, list) or isinstance(v, dict)
                    else f"f.{k}"
                ): (json.dumps(v) if isinstance(v, list) or isinstance(v, dict) else v)
                for k, v in flow_dict.items()
            }
        )
        .merge()
        .node(labels="tags", ref_name="t")
        .merge()
        .node(ref_name="f")
        .related_to(label="has_tags", properties=tags)
        .node(ref_name="t")
        .merge()
        .node(labels="essence_parameters", ref_name="ep")
        .merge()
        .node(ref_name="f")
        .related_to(
            label="has_essence_parameters", properties=essence_parameters_properties
        )
        .node(ref_name="ep")
        .merge()
        .node(ref_name="f")
        .related_to(label="represents")
        .node(ref_name="s")
    )
    for n, collection in enumerate(flow_collection):
        node_ref = f"f{n}"
        collection_properties = {
            k: json.dumps(v) if isinstance(v, list) or isinstance(v, dict) else v
            for k, v in collection.items()
        }
        query = (
            qb.match().node(
                labels="flow",
                ref_name=node_ref,
                properties={"id": collection.pop("id")},
            )
            + query
            + qb.merge()
            .node(ref_name=node_ref)
            .related_to(label="collected_by", properties=collection_properties)
            .node(ref_name="f")
        )
    neptune.execute_open_cypher_query(openCypherQuery=query.get())


@tracer.capture_method(capture_response=False)
def merge_delete_request(delete_request_dict: dict) -> None:
    """Perform an OpenCypher Merge operation on the supplied TAMS Delete Request record"""
    error = delete_request_dict.pop("error", {})
    error_properties = {
        (f"SERIALISE_{k}" if isinstance(v, list) or isinstance(v, dict) else k): (
            json.dumps(v) if isinstance(v, list) or isinstance(v, dict) else v
        )
        for k, v in error.items()
    }
    query = (
        qb.merge()
        .node(
            ref_name="d",
            labels="delete_request",
            properties={"id": delete_request_dict.pop("id")},
        )
        .set(
            {
                f"d.{k}": (
                    json.dumps(v) if isinstance(v, list) or isinstance(v, dict) else v
                )
                for k, v in delete_request_dict.items()
            }
        )
        .merge()
        .node(labels="error", ref_name="e")
        .merge()
        .node(ref_name="d")
        .related_to(label="has_error", properties=error_properties)
        .node(ref_name="e")
        .get()
    )
    neptune.execute_open_cypher_query(openCypherQuery=query)


@tracer.capture_method(capture_response=False)
def delete_flow(flow_id: str) -> str | None:
    """Deletes the specified Flow from the Neptune Database"""
    query = (
        qb.match()
        .node(ref_name="flow", labels="flow", properties={"id": flow_id})
        .related_to(label="represents")
        .node(ref_name="s", labels="source")
        .detach_delete(ref_name="flow")
        .return_literal("s.id AS source_id")
        .get()
    )
    results = neptune.execute_open_cypher_query(openCypherQuery=query)
    if len(results["results"]) == 0:
        return None
    else:
        return results["results"][0]["source_id"]


@tracer.capture_method(capture_response=False)
def set_node_property_base(record_type: str, record_id: str, props: dict) -> dict:
    """Performs an OpenCypher Set operation on the specified Node and properties"""
    query = (
        generate_match_query(
            record_type,
            {record_type: {"id": record_id}},
            set_dict=props,
        )
        .return_literal(constants.RETURN_LITERAL[record_type])
        .get()
    )
    results = neptune.execute_open_cypher_query(openCypherQuery=query)
    for result in results["results"]:
        serialise_neptune_obj(result[record_type])
    if len(results["results"]) == 0:
        raise ValueError("No results returned from the database query.")
    else:
        return results["results"][0][record_type]


@tracer.capture_method(capture_response=False)
def set_node_property(
    record_type: str, record_id: str, username: str, props: dict
) -> dict:
    """Performs an OpenCypher Set operation on the specified Node and properties with the addition of updated and updated_by properties"""
    meta_props = {
        **props,
        f"{record_type}.{"metadata_" if record_type == "flow" else ""}updated": datetime.now()
        .astimezone(timezone.utc)
        .strftime(constants.DATETIME_FORMAT),
        f"{record_type}.updated_by": username,
    }
    return set_node_property_base(record_type, record_id, meta_props)
