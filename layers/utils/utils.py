import json
import math
import os
import urllib.parse
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from itertools import batched

import boto3

# pylint: disable=no-member
import constants
from aws_lambda_powertools import Tracer
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from aws_lambda_powertools.utilities.data_classes.api_gateway_proxy_event import (
    APIGatewayEventRequestContext,
)
from botocore.config import Config
from botocore.exceptions import ClientError
from mediatimestamp.immutable import TimeRange
from params import essence_params, query_params
from pydantic import BaseModel

tracer = Tracer()

events = boto3.client("events")
sqs = boto3.client("sqs")
lmda = boto3.client("lambda")
s3 = boto3.client(
    "s3", config=Config(s3={"addressing_style": "virtual"})
)  # Addressing style is required to ensure pre-signed URLs work as soon as the bucket is created.
idp = boto3.client("cognito-idp")
ssm = boto3.client("ssm")
user_pool_id = os.environ.get("USER_POOL_ID", "")
info_param_name = os.environ.get("SERVICE_INFO_PARAMETER", "")
cognito_lambda = os.environ.get("COGNITO_LAMBDA_NAME", "")


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
        "created_by": get_username(parse_claims(request_context)),
    }


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
def put_message_batches(queue: str, items: list) -> list:
    """Batch sends a list of message items to the specified queue url"""
    for message in get_message_batches(items):
        sqs.send_message(
            QueueUrl=queue,
            MessageBody=json.dumps(message),
        )


@tracer.capture_method(capture_response=False)
def parse_claims(request_context: APIGatewayEventRequestContext) -> tuple[str, str]:
    """Extract just the username and client_id values from the authorizer claims"""
    return (
        request_context.authorizer.claims.get("username", ""),
        request_context.authorizer.claims.get("client_id", ""),
    )


@tracer.capture_method(capture_response=False)
@lru_cache()
def get_store_name() -> str:
    """Parse store name from SSM parameter value or return default if not found"""
    get_parameter = ssm.get_parameter(Name=info_param_name)
    print(get_parameter)
    service_dict = json.loads(get_parameter["Parameter"]["Value"])
    if service_dict.get("name") is None:
        return "tams"
    return service_dict["name"]


@tracer.capture_method(capture_response=False)
@lru_cache()
def get_user_pool() -> dict:
    """Retrieve the user pool details"""
    return idp.describe_user_pool(UserPoolId=user_pool_id)["UserPool"]


@tracer.capture_method(capture_response=False)
@lru_cache()
def get_username(claims_tuple: tuple[str, str]) -> str:
    """Dervive a suitable username from the API Gateway request details"""
    invoke = lmda.invoke(
        FunctionName=cognito_lambda,
        InvocationType="RequestResponse",
        LogType="None",
        Payload=json.dumps(
            {
                "username": claims_tuple[0],
                "client_id": claims_tuple[1],
            }
        ),
    )
    if invoke["StatusCode"] != 200:
        raise ClientError(
            operation_name="LambdaInvoke", error_response=invoke["FunctionError"]
        )
    return json.loads(invoke["Payload"].read().decode("utf-8"))


@tracer.capture_method(capture_response=False)
def model_dump(
    model: BaseModel | list[BaseModel], **kwargs: None | dict
) -> dict | list:
    """Dumps a pydantic model to a dict, removing null and other "empty" keys"""
    if isinstance(model, list):
        model_dict = [model_dump(m, **kwargs) for m in model]
    else:
        args = {"by_alias": True, "exclude_unset": True, "exclude_none": True, **kwargs}
        model_dict = model.model_dump(mode="json", **args)
        remove_null(model_dict)
    return model_dict


@tracer.capture_method(capture_response=False)
def pop_outliers(timerange: TimeRange, items: list) -> list:
    """Remove ends of a list of Timerange items if they do not fully cover the supplied Timerange"""
    if len(items) > 1:
        if not timerange.contains_subrange(TimeRange.from_str(items[-1]["timerange"])):
            return items[:-1]
    if len(items) > 0:
        if not timerange.contains_subrange(TimeRange.from_str(items[0]["timerange"])):
            return items[1:]
    return items


@tracer.capture_method(capture_response=False)
def remove_null(obj: dict | list) -> None:
    """Removes null and other "empty" keys from a dict/list recursively"""
    if isinstance(obj, list):
        for i in obj:
            remove_null(i)
    elif isinstance(obj, dict):
        for k, v in list(obj.items()):
            if v is None or v == {} or v == []:
                obj.pop(k)
            elif isinstance(v, str):
                for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f"):
                    try:
                        dt = datetime.strptime(v, fmt)
                        obj[k] = dt.astimezone(timezone.utc).strftime(
                            constants.DATETIME_FORMAT
                        )
                    except ValueError:
                        pass
            else:
                remove_null(v)


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
def json_number(x: any) -> float | int:
    """Returns a numeric value as the int of float based upon whether it contains a decimal point"""
    f = float(x)
    if f.is_integer():
        return int(f)
    return f


@tracer.capture_method(capture_response=False)
def serialise_neptune_obj(obj: dict, key_prefix: str = "") -> dict:
    """Return a new dict with properties of type dict/list serialised into string"""
    serialised = {}
    for k, v in obj.items():
        if isinstance(v, (list, dict)):
            serialised[f"{key_prefix}{constants.SERIALISE_PREFIX}{k}"] = json.dumps(
                v, default=json_number
            )
        else:
            serialised[f"{key_prefix}{k}"] = v
    return serialised


@tracer.capture_method(capture_response=False)
def deserialise_neptune_obj(obj: dict) -> dict:
    """Return a new dict with serialised properties deserialised into dict/list"""
    deserialised = {}
    for prop_name, prop_value in obj.items():
        if prop_name.startswith(constants.SERIALISE_PREFIX):
            actual_name = prop_name[len(constants.SERIALISE_PREFIX) :]
            deserialised[actual_name] = json.loads(prop_value)
        elif isinstance(prop_value, dict):
            deserialised[prop_name] = deserialise_neptune_obj(prop_value)
        else:
            deserialised[prop_name] = prop_value
    return deserialised


@tracer.capture_method(capture_response=False)
def parse_parameters(parameters: dict) -> tuple[int, int, dict, list]:
    """Parses API Gateway parameters into the structure used by OpenCypher query"""
    source_id = parameters.get("source_id", None)
    page = int(parameters.get("page", 0))
    limit = min(
        int(parameters.get("limit", constants.DEFAULT_PAGE_LIMIT)),
        constants.MAX_PAGE_LIMIT,
    )
    where_literals = []
    return_dict = {
        "source_properties": {"id": source_id} if source_id else {},
        "properties": {},
        "essence_properties": {},
        "tag_properties": {},
    }
    for key, value in parameters.items():
        if key not in ["page", "limit", "source_id", "timerange"]:
            if key in essence_params:
                if essence_params[key] == "int":
                    return_dict["essence_properties"][key] = int(value)
                elif essence_params[key] == "float":
                    return_dict["essence_properties"][key] = float(value)
                elif essence_params[key] == "bool":
                    return_dict["essence_properties"][key] = value.lower() == "true"
            elif key.startswith("tag.") or key.startswith("tag_exists."):
                tag_name = key.split(".", 1)[-1]
                if key.startswith("tag."):
                    return_dict["tag_properties"][tag_name] = value
                elif key.startswith("tag_exists."):
                    if value.lower() in ["true", "false"]:
                        if value.lower() == "true":
                            where_literals.append(f"t.{tag_name} IS NOT NULL")
                        else:
                            where_literals.append(f"t.{tag_name} IS NULL")
            else:
                return_dict["properties"][key] = value
    return page, limit, return_dict, where_literals


@tracer.capture_method(capture_response=False)
def filter_dict(obj: dict, keys: set) -> dict:
    """Returns a dictionary with specific keys removed"""
    return {k: v for k, v in obj.items() if k not in keys}


@tracer.capture_method(capture_response=False)
def publish_event(detail_type: str, details: dict, resources) -> None:
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
def put_message(queue: str, item: dict) -> None:
    """Publishs a message to SQS"""
    print("cde")
    sqs.send_message(
        QueueUrl=queue,
        MessageBody=json.dumps(item),
    )


@tracer.capture_method(capture_response=False)
def check_object_exists(bucket, object_id: str) -> bool:
    """Checks whether the specified object_id (as key) currently exists in the specified S3 Bucket"""
    try:
        s3.head_object(Bucket=bucket, Key=object_id)
        return True
    except ClientError:
        return False


@tracer.capture_method(capture_response=False)
def generate_presigned_url(
    method: str, bucket: str, key: str, **kwargs: None | dict
) -> str:
    """Generates an S3 pre-signed URL"""
    url = s3.generate_presigned_url(
        ClientMethod=method,
        Params={
            "Bucket": bucket,
            "Key": key,
            **kwargs,
        },
        ExpiresIn=3600,
    )
    return url
