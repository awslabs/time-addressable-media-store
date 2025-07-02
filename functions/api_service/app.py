import os
from http import HTTPStatus

import boto3
import constants
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, CORSConfig
from aws_lambda_powertools.event_handler.exceptions import (
    BadRequestError,
    NotFoundError,
)
from aws_lambda_powertools.event_handler.openapi.exceptions import (
    RequestValidationError,
)
from aws_lambda_powertools.event_handler.openapi.params import Body
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from dynamodb import get_storage_backend_dict, get_store_name
from schema import (
    Eventstreamcommon,
    Service,
    Servicepost,
    Storagebackendslist,
    StoragebackendslistItem,
    Webhook,
    Webhookpost,
)
from typing_extensions import Annotated
from utils import filter_dict, model_dump

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(
    enable_validation=True, cors=CORSConfig(expose_headers=["*"])
)
metrics = Metrics()

dynamodb = boto3.resource("dynamodb")
table_name = os.environ.get("WEBHOOKS_TABLE", None)
service_table = dynamodb.Table(os.environ["SERVICE_TABLE"])


@app.head("/")
@app.get("/")
@tracer.capture_method(capture_response=False)
def get_root():
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return [
        "service",
        "flows",
        "sources",
        "objects",
        "flow-delete-requests",
    ], HTTPStatus.OK.value  # 200


@app.head("/service")
@app.get("/service")
@tracer.capture_method(capture_response=False)
def get_service():
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    get_item = service_table.get_item(
        Key={"record_type": "service", "id": constants.SERVICE_INFO_ID}
    )
    stage_variables = app.current_event.stage_variables
    webhooks_enabled = stage_variables.get("webhooks_enabled", "false").lower() == "yes"
    service = Service(
        type="urn:x-tams:service.example",
        api_version=stage_variables["api_version"],
        service_version=stage_variables["service_version"],
        **get_item.get("Item", {}),
    )
    if webhooks_enabled:
        service.event_stream_mechanisms = [Eventstreamcommon(name="webhooks")]
    return model_dump(service), HTTPStatus.OK.value  # 200


@app.post("/service")
@tracer.capture_method(capture_response=False)
def post_service(service_post: Annotated[Servicepost, Body()]):
    get_item = service_table.get_item(
        Key={"record_type": "service", "id": constants.SERVICE_INFO_ID}
    )
    service_record = get_item.get(
        "Item", {"record_type": "service", "id": constants.SERVICE_INFO_ID}
    )
    if service_post.name == "":
        del service_record["name"]
    if service_post.description == "":
        del service_record["description"]
    if service_post.name:
        service_record["name"] = service_post.name
    if service_post.description:
        service_record["description"] = service_post.description
    service_table.put_item(Item=service_record)
    return None, HTTPStatus.OK.value  # 200


@app.head("/service/webhooks")
@app.get("/service/webhooks")
@tracer.capture_method(capture_response=False)
def get_webhooks():
    if table_name is None:
        raise NotFoundError(
            "Webhooks are not supported by this API implementation"
        )  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    webhooks_table = dynamodb.Table(table_name)
    scan = webhooks_table.scan()
    # Group records to match API schema
    schema_dict = {}
    for item in scan["Items"]:
        if "api_key_value" in item:
            item.pop("api_key_value")  # api key value not to be returnable as per spec.
        event = item.pop("event")
        if item["url"] in schema_dict:
            schema_dict[item["url"]]["events"].append(event)
        else:
            schema_dict[item["url"]] = {**item, "events": [event]}
    return [
        model_dump_webhook(Webhook(**item)) for item in schema_dict.values()
    ], HTTPStatus.OK.value  # 200


@app.post("/service/webhooks")
@tracer.capture_method(capture_response=False)
def post_webhooks(webhook: Annotated[Webhookpost, Body()]):
    if table_name is None:
        raise NotFoundError(
            "Webhooks are not supported by this API implementation"
        )  # 404
    webhooks_table = dynamodb.Table(table_name)
    query = webhooks_table.query(
        IndexName="url-index",
        KeyConditionExpression=Key("url").eq(webhook.url),
    )
    existing_events = [item["event"] for item in query["Items"]]
    delete_events = [
        {"event": e, "url": webhook.url}
        for e in existing_events
        if e not in webhook.events
    ]
    item_dict = model_dump_webhook(webhook)
    with webhooks_table.batch_writer() as batch:
        for event in webhook.events:
            batch.put_item(
                Item={
                    **filter_dict(item_dict, {"events"}),
                    "event": event,
                }
            )
        for item in delete_events:
            batch.delete_item(Key=item)
    if len(webhook.events) == 0:
        return None, HTTPStatus.NO_CONTENT.value  # 204
    return item_dict, HTTPStatus.CREATED.value  # 201


@app.head("/service/storage-backends")
@app.get("/service/storage-backends")
@tracer.capture_method(capture_response=False)
def get_storage_backends():
    args = {"KeyConditionExpression": Key("record_type").eq("storage-backend")}
    query = service_table.query(**args)
    items = query["Items"]
    while "LastEvaluatedKey" in query:
        args["ExclusiveStartKey"] = query["LastEvaluatedKey"]
        query = service_table.query(**args)
        items.extend(query["Items"])
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    store_name = get_store_name()
    return model_dump(
        Storagebackendslist(
            [
                StoragebackendslistItem(**get_storage_backend_dict(item, store_name))
                for item in items
            ]
        )
    )


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
def model_dump_webhook(webhook: Webhook):
    """Custom model dump to retain empty values from webhook"""
    return webhook.model_dump(by_alias=True, exclude_unset=True)
