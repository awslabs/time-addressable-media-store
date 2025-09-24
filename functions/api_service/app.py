import os
from http import HTTPStatus

import boto3
import constants
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, CORSConfig
from aws_lambda_powertools.event_handler.exceptions import BadRequestError
from aws_lambda_powertools.event_handler.openapi.exceptions import (
    RequestValidationError,
)
from aws_lambda_powertools.event_handler.openapi.params import Body
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from dynamodb import get_storage_backend_dict, get_store_name
from neptune import delete_webhook, merge_webhook, query_node, query_webhooks
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
from utils import model_dump

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(
    enable_validation=True, cors=CORSConfig(expose_headers=["*"])
)
metrics = Metrics()

record_type = "webhook"
dynamodb = boto3.resource("dynamodb")
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
    service = Service(
        type="urn:x-tams:service.example",
        api_version=stage_variables["api_version"],
        service_version=stage_variables["service_version"],
        **get_item.get("Item", {}),
    )
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
    items = query_webhooks()
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return (
        model_dump([Webhook(**item) for item in items]),
        HTTPStatus.OK.value,
    )  # 200


@app.post("/service/webhooks")
@tracer.capture_method(capture_response=False)
def post_webhooks(webhook: Annotated[Webhookpost, Body()]):

    if len(webhook.events) == 0:
        delete_webhook(webhook.url)
        return None, HTTPStatus.NO_CONTENT.value  # 204
    try:
        existing_item = query_node(record_type, webhook.url, "url")
    except ValueError:
        existing_item = {}
    item_dict = model_dump(Webhook(**merge_webhook(model_dump(webhook), existing_item)))
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
