import os
from http import HTTPStatus
from typing import Optional

import boto3
import constants
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
)
from aws_lambda_powertools.event_handler.openapi.exceptions import (
    RequestValidationError,
)
from aws_lambda_powertools.event_handler.openapi.params import Body, Path, Query
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from dynamodb import list_storage_backends
from neptune import (
    check_node_exists,
    delete_webhook,
    merge_webhook,
    query_node,
    query_webhooks,
)
from schema import (
    Eventstreamcommon,
    Service,
    Servicepost,
    Storagebackendslist,
    StoragebackendslistItem,
    Uuid,
    Webhookget,
    Webhookpost,
    Webhookput,
)
from typing_extensions import Annotated
from utils import generate_link_url, model_dump

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(
    enable_validation=True, cors=CORSConfig(expose_headers=["*"])
)
metrics = Metrics()

record_type = "webhook"
dynamodb = boto3.resource("dynamodb")
service_table = dynamodb.Table(os.environ["SERVICE_TABLE"])

UUID_PATTERN = Uuid.model_fields["root"].metadata[0].pattern


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
def get_webhooks(
    param_page: Annotated[Optional[str], Query(alias="page")] = None,
    param_limit: Annotated[Optional[int], Query(alias="limit", gt=0)] = None,
):
    custom_headers = {}
    items, next_page, limit_used = query_webhooks(
        {
            "page": param_page,
            "limit": param_limit,
        }
    )
    if next_page:
        custom_headers["X-Paging-NextKey"] = str(next_page)
        custom_headers["Link"] = generate_link_url(app.current_event, str(next_page))
    if next_page or limit_used != param_limit:
        custom_headers["X-Paging-Limit"] = str(limit_used)
    if app.current_event.request_context.http_method == "HEAD":
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            body=None,
            headers=custom_headers,
        )
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=model_dump([Webhookget(**item) for item in items]),
        headers=custom_headers,
    )


@app.post("/service/webhooks")
@tracer.capture_method(capture_response=False)
def post_webhooks(webhook: Annotated[Webhookpost, Body()]):
    webhook_dict = webhook.model_dump(mode="json")
    # Set default status if None
    if not webhook_dict.get("status"):
        webhook_dict["status"] = "created"
    webhook_put = Webhookput(
        **webhook_dict,
        id=app.current_event.request_context.request_id,
    )
    item_dict = model_dump(
        Webhookget(**merge_webhook(webhook_put.model_dump(mode="json"), None))
    )
    return item_dict, HTTPStatus.CREATED.value  # 201


@app.head("/service/webhooks/<webhookId>")
@app.get("/service/webhooks/<webhookId>")
@tracer.capture_method(capture_response=False)
def get_webhook_by_id(
    webhook_id: Annotated[str, Path(alias="webhookId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, webhook_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested Webhook ID in the path is invalid."
        ) from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump(Webhookget(**item)), HTTPStatus.OK.value  # 200


@app.put("/service/webhooks/<webhookId>")
@tracer.capture_method(capture_response=False)
def put_webhook_by_id(
    webhook: Annotated[Webhookput, Body()],
    webhook_id: Annotated[str, Path(alias="webhookId", pattern=UUID_PATTERN)],
):
    if webhook.id.root != webhook_id:
        raise NotFoundError("The requested Webhook ID in the path is invalid.")  # 404
    try:
        existing_item = query_node(record_type, webhook_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested Webhook ID in the path is invalid."
        ) from e  # 404
    if (
        webhook.status
        and webhook.status.value == "disabled"
        and existing_item["status"] == "error"
    ):
        raise BadRequestError(
            "Bad request. The Webhook is currently in an error status and therefore cannot be updated to disabled."
        )  # 400
    updated_webhook = merge_webhook(model_dump(webhook), existing_item)
    return model_dump(Webhookget(**updated_webhook)), HTTPStatus.CREATED.value  # 201


@app.delete("/service/webhooks/<webhookId>")
@tracer.capture_method(capture_response=False)
def delete_webhook_by_id(
    webhook_id: Annotated[str, Path(alias="webhookId", pattern=UUID_PATTERN)],
):
    if not check_node_exists(record_type, webhook_id):
        raise NotFoundError("The requested Webhook ID in the path is invalid.")  # 404
    delete_webhook(webhook_id)
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/service/storage-backends")
@app.get("/service/storage-backends")
@tracer.capture_method(capture_response=False)
def get_storage_backends():
    storage_backends = list_storage_backends()
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump(
        Storagebackendslist(
            [StoragebackendslistItem(**item) for item in storage_backends]
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
