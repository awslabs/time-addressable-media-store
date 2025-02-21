import os
from http import HTTPStatus

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, CORSConfig
from aws_lambda_powertools.event_handler.exceptions import NotFoundError
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from schema import (
    Eventstreamcommon,
    MediaStore,
    Service,
    Servicepost,
    Webhook,
    Webhookpost,
)
from utils import filter_dict, model_dump

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(enable_validation=True, cors=CORSConfig())
metrics = Metrics(namespace="Powertools")


dynamodb = boto3.resource("dynamodb")
table_name = os.environ.get("WEBHOOKS_TABLE", None)


@app.get("/service")
@tracer.capture_method(capture_response=False)
def get_service():
    try:
        stage_variables = app.current_event.stage_variables
        service = Service(
            type="urn:x-tams:service.example",
            api_version=stage_variables["api_version"],
            service_version=stage_variables["service_version"],
            media_store=MediaStore(type="http_object_store"),
        )
        if "name" in stage_variables:
            service.name = stage_variables["name"]
        if "description" in stage_variables:
            service.description = stage_variables["description"]
        webhooks_enabled = (
            stage_variables.get("webhooks_enabled", "false").lower() == "yes"
        )
        if webhooks_enabled:
            if service.event_stream_mechanisms:
                service.event_stream_mechanisms.append(
                    Eventstreamcommon(name="webhooks")
                )
            else:
                service.event_stream_mechanisms = [Eventstreamcommon(name="webhooks")]
        return model_dump(service), HTTPStatus.OK.value  # 200
    except Exception as e:
        raise NotFoundError from e  # 404


@app.post("/service")
@tracer.capture_method(capture_response=False)
def post_service(service_post: Servicepost):
    patch_operations = []
    if service_post.name is not None:
        operation = {
            "path": "/variables/name",
            "op": "remove" if service_post.name == "" else "replace",
        }
        if service_post.name != "":
            operation["value"] = service_post.name
        patch_operations.append(operation)
    if service_post.description is not None:
        operation = {
            "path": "/variables/description",
            "op": "remove" if service_post.description == "" else "replace",
        }
        if service_post.description != "":
            operation["value"] = service_post.description
        patch_operations.append(operation)
    agw = boto3.client("apigateway")
    agw.update_stage(
        restApiId=app.current_event.request_context.api_id,
        stageName=app.current_event.request_context.stage,
        patchOperations=patch_operations,
    )
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
def post_webhooks(webhook: Webhookpost):
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


@logger.inject_lambda_context(
    log_event=True, correlation_id_path=correlation_paths.API_GATEWAY_REST
)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)


@tracer.capture_method(capture_response=False)
def model_dump_webhook(webhook: Webhook):
    """Custom model dump to retain empty values from webhook"""
    return webhook.model_dump(by_alias=True, exclude_unset=True)
