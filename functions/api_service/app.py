import json
import os
from http import HTTPStatus

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, CORSConfig
from aws_lambda_powertools.event_handler.exceptions import NotFoundError
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.parser import parse
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from schema import Service, Servicepost, Webhookpost
from utils import get_clean_item

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(enable_validation=True, cors=CORSConfig())
metrics = Metrics(namespace="Powertools")


dynamodb = boto3.resource("dynamodb")
webhooks_enabled = os.environ["WEBHOOKS_ENABLED"].lower() == "yes"
table = dynamodb.Table(os.environ["TABLE"])
record_type = "service"
webhooks_table = dynamodb.Table(os.environ["WEBHOOKS_TABLE"])


@app.get("/service")
@tracer.capture_method(capture_response=False)
def get_service():
    item = table.get_item(
        Key={"record_type": record_type, "id": "1"}, ProjectionExpression="information"
    )
    if "Item" not in item:
        raise NotFoundError  # 404
    service: Service = parse(event=item["Item"]["information"], model=Service)
    if webhooks_enabled:
        if service.event_stream_mechanisms:
            service.event_stream_mechanisms.append({"name": "webhooks"})
        else:
            service.event_stream_mechanisms = [{"name": "webhooks"}]
    return get_clean_item(service), HTTPStatus.OK.value  # 200


@app.post("/service")
@tracer.capture_method(capture_response=False)
def post_service(service_post: Servicepost):
    item = table.get_item(
        Key={"record_type": record_type, "id": "1"}, ProjectionExpression="information"
    )
    if "Item" not in item:
        raise NotFoundError  # 404
    service: Service = parse(event=item["Item"]["information"], model=Service)
    service.name = service_post.name
    service.description = service_post.description
    item_dict = get_clean_item(service)
    table.put_item(
        Item={"record_type": record_type, "id": "1", "information": item_dict}
    )
    return None, HTTPStatus.OK.value  # 200


@app.route("/service/webhooks", method=["HEAD"])
@app.get("/service/webhooks")
@tracer.capture_method(capture_response=False)
def get_webhooks():
    if not webhooks_enabled:
        raise NotFoundError(
            "Webhooks are not supported by this API implementation"
        )  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    scan = webhooks_table.scan()
    # Group records to match API schema
    schema_dict = {}
    for item in scan["Items"]:
        item.pop("api_key_value")  # api key value not to be returnable as per spec.
        event = item.pop("event")
        if item["url"] in schema_dict:
            schema_dict[item["url"]]["events"].append(event)
        else:
            schema_dict[item["url"]] = {**item, "events": [event]}
    return (
        json.dumps(list(schema_dict.values())),
        HTTPStatus.OK.value,
    )  # 200


@app.post("/service/webhooks")
@tracer.capture_method(capture_response=False)
def post_webhooks(webhook: Webhookpost):
    if not webhooks_enabled:
        raise NotFoundError(
            "Webhooks are not supported by this API implementation"
        )  # 404
    item_dict = get_clean_item(webhook)
    request_events = []
    if "events" in item_dict:
        request_events = item_dict.pop("events")
    query = webhooks_table.query(
        IndexName="url-index",
        KeyConditionExpression=Key("url").eq(item_dict["url"]),
    )
    existing_events = [item["event"] for item in query["Items"]]
    delete_events = [
        {"event": e, "url": item_dict["url"]}
        for e in existing_events
        if e not in request_events
    ]
    with webhooks_table.batch_writer() as batch:
        for event in request_events:
            batch.put_item(Item={**item_dict, "event": event})
        for item in delete_events:
            batch.delete_item(Key=item)
    if len(request_events) == 0:
        return None, HTTPStatus.NO_CONTENT.value  # 204
    return get_clean_item(webhook), HTTPStatus.CREATED.value  # 201


@logger.inject_lambda_context(
    log_event=True, correlation_id_path=correlation_paths.API_GATEWAY_REST
)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)
