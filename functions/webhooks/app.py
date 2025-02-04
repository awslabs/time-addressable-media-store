import os

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer, single_metric
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.data_classes.event_bridge_event import (
    EventBridgeEvent,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from requests import Session
from requests.adapters import HTTPAdapter, Retry
from schema import Webhook

tracer = Tracer()
logger = Logger()
metrics = Metrics(namespace="Powertools")

dynamodb = boto3.resource("dynamodb")
webhooks_table = dynamodb.Table(os.environ["WEBHOOKS_TABLE"])

event_mappings = {
    "flows/created": ("flow_ids", ["flow", "id"]),
    "flows/updated": ("flow_ids", ["flow", "id"]),
    "flows/deleted": ("flow_ids", ["flow_id"]),
    "flows/segments_added": ("flow_ids", ["flow_id"]),
    "flows/segments_deleted": ("flow_ids", ["flow_id"]),
    "sources/created": ("source_ids", ["source", "id"]),
    "sources/updated": ("source_ids", ["source", "id"]),
    "sources/deleted": ("source_ids", ["source_id"]),
}


@tracer.capture_lambda_handler(capture_response=False)
def get_event_key(event, keys):
    response = event
    for key in keys:
        response = response.get(key, None)
        if not response:
            return None
    return response


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
# pylint: disable=unused-argument
def lambda_handler(event: dict, context: LambdaContext):
    event: EventBridgeEvent = EventBridgeEvent(event)
    query = webhooks_table.query(
        KeyConditionExpression=Key("event").eq(event.detail_type),
    )
    items = query["Items"]
    while "LastEvaluatedKey" in query:
        query = webhooks_table.query(
            KeyConditionExpression=Key("event").eq(event.detail_type),
            ExclusiveStartKey=query["LastEvaluatedKey"],
        )
        items.extend(query["Items"])
    schema_items = [Webhook(**item, events=[item["event"]]) for item in items]
    for item in schema_items:
        webhook_attr, event_keys = event_mappings[event.detail_type]
        item_ids = getattr(item, webhook_attr, None)
        event_id = get_event_key(event.detail, event_keys)
        # Post event if no relevant filters are supplied
        if not item_ids:
            post_event(event, item)
        # Post event if event_id is found within filters supplied
        elif event_id in item_ids:
            post_event(event, item)
        else:
            logger.info("No matching webhook definition found")


def post_event(event, item):
    retries = Retry(
        total=5,
        backoff_factor=0.1,
        status_forcelist=[500],
        allowed_methods=["POST"],
    )
    s = Session()
    headers = {"Content-Type": "application/json"}
    if item.api_key_name and item.api_key_value:
        headers[item.api_key_name] = item.api_key_value
    s.mount(item.url, HTTPAdapter(max_retries=retries))
    response = s.post(
        item.url,
        headers=headers,
        json={
            "event_timestamp": event.time,
            "event_type": event.detail_type,
            "event": event.detail,
        },
        timeout=30,
    )
    with single_metric(
        namespace="Powertools",
        name=f"StatusCode-{response.status_code}",
        unit=MetricUnit.Count,
        value=1,
    ) as metric:
        metric.add_dimension(name="url", value=item.url)
        metric.add_dimension(name="event_type", value=event.detail_type)
    logger.info(f"Status Code: {response.status_code}")
    logger.info(response.text)
