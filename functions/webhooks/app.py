import os

import boto3
import requests
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.data_classes.event_bridge_event import (
    EventBridgeEvent,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key

tracer = Tracer()
logger = Logger()
metrics = Metrics(namespace="Powertools")

dynamodb = boto3.resource("dynamodb")
webhooks_table = dynamodb.Table(os.environ["WEBHOOKS_TABLE"])


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
    for item in items:
        headers = {
            "Content-Type": "application/json",
            item["api_key_name"]: item["api_key_value"],
        }
        response = requests.post(
            item["url"],
            headers=headers,
            json={
                "event_timestamp": event.time,
                "event_type": event.detail_type,
                "event": event.detail,
            },
            timeout=30,
        )
        logger.info(f"Status Code: {response.status_code}")
