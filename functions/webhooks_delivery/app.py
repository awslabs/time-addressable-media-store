import json

from aws_lambda_powertools import Logger, Metrics, Tracer, single_metric
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.data_classes.event_bridge_event import (
    EventBridgeEvent,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
from requests import Session
from requests.adapters import HTTPAdapter, Retry
from schema import Flow, Flowsegment, Source, Webhook
from utils import model_dump

tracer = Tracer()
logger = Logger()
metrics = Metrics()


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
# pylint: disable=unused-argument
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    message: SQSEvent = SQSEvent(event)
    for record in message.records:
        body = json.loads(record.body)
        event = EventBridgeEvent(body["event"])
        webhook = Webhook(**body["item"])
        get_urls = body["get_urls"]
        retries = Retry(
            total=5,
            backoff_factor=0.1,
            status_forcelist=[500],
            allowed_methods=["POST"],
        )
        s = Session()
        headers = {"Content-Type": "application/json"}
        if webhook.api_key_name and webhook.api_key_value:
            headers[webhook.api_key_name] = webhook.api_key_value
        s.mount(webhook.url, HTTPAdapter(max_retries=retries))
        # Use associated model to clean the response data
        match event.detail_type:
            case "flows/created" | "flows/updated":
                event.detail["flow"] = model_dump(Flow(**event.detail["flow"]))
            case "source/created" | "source/updated":
                event.detail["source"] = model_dump(Source(**event.detail["source"]))
            case "flows/segments_added":
                event.detail["segments"][0]["get_urls"] = get_urls
                event.detail["segments"][0] = model_dump(
                    Flowsegment(**event.detail["segments"][0])
                )
        response = s.post(
            webhook.url,
            headers=headers,
            json={
                "event_timestamp": event.time,
                "event_type": event.detail_type,
                "event": event.detail,
            },
            timeout=30,
        )
        with single_metric(
            name=f"StatusCode-{response.status_code}",
            unit=MetricUnit.Count,
            value=1,
        ) as metric:
            metric.add_dimension(name="url", value=webhook.url)
            metric.add_dimension(name="event_type", value=event.detail_type)
        logger.info(f"Status Code: {response.status_code}", response_text=response.text)
