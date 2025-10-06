import json
import os
import traceback
from datetime import datetime

from aws_lambda_powertools import Logger, Metrics, Tracer, single_metric
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.event_bridge_event import (
    EventBridgeEvent,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSEvent, SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from requests import Session
from requests.adapters import HTTPAdapter, Retry
from schema import Error, Flow, Flowsegment, Source
from schema_extra import Webhookfull
from utils import model_dump, put_message

tracer = Tracer()
logger = Logger()
metrics = Metrics()
batch_processor = BatchProcessor(event_type=EventType.SQS)

error_queue = os.environ["ERROR_QUEUE_URL"]


@tracer.capture_method(capture_response=False)
def record_handler(record: SQSRecord) -> None:
    """Processes a single SQS record"""
    body = json.loads(record.body)
    event = EventBridgeEvent(body["event"])
    webhook = Webhookfull(**body["item"])
    get_urls = body["get_urls"]
    retries = Retry(
        total=5,
        backoff_factor=0.1,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False,
        respect_retry_after_header=True,
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
    error = None
    try:
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
        if not response.ok:  # Status code >= 400
            error = Error(
                type="HTTPError",
                summary=f"HTTP {response.status_code}: {response.reason}",
                traceback=[],
                time=datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        with single_metric(
            name=f"StatusCode-{response.status_code}",
            unit=MetricUnit.Count,
            value=1,
        ) as metric:
            metric.add_dimension(name="webhook_id", value=webhook.id.root)
        logger.info(f"Status Code: {response.status_code}", response_text=response.text)
    # pylint: disable=broad-exception-caught
    except Exception as e:
        error = Error(
            type=type(e).__name__,
            summary=str(e),
            traceback=traceback.format_exc().splitlines(),
            time=datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
    if error:
        put_message(error_queue, {"id": webhook.id.root, "error": model_dump(error)})


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
# pylint: disable=unused-argument
def lambda_handler(event: SQSEvent, context: LambdaContext) -> dict:
    return process_partial_response(
        event=event,
        record_handler=record_handler,
        processor=batch_processor,
        context=context,
    )
