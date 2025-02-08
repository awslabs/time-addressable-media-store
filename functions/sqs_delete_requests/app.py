import json
import os

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
from mediatimestamp.immutable import TimeRange
from utils import (
    check_delete_source,
    delete_flow,
    delete_flow_segments,
    publish_event,
    set_node_property_base,
)

tracer = Tracer()
logger = Logger()
metrics = Metrics(namespace="Powertools")

dynamodb = boto3.resource("dynamodb")
segments_table = dynamodb.Table(os.environ["SEGMENTS_TABLE"])
s3_queue = os.environ["S3_QUEUE_URL"]
del_queue = os.environ["DELETE_QUEUE_URL"]


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
# pylint: disable=unused-argument
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    event: SQSEvent = SQSEvent(event)
    for record in event.records:
        body = json.loads(record.body)
        # If delete request has never been started and instructs flow deletion then delete the flow
        if body["delete_flow"] and body["status"] == "created":
            # Delete the flow
            source_id = delete_flow(body["flow_id"])
            if source_id:
                publish_event(
                    "flows/deleted",
                    {"flow_id": body["flow_id"]},
                    [f'tams:flow:{body["flow_id"]}'],
                )
            # Delete source if no longer referenced by any other flows
            if check_delete_source(source_id):
                publish_event(
                    "sources/deleted",
                    {"source_id": source_id},
                    [f"tams:source:{source_id}"],
                )
        # Now proceed with deleting the flow segments
        body["status"] = "started"
        set_node_property_base(
            "delete_request", body["id"], {"delete_request.status": body["status"]}
        )
        delete_flow_segments(
            segments_table,
            body["flow_id"],
            {"timerange": body["timerange_remaining"]},
            TimeRange.from_str(body["timerange_remaining"]),
            context,
            s3_queue,
            del_queue,
            item_dict=body,
        )
