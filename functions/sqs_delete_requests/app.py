import json
import os

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSEvent
from aws_lambda_powertools.utilities.parser import parse
from aws_lambda_powertools.utilities.typing import LambdaContext
from mediatimestamp.immutable import TimeRange
from schema import Flow
from utils import (
    check_delete_source,
    delete_flow_segments,
    get_model_by_id,
    publish_event,
    update_collected_by,
    update_flow_collection,
)

tracer = Tracer()
logger = Logger()
metrics = Metrics(namespace="Powertools")

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE"])
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
            delete_item = table.delete_item(
                Key={"record_type": "flow", "id": body["flow_id"]},
                ReturnValues="ALL_OLD",
            )
            if "Attributes" in delete_item:
                publish_event(
                    "flows/deleted", {"flow_id": body["flow_id"]}, [body["flow_id"]]
                )
                flow: Flow = parse(event=delete_item["Attributes"], model=Flow)
                # Delete source if no longer referenced by any other flows
                check_delete_source(table, delete_item["Attributes"]["source_id"])
                # Update collections that either referenced this flow or were referenced by it
                if flow.root.flow_collection:
                    for collection in flow.root.flow_collection:
                        collection_flow = get_model_by_id(table, "flow", collection.id)
                        update_collected_by(
                            table, body["flow_id"], collection_flow, False
                        )
                if flow.root.collected_by:
                    for collected_by_id in flow.root.collected_by:
                        update_flow_collection(table, body["flow_id"], collected_by_id)
        # Now proceed with deleting the flow segments
        body["status"] = "started"
        table.update_item(
            Key={"record_type": "delete-request", "id": body["id"]},
            UpdateExpression="SET #status = :new_status",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":new_status": body["status"]},
        )
        delete_flow_segments(
            table,
            segments_table,
            body["flow_id"],
            {"timerange": body["timerange_remaining"]},
            ["timerange"],
            TimeRange.from_str(body["timerange_remaining"]),
            context,
            s3_queue,
            del_queue,
            item_dict=body,
        )
