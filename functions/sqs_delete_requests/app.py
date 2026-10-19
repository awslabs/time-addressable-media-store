import json
import os
from typing import Optional

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSEvent, SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from dynamodb import delete_flow_segments
from mediatimestamp.immutable import TimeRange
from neptune import (
    check_delete_source,
    delete_flow,
    enhance_resources,
    set_node_property_base,
)
from utils import publish_event

tracer = Tracer()
logger = Logger()
metrics = Metrics()
batch_processor = BatchProcessor(event_type=EventType.SQS)

s3_queue = os.environ["S3_QUEUE_URL"]
del_queue = os.environ["DELETE_QUEUE_URL"]


@tracer.capture_method(capture_response=False)
def record_handler(
    record: SQSRecord, lambda_context: Optional[LambdaContext] = None
) -> None:
    """Processes a single SQS record"""
    body = json.loads(record.body)
    # If delete request has never been started and instructs flow deletion then delete the flow
    if body["delete_flow"] and body["status"] == "created":
        # Resolve event resources (source and collected-by) while the Flow still
        # exists and carry them on the SQS message across continuations, so
        # webhook source/collection filters still match after deletion. Stripped
        # in merge_delete_request so it is never persisted.
        body["segments_deleted_resources"] = enhance_resources(
            [f"tams:flow:{body['flow_id']}"]
        )
        source_id = delete_flow(body["flow_id"])
        # Reuse the pre-deletion resources; sources/deleted takes only the
        # source-typed entries so it doesn't route to flow-filtered webhooks.
        deleted_resources = body["segments_deleted_resources"]
        if source_id:
            publish_event(
                "flows/deleted",
                {"flow_id": body["flow_id"]},
                deleted_resources,
            )
        # Delete source if no longer referenced by any other flows
        if check_delete_source(source_id):
            publish_event(
                "sources/deleted",
                {"source_id": source_id},
                [r for r in deleted_resources if r.startswith("tams:source")],
            )
    # Proceed with deleting the flow segments
    body["status"] = "started"
    set_node_property_base(
        "delete_request", body["id"], {"delete_request.status": body["status"]}
    )
    delete_flow_segments(
        body["flow_id"],
        {"timerange": body["timerange_remaining"]},
        TimeRange.from_str(body["timerange_remaining"]),
        lambda_context,
        s3_queue,
        del_queue,
        item_dict=body,
        resources=body.get("segments_deleted_resources"),
    )


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: SQSEvent, context: LambdaContext) -> dict:
    return process_partial_response(
        event=event,
        record_handler=record_handler,
        processor=batch_processor,
        context=context,
    )
