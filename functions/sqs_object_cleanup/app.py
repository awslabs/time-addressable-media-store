import json
import os
from itertools import batched

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSEvent, SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from dynamodb import delete_flow_storage_record, segments_table

tracer = Tracer()
logger = Logger()
metrics = Metrics()
batch_processor = BatchProcessor(event_type=EventType.SQS)

s3 = boto3.client("s3")
bucket = os.environ["BUCKET"]


@tracer.capture_method(capture_response=False)
def record_handler(record: SQSRecord) -> None:
    """Processes a single SQS record"""
    delete_objects = []
    object_ids = json.loads(record.body)
    # Check if object_id of each deleted segment is still in use
    for object_id in object_ids:
        query = segments_table.query(
            IndexName="object-id-index",
            KeyConditionExpression=Key("object_id").eq(object_id),
            Select="COUNT",
        )
        if query["Count"] == 0:
            delete_objects.append({"Key": object_id})
    # Delete orphan objects from S3
    for delete_batch in batched(delete_objects, 1000):
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": delete_batch},
        )
        for delete_item in delete_batch:
            delete_flow_storage_record(delete_item["Key"])


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
