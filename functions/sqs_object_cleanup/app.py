import json
from collections import defaultdict
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
from dynamodb import (
    delete_flow_storage_record,
    get_default_storage_backend,
    get_storage_backend,
    query_segments_by_object_id,
)

tracer = Tracer()
logger = Logger()
metrics = Metrics()
batch_processor = BatchProcessor(event_type=EventType.SQS)

s3 = boto3.client("s3")
default_storage_backend = get_default_storage_backend()


@tracer.capture_method(capture_response=False)
def delete_objects_batch(storage_backend, keys):
    # Delete orphan objects from S3
    for delete_batch in batched(keys, 1000):
        s3.delete_objects(
            Bucket=storage_backend["bucket_name"],
            Delete={"Objects": delete_batch},
        )
        for delete_item in delete_batch:
            delete_flow_storage_record(delete_item["Key"], storage_backend["id"])


@tracer.capture_method(capture_response=False)
def record_handler(record: SQSRecord) -> None:
    """Processes a single SQS record"""
    delete_objects = defaultdict(list)
    object_ids = json.loads(record.body)
    # Check if object_id of each deleted segment is still in use
    for object_id, storage_ids in object_ids:
        # Handle SQS message storage_ids: missing -> default, empty -> skip
        if storage_ids is None:
            storage_ids = [default_storage_backend["id"]]
        elif len(storage_ids) == 0:
            # Empty list means no S3 cleanup needed, just flow storage record
            delete_flow_storage_record(object_id)

        items, _, _ = query_segments_by_object_id(
            object_id, projection="storage_ids", fetch_all=True
        )

        # If no other records found with this object_id, delete for all storage_ids
        if len(items) == 0:
            for storage_id in storage_ids:
                delete_objects[storage_id].append({"Key": object_id})
        else:
            # Collect all unique storage_ids from DDB records
            ddb_storage_ids = set()
            # Handle DynamoDB storage_ids: missing -> default, empty -> empty
            for item in items:
                item_storage_ids = item.get("storage_ids")
                if item_storage_ids is None:
                    ddb_storage_ids.add(default_storage_backend["id"])
                else:
                    ddb_storage_ids.update(item_storage_ids)

            # Only delete storage_ids not found in DDB
            for storage_id in storage_ids:
                if storage_id not in ddb_storage_ids:
                    delete_objects[storage_id].append({"Key": object_id})

    for storage_id, keys in delete_objects.items():
        if storage_id == default_storage_backend["id"]:
            delete_objects_batch(default_storage_backend, keys)
        else:
            storage_backend = get_storage_backend(storage_id)
            delete_objects_batch(storage_backend, keys)


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
