import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSEvent, SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.s3.transfer import TransferConfig
from dynamodb import (
    append_to_segment_list,
    get_storage_backend,
    query_segments_by_object_id,
)

tracer = Tracer()
logger = Logger()
metrics = Metrics()
batch_processor = BatchProcessor(event_type=EventType.SQS)

s3 = boto3.client("s3")
transfer_config = TransferConfig(
    multipart_threshold=50_000_000,
    multipart_chunksize=50_000_000,
)


@tracer.capture_method(capture_response=False)
def record_handler(record: SQSRecord) -> None:
    """Processes a single SQS record"""
    body = record.json_body
    object_id = body["object_id"]
    dst_storage_id = body["destination_storage_id"]
    dst_storage_backend = get_storage_backend(dst_storage_id)
    items, _, _ = query_segments_by_object_id(object_id, fetch_all=True)
    src_storage_ids = list(
        {storage_id for item in items for storage_id in item.get("storage_ids", [])}
    )
    src_storage_backend = get_storage_backend(src_storage_ids[0])
    src_metadata = s3.head_object(
        Bucket=src_storage_backend["bucket_name"], Key=object_id
    )
    s3.copy(
        CopySource={
            "Bucket": src_storage_backend["bucket_name"],
            "Key": object_id,
        },
        Bucket=dst_storage_backend["bucket_name"],
        Key=object_id,
        Config=transfer_config,
        ExtraArgs={
            "ContentType": src_metadata["ContentType"],
            "MetadataDirective": "COPY",
        },
    )
    for item in items:
        append_to_segment_list(item, "storage_ids", dst_storage_id)


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
