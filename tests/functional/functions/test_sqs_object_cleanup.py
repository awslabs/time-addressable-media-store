import json
import time
import uuid

import pytest
from conftest import DEFAULT_STORAGE_ID

pytestmark = [
    pytest.mark.functional,
]

############
# FIXTURES #
############


@pytest.fixture(scope="module")
def sqs_object_cleanup():
    """
    Import sqs_object_cleanup Lambda handler after moto is active.

    Returns:
        module: The sqs_object_cleanup Lambda handler module
    """
    # pylint: disable=import-outside-toplevel
    from sqs_object_cleanup import app

    return app


@pytest.fixture
def sqs_event_factory():
    """
    Factory fixture to create SQS events carrying [[object_id, storage_ids]] bodies,
    the format produced by the expired storage record pipe.

    Returns:
        function: A factory function that creates SQS event dictionaries
    """

    def _create_event(object_id, storage_ids):
        return {
            "Records": [
                {
                    "messageId": str(uuid.uuid4()),
                    "receiptHandle": "receipt-handle",
                    "body": json.dumps([[object_id, storage_ids]]),
                    "attributes": {},
                    "messageAttributes": {},
                    "md5OfBody": "",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:eu-west-1:123456789012:cleanup",
                    "awsRegion": "eu-west-1",
                }
            ]
        }

    return _create_event


def object_exists(s3_bucket, object_id):
    return any(obj.key == object_id for obj in s3_bucket.objects.all())


#########
# TESTS #
#########


# pylint: disable=redefined-outer-name
def test_expired_unregistered_object_is_deleted(
    sqs_object_cleanup, sqs_event_factory, lambda_context, s3_bucket, storage_table
):
    """An uploaded object whose storage record has TTL-expired and is not
    referenced by any segment is deleted from S3."""
    object_id = str(uuid.uuid4())
    s3_bucket.put_object(Key=object_id, Body=b"orphan")

    response = sqs_object_cleanup.lambda_handler(
        sqs_event_factory(object_id, [DEFAULT_STORAGE_ID]), lambda_context
    )

    assert response["batchItemFailures"] == []
    assert not object_exists(s3_bucket, object_id)
    assert "Item" not in storage_table.get_item(Key={"id": object_id})


def test_reallocated_object_is_not_deleted(
    sqs_object_cleanup, sqs_event_factory, lambda_context, s3_bucket, storage_table
):
    """An object_id re-requested via the storage endpoint after its previous
    record expired has a live allocation (expire_at set) and is left alone."""
    object_id = str(uuid.uuid4())
    s3_bucket.put_object(Key=object_id, Body=b"in-flight")
    storage_table.put_item(
        Item={
            "id": object_id,
            "flow_id": str(uuid.uuid4()),
            "expire_at": int(time.time()) + 3600,
            "storage_id": DEFAULT_STORAGE_ID,
        }
    )

    response = sqs_object_cleanup.lambda_handler(
        sqs_event_factory(object_id, [DEFAULT_STORAGE_ID]), lambda_context
    )

    assert response["batchItemFailures"] == []
    assert object_exists(s3_bucket, object_id)
    assert "Item" in storage_table.get_item(Key={"id": object_id})
    s3_bucket.delete_objects(Delete={"Objects": [{"Key": object_id}]})
    storage_table.delete_item(Key={"id": object_id})


def test_referenced_object_is_not_deleted(
    sqs_object_cleanup, sqs_event_factory, lambda_context, s3_bucket, segments_table
):
    """An object still referenced by a segment is not deleted from S3."""
    object_id = str(uuid.uuid4())
    flow_id = str(uuid.uuid4())
    s3_bucket.put_object(Key=object_id, Body=b"in-use")
    segments_table.put_item(
        Item={
            "flow_id": flow_id,
            "timerange_end": 999,
            "object_id": object_id,
            "storage_ids": [DEFAULT_STORAGE_ID],
        }
    )

    response = sqs_object_cleanup.lambda_handler(
        sqs_event_factory(object_id, [DEFAULT_STORAGE_ID]), lambda_context
    )

    assert response["batchItemFailures"] == []
    assert object_exists(s3_bucket, object_id)
    s3_bucket.delete_objects(Delete={"Objects": [{"Key": object_id}]})
    segments_table.delete_item(Key={"flow_id": flow_id, "timerange_end": 999})
