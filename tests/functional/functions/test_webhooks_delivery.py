import json
import os
import uuid

import boto3
import pytest
import responses

pytestmark = [
    pytest.mark.functional,
]

############
# FIXTURES #
############


@pytest.fixture(scope="module")
def webhooks_delivery():
    """
    Import webhooks_delivery Lambda handler after moto is active.

    Returns:
        module: The webhooks_delivery Lambda handler module
    """
    # pylint: disable=import-outside-toplevel
    from webhooks_delivery import app

    return app


@pytest.fixture
def sample_webhook():
    """Sample webhook configuration."""
    return {
        "id": str(uuid.uuid4()),
        "url": "https://webhook.example.com/events",
        "api_key_name": "Authorization",
        "api_key_value": "Bearer test-token",
        "status": "started",
        "events": [
            "flows/created",
            "flows/updated",
            "sources/created",
        ],  # Required field
    }


@pytest.fixture
def sample_sqs_event():
    """Factory for creating SQS event records."""

    def _create_event(event_type, detail, webhook, get_urls=None):
        event_bridge_event = {
            "version": "0",
            "id": str(uuid.uuid4()),
            "detail-type": event_type,
            "source": "tams.api",
            "account": "123456789012",
            "time": "2024-01-01T12:00:00Z",
            "region": "us-east-1",
            "resources": [],
            "detail": detail,
        }

        message_body = {
            "event": event_bridge_event,
            "item": webhook,
            "get_urls": get_urls,
        }

        return {
            "Records": [
                {
                    "messageId": str(uuid.uuid4()),
                    "receiptHandle": "test-receipt-handle",
                    "body": json.dumps(message_body),
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "SentTimestamp": "1640000000000",
                        "SenderId": "DUMMY-ID",
                        "ApproximateFirstReceiveTimestamp": "1640000000000",
                    },
                    "messageAttributes": {},
                    "md5OfBody": "test-md5",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:test-queue",
                    "awsRegion": "us-east-1",
                }
            ]
        }

    return _create_event


#########
# TESTS #
#########


@responses.activate
# pylint: disable=redefined-outer-name
def test_successful_webhook_delivery(
    lambda_context, webhooks_delivery, sample_webhook, sample_sqs_event
):
    """Test successful delivery of webhook to endpoint."""
    # Arrange
    flow_id = str(uuid.uuid4())
    responses.add(
        responses.POST,
        sample_webhook["url"],
        json={"status": "ok"},
        status=200,
    )

    # Use flows/deleted which only requires flow_id (no complex validation)
    event = sample_sqs_event(
        "flows/deleted",
        {"flow_id": flow_id},
        sample_webhook,
    )

    # Act
    result = webhooks_delivery.lambda_handler(event, lambda_context)

    # Assert
    assert result["batchItemFailures"] == []
    assert len(responses.calls) == 1

    # Verify request
    request = responses.calls[0].request
    assert request.headers["Authorization"] == "Bearer test-token"
    assert request.headers["Content-Type"] == "application/json"

    body = json.loads(request.body)
    assert body["event_type"] == "flows/deleted"
    assert body["event_timestamp"] == "2024-01-01T12:00:00Z"
    assert "event" in body
    assert body["event"]["flow_id"] == flow_id


@responses.activate
# pylint: disable=redefined-outer-name
def test_webhook_delivery_with_http_error(
    lambda_context, webhooks_delivery, sample_webhook, sample_sqs_event
):
    """Test webhook delivery when endpoint returns error status."""
    # Arrange
    responses.add(
        responses.POST,
        sample_webhook["url"],
        json={"error": "Internal Server Error"},
        status=500,
    )

    event = sample_sqs_event(
        "sources/deleted",
        {"source_id": str(uuid.uuid4())},
        sample_webhook,
    )

    # Act
    webhooks_delivery.lambda_handler(event, lambda_context)

    # Assert - error should be sent to error queue (using moto's actual queue)
    client = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])
    response = client.receive_message(QueueUrl=os.environ["ERROR_QUEUE_URL"])

    assert "Messages" in response, "Expected error message in error queue"
    error_message = json.loads(response["Messages"][0]["Body"])
    assert "error" in error_message
    assert error_message["error"]["type"] == "HTTPError"
    assert "HTTP 500" in error_message["error"]["summary"]


@responses.activate
# pylint: disable=redefined-outer-name
def test_webhook_delivery_segments_added(
    lambda_context, webhooks_delivery, sample_webhook, sample_sqs_event
):
    """Test delivery of segments_added event with get_urls."""
    # Arrange
    flow_id = str(uuid.uuid4())
    get_urls = [{"label": "test-label", "url": "s3://bucket/key", "presigned": False}]

    responses.add(
        responses.POST,
        sample_webhook["url"],
        json={"status": "ok"},
        status=200,
    )

    event = sample_sqs_event(
        "flows/segments_added",
        {
            "flow_id": flow_id,
            "segments": [
                {
                    "object_id": str(uuid.uuid4()),
                    "timerange": "[0:0_6:0)",
                    "timerange_start": 0,
                    "timerange_end": 5999999999,
                }
            ],
        },
        sample_webhook,
        get_urls=get_urls,
    )

    # Act
    result = webhooks_delivery.lambda_handler(event, lambda_context)

    # Assert
    assert result["batchItemFailures"] == []
    assert len(responses.calls) == 1

    body = json.loads(responses.calls[0].request.body)
    assert body["event_type"] == "flows/segments_added"
    assert body["event"]["segments"][0]["get_urls"] == get_urls


@responses.activate
# pylint: disable=redefined-outer-name
def test_webhook_delivery_batch_processing(
    lambda_context, webhooks_delivery, sample_webhook
):
    """Test batch processing of multiple webhook messages."""
    # Arrange
    responses.add(
        responses.POST,
        sample_webhook["url"],
        json={"status": "ok"},
        status=200,
    )

    # Create batch of 3 messages - use flows/deleted for simplicity
    event_bridge_event = {
        "version": "0",
        "id": str(uuid.uuid4()),
        "detail-type": "flows/deleted",
        "source": "tams.api",
        "account": "123456789012",
        "time": "2024-01-01T12:00:00Z",
        "region": "us-east-1",
        "resources": [],
        "detail": {"flow_id": str(uuid.uuid4())},
    }

    records = []
    for i in range(3):
        message_body = {
            "event": event_bridge_event,
            "item": sample_webhook,
            "get_urls": None,
        }
        records.append(
            {
                "messageId": str(uuid.uuid4()),
                "receiptHandle": f"test-receipt-handle-{i}",
                "body": json.dumps(message_body),
                "attributes": {},
                "messageAttributes": {},
                "md5OfBody": "test-md5",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:test-queue",
                "awsRegion": "us-east-1",
            }
        )

    event = {"Records": records}

    # Act
    result = webhooks_delivery.lambda_handler(event, lambda_context)

    # Assert
    assert result["batchItemFailures"] == []
    assert len(responses.calls) == 3


@responses.activate
# pylint: disable=redefined-outer-name
def test_webhook_delivery_partial_batch_failure(
    lambda_context, webhooks_delivery, sample_webhook
):
    """Test partial batch failure handling."""
    # Arrange - first call succeeds, second fails
    responses.add(
        responses.POST,
        sample_webhook["url"],
        json={"status": "ok"},
        status=200,
    )
    responses.add(
        responses.POST,
        sample_webhook["url"],
        json={"error": "error"},
        status=500,
    )

    # Create batch of 2 messages - use sources/deleted for simplicity
    records = []
    for i in range(2):
        event_bridge_event = {
            "version": "0",
            "id": str(uuid.uuid4()),
            "detail-type": "sources/deleted",
            "source": "tams.api",
            "account": "123456789012",
            "time": "2024-01-01T12:00:00Z",
            "region": "us-east-1",
            "resources": [],
            "detail": {"source_id": str(uuid.uuid4())},
        }
        message_body = {
            "event": event_bridge_event,
            "item": sample_webhook,
            "get_urls": None,
        }
        records.append(
            {
                "messageId": str(uuid.uuid4()),
                "receiptHandle": f"test-receipt-handle-{i}",
                "body": json.dumps(message_body),
                "attributes": {},
                "messageAttributes": {},
                "md5OfBody": "test-md5",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:test-queue",
                "awsRegion": "us-east-1",
            }
        )

    event = {"Records": records}

    # Act
    result = webhooks_delivery.lambda_handler(event, lambda_context)

    # Assert - no batch failures (errors sent to error queue instead)
    assert result["batchItemFailures"] == []

    # Verify error was sent to error queue for the failed message
    client = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])
    response = client.receive_message(QueueUrl=os.environ["ERROR_QUEUE_URL"])
    assert "Messages" in response, "Expected error message in error queue"


@responses.activate
# pylint: disable=redefined-outer-name
def test_webhook_delivery_without_api_key(
    lambda_context, webhooks_delivery, sample_webhook, sample_sqs_event
):
    """Test webhook delivery when no API key is configured."""
    # Arrange
    webhook_no_auth = {**sample_webhook}
    webhook_no_auth["api_key_name"] = None
    webhook_no_auth["api_key_value"] = None

    responses.add(
        responses.POST,
        webhook_no_auth["url"],
        json={"status": "ok"},
        status=200,
    )

    event = sample_sqs_event(
        "sources/deleted",
        {"source_id": str(uuid.uuid4())},
        webhook_no_auth,
    )

    # Act
    result = webhooks_delivery.lambda_handler(event, lambda_context)

    # Assert
    assert result["batchItemFailures"] == []
    request = responses.calls[0].request
    assert (
        "Authorization" not in request.headers
        or request.headers.get("Authorization") is None
    )
