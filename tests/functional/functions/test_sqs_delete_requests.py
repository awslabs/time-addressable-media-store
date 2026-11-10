import json
import uuid

import constants
import pytest
from aws_lambda_powertools.utilities.batch.exceptions import BatchProcessingError

pytestmark = [
    pytest.mark.functional,
]

############
# FIXTURES #
############


@pytest.fixture(scope="module")
def sqs_delete_requests():
    """
    Import sqs_delete_requests Lambda handler after moto is active.

    Returns:
        module: The sqs_delete_requests Lambda handler module
    """
    # pylint: disable=import-outside-toplevel
    from sqs_delete_requests import app

    return app


@pytest.fixture
def delete_request_body():
    """Provides a delete request as carried on the DeleteRequestQueue message."""
    return {
        "id": str(uuid.uuid4()),
        "flow_id": str(uuid.uuid4()),
        "created": "2026-01-01T00:00:00Z",
        "updated": "2026-01-01T00:00:00Z",
        "created_by": "pytest",
        "status": "created",
        "delete_flow": False,
        "timerange_to_delete": "[0:0_10:0)",
        "timerange_remaining": "[0:0_10:0)",
    }


@pytest.fixture
def sqs_event_factory():
    """
    Factory fixture to create DeleteRequestQueue SQS events.

    Returns:
        function: A factory function that creates SQS event dictionaries
    """

    def _create_event(body, receive_count):
        return {
            "Records": [
                {
                    "messageId": str(uuid.uuid4()),
                    "receiptHandle": "receipt-handle",
                    "body": json.dumps(body),
                    "attributes": {"ApproximateReceiveCount": str(receive_count)},
                    "messageAttributes": {},
                    "md5OfBody": "",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:eu-west-1:123456789012:delete",
                    "awsRegion": "eu-west-1",
                }
            ]
        }

    return _create_event


@pytest.fixture
# pylint: disable=redefined-outer-name
def merged(sqs_delete_requests, monkeypatch):
    """Records every delete request written by merge_delete_request."""
    calls = []
    monkeypatch.setattr(sqs_delete_requests, "merge_delete_request", calls.append)
    return calls


def _fail(*_args, **_kwargs):
    raise ValueError("No results returned from the database query.")


#########
# TESTS #
#########


@pytest.mark.parametrize(
    "receive_count", range(1, constants.DELETE_REQUEST_MAX_RECEIVE_COUNT)
)
# pylint: disable=redefined-outer-name
def test_failure_before_final_attempt_is_retried_without_error_status(
    sqs_delete_requests,
    sqs_event_factory,
    lambda_context,
    delete_request_body,
    merged,
    monkeypatch,
    receive_count,
):
    """Failures before the final attempt leave the delete request unchanged and
    fail the batch so SQS retries the message."""
    monkeypatch.setattr(sqs_delete_requests, "set_node_property_base", _fail)

    # All records failing raises, which makes Lambda report the batch as failed
    with pytest.raises(BatchProcessingError):
        sqs_delete_requests.lambda_handler(
            sqs_event_factory(delete_request_body, receive_count), lambda_context
        )
    assert merged == []


# pylint: disable=redefined-outer-name
def test_failure_on_final_attempt_sets_error_status(
    sqs_delete_requests,
    sqs_event_factory,
    lambda_context,
    delete_request_body,
    merged,
    monkeypatch,
):
    """On the final attempt the delete request is set to error with a
    spec-compliant error object, and the batch still fails so the message
    moves to the DLQ."""
    monkeypatch.setattr(sqs_delete_requests, "set_node_property_base", _fail)

    # All records failing raises, which makes Lambda report the batch as failed
    with pytest.raises(BatchProcessingError):
        sqs_delete_requests.lambda_handler(
            sqs_event_factory(
                delete_request_body, constants.DELETE_REQUEST_MAX_RECEIVE_COUNT
            ),
            lambda_context,
        )
    assert len(merged) == 1
    written = merged[0]
    assert written["id"] == delete_request_body["id"]
    assert written["status"] == "error"
    error = written["error"]
    # type, summary and time are required by the TAMS Error schema
    assert error["type"] == "ValueError"
    assert "No results returned from the database query." in error["summary"]
    assert error["time"]
    assert isinstance(error["traceback"], list)


# pylint: disable=redefined-outer-name
def test_failure_to_record_error_still_fails_record(
    sqs_delete_requests,
    sqs_event_factory,
    lambda_context,
    delete_request_body,
    monkeypatch,
):
    """If writing the error status itself fails, the original failure is still
    raised so the message moves to the DLQ."""
    monkeypatch.setattr(sqs_delete_requests, "set_node_property_base", _fail)
    monkeypatch.setattr(sqs_delete_requests, "merge_delete_request", _fail)

    # All records failing raises, which makes Lambda report the batch as failed
    with pytest.raises(BatchProcessingError):
        sqs_delete_requests.lambda_handler(
            sqs_event_factory(
                delete_request_body, constants.DELETE_REQUEST_MAX_RECEIVE_COUNT
            ),
            lambda_context,
        )


# pylint: disable=redefined-outer-name
def test_success_does_not_set_error_status(
    sqs_delete_requests,
    sqs_event_factory,
    lambda_context,
    delete_request_body,
    merged,
    monkeypatch,
):
    """A successful run on the final attempt is unaffected."""
    monkeypatch.setattr(
        sqs_delete_requests, "set_node_property_base", lambda *_args: {}
    )
    monkeypatch.setattr(
        sqs_delete_requests, "delete_flow_segments", lambda *_args, **_kwargs: None
    )

    response = sqs_delete_requests.lambda_handler(
        sqs_event_factory(
            delete_request_body, constants.DELETE_REQUEST_MAX_RECEIVE_COUNT
        ),
        lambda_context,
    )

    assert response["batchItemFailures"] == []
    assert merged == []
