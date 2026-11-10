import json
import uuid
from http import HTTPStatus

import pytest
from schema import Flowsegmentpost

pytestmark = [
    pytest.mark.functional,
]


@pytest.fixture(scope="module")
def api_flow_segments():
    """Import the Flow Segments Lambda handler after moto is active."""
    # pylint: disable=import-outside-toplevel
    from api_flow_segments import app

    return app


@pytest.mark.parametrize(
    ("timerange", "expected_message"),
    [
        (
            "()",
            "Bad request. The timerange of the segment MUST NOT be empty.",
        ),
        (
            "[1:0_",
            "Bad request. The timerange of the segment MUST have a finite start and end.",
        ),
        (
            "_2:0)",
            "Bad request. The timerange of the segment MUST have a finite start and end.",
        ),
        (
            "_",
            "Bad request. The timerange of the segment MUST have a finite start and end.",
        ),
        (
            "(1:0_2:0)",
            "Bad request. The timerange start of the segment MUST be inclusive.",
        ),
    ],
)
def test_process_single_segment_rejects_invalid_timeranges(
    api_flow_segments, monkeypatch, timerange, expected_message
):
    """Segment ranges must be non-empty, bounded and start-inclusive."""
    monkeypatch.setattr(
        api_flow_segments,
        "validate_object_id",
        lambda *_: pytest.fail(
            "Object validation must not run for an invalid timerange"
        ),
    )
    segment = Flowsegmentpost(object_id="test-object", timerange=timerange)

    result = api_flow_segments.process_single_segment({"id": "test-flow"}, segment)

    assert result.error.summary == expected_message


def test_DELETE_flow_segments_persists_delete_request_before_enqueueing(
    lambda_context,
    api_event_factory,
    api_flow_segments,
    monkeypatch,
):
    """
    Verifies that DELETE /flows/{flowId}/segments with a timerange writes the
    delete request to Neptune before sending it to the delete queue, so the
    worker never receives a message for a node that does not yet exist.
    """
    # Arrange
    flow_id = str(uuid.uuid4())
    calls = []
    monkeypatch.setattr(api_flow_segments, "query_node", lambda *_args: {"id": flow_id})
    monkeypatch.setattr(
        api_flow_segments, "get_flow_timerange", lambda _flow_id: "[0:0_10:0)"
    )
    monkeypatch.setattr(
        api_flow_segments,
        "merge_delete_request",
        lambda item: calls.append(("merge_delete_request", item["id"])),
    )
    monkeypatch.setattr(
        api_flow_segments,
        "put_message",
        lambda _queue, item: calls.append(("put_message", item["id"])),
    )
    event = api_event_factory(
        "DELETE",
        f"/flows/{flow_id}/segments",
        query_params={"timerange": "[0:0_5:0)"},
    )
    event["requestContext"]["authorizer"] = {"username": "pytest"}

    # Act
    response = api_flow_segments.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.ACCEPTED.value
    request_id = json.loads(response["body"])["id"]
    assert calls == [
        ("merge_delete_request", request_id),
        ("put_message", request_id),
    ]
