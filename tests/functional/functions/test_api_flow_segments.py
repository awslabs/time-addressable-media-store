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
