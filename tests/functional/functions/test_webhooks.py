# pylint: disable=too-many-lines
import json
import os
import uuid

import boto3
import pytest
from conftest import serialise_dict
from pytest_lazy_fixtures import lf

pytestmark = [
    pytest.mark.functional,
]

############
# FIXTURES #
############


@pytest.fixture(scope="module")
def webhooks():
    """
    Import webhooks Lambda handler after moto is active.

    Returns:
        module: The api_objects Lambda handler module
    """
    # pylint: disable=import-outside-toplevel
    from webhooks import app

    return app


#############
# FUNCTIONS #
#############


def generate_opencyher_query(event_type, where_conditions):
    where_expression = ""
    for k, v in where_conditions.items():
        where_expression += (
            " AND "
            + rf'(webhook.SERIALISE_{k} IS NULL OR webhook.SERIALISE_{k} CONTAINS "\"{v}\"")'
        )
    return (
        r"MATCH (webhook: webhook) WHERE "
        + rf'webhook.SERIALISE_events CONTAINS "\"{event_type}\""'
        + where_expression
        + r" RETURN webhook {.*}"
    )


#########
# TESTS #
#########


# Constants for test parameters
SAMPLE_FLOW_ID = str(uuid.uuid4())
SAMPLE_LABELS = [
    "aws.eu-west-1:s3:Example TAMS",
    "aws.eu-west-1:s3.presigned:Example TAMS",
]
INVALID_STORAGE_ID = str(uuid.uuid4())
DUMMY_ID = str(uuid.uuid4())


@pytest.mark.parametrize(
    "webhook_item,query_data,resources,expected_count",
    [
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="basic",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "presigned": True,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="presigned-true",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "presigned": False,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="presigned-false",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "verbose_storage": True,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="verbose-true",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "verbose_storage": False,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="verbose-false",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_storage_ids": [],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_storage_ids-empty",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_storage_ids": [INVALID_STORAGE_ID],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_storage_ids-unmatched",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_storage_ids": [lf("default_storage_id")],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_storage_ids-matched",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": [],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-empty",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": ["dummy"],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-single-unmatched",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": SAMPLE_LABELS[:1],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-single-matched",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": SAMPLE_LABELS,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_get_urls-double-both",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": [SAMPLE_LABELS[0], "dummy"],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-double-one",
        ),
        pytest.param(
            None,
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 0, "get_url": 0},
            id="flow_ids-no-match",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "flow_ids": [SAMPLE_FLOW_ID],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="flow_ids-match",
        ),
    ],
)
# pylint: disable=redefined-outer-name
def test_segments_added_using_controlled_storage(
    lambda_context,
    webhooks,
    default_storage_id,
    mock_neptune_client,
    webhook_item,
    query_data,
    resources,
    expected_count,
):
    # Arrange
    detail = {
        "flow_id": SAMPLE_FLOW_ID,
        "segments": [
            {
                "object_id": str(uuid.uuid4()),
                "timerange": "[0:0_6:0)",
                "timerange_start": 0,
                "timerange_end": 5999999999,
                "storage_ids": [default_storage_id],
            }
        ],
    }
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [{"webhook": serialise_dict(webhook_item)}] if webhook_item else []
    }
    event = {
        "detail-type": "flows/segments_added",
        "resources": resources,
        "detail": detail,
    }

    # Act
    webhooks.lambda_handler(event, lambda_context)
    client = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])
    receive_message = client.receive_message(QueueUrl=os.environ["WEBHOOKS_QUEUE_URL"])
    opencypher_query = generate_opencyher_query(
        query_data["event_type"],
        query_data["conditions"],
    )

    # Assert
    mock_neptune_client.execute_open_cypher_query.assert_called_with(
        openCypherQuery=opencypher_query
    )

    if expected_count["message"] == 0:
        assert "Messages" not in receive_message
        return

    message_body = json.loads(receive_message["Messages"][0]["Body"])

    # Check expected number of messages
    assert len(receive_message["Messages"]) == expected_count["message"]

    # Check expected number of get_urls in message
    assert "get_urls" in message_body
    assert len(message_body["get_urls"]) == expected_count["get_url"]

    # Check label filter
    if webhook_item.get("accept_get_urls"):
        assert not any(
            True
            for get_url in message_body["get_urls"]
            if get_url["label"] not in webhook_item["accept_get_urls"]
        )

    # Check storage filter
    if webhook_item.get("accept_storage_ids"):
        assert not any(
            True
            for get_url in message_body["get_urls"]
            if get_url.get("storage_id", None) not in webhook_item["accept_storage_ids"]
        )

    # Check presigned option
    if webhook_item.get("presigned"):
        assert not any(
            True
            for get_url in message_body["get_urls"]
            if get_url["presigned"] != webhook_item["presigned"]
        )

    # Check get_url structure
    for get_url in message_body["get_urls"]:
        # Check mandatory fields
        assert "url" in get_url
        assert "label" in get_url
        # Check verbose option
        if webhook_item.get("verbose_storage", False):
            assert "record_type" in get_url
            assert "id" in get_url
            assert "provider" in get_url
            assert "region" in get_url
            assert "store_product" in get_url
            assert "store_type" in get_url
            assert "default_storage" in get_url
            assert "storage_id" in get_url
            assert "controlled" in get_url

    assert message_body["event"] == event

    # Check item structure
    for key, value in message_body["item"].items():
        if value:
            assert value == webhook_item[key]


@pytest.mark.parametrize(
    "webhook_item,query_data,resources,expected_count",
    [
        pytest.param(
            {"events": ["flows/segments_added"], "url": "test-url"},
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="basic",
        ),
        pytest.param(
            {"events": ["flows/segments_added"], "url": "test-url", "presigned": True},
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="presigned-true",
        ),
        pytest.param(
            {"events": ["flows/segments_added"], "url": "test-url", "presigned": False},
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="presigned-false",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "verbose_storage": True,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="verbose-true",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "verbose_storage": False,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="verbose-false",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_storage_ids": [],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_storage_ids-empty",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_storage_ids": [INVALID_STORAGE_ID],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_storage_ids-unmatched",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_storage_ids": [lf("default_storage_id")],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_storage_ids-matched",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": [],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-empty",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": ["dummy"],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-single-unmatched",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": SAMPLE_LABELS[:1],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-single-matched",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": SAMPLE_LABELS,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_get_urls-double-both",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": [SAMPLE_LABELS[0], "dummy"],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-double-one",
        ),
        pytest.param(
            None,
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 0, "get_url": 0},
            id="flow_ids-no-match",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "flow_ids": [SAMPLE_FLOW_ID],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="flow_ids-match",
        ),
    ],
)
# pylint: disable=redefined-outer-name
def test_segments_added_using_legacy_storage(
    lambda_context,
    webhooks,
    mock_neptune_client,
    webhook_item,
    query_data,
    resources,
    expected_count,
):
    # Arrange
    detail = {
        "flow_id": SAMPLE_FLOW_ID,
        "segments": [
            {
                "object_id": str(uuid.uuid4()),
                "timerange": "[0:0_6:0)",
                "timerange_start": 0,
                "timerange_end": 5999999999,
            }
        ],
    }
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [{"webhook": serialise_dict(webhook_item)}] if webhook_item else []
    }
    event = {
        "detail-type": "flows/segments_added",
        "resources": resources,
        "detail": detail,
    }

    # Act
    webhooks.lambda_handler(event, lambda_context)
    client = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])
    receive_message = client.receive_message(QueueUrl=os.environ["WEBHOOKS_QUEUE_URL"])
    opencypher_query = generate_opencyher_query(
        query_data["event_type"],
        query_data["conditions"],
    )

    # Assert
    mock_neptune_client.execute_open_cypher_query.assert_called_with(
        openCypherQuery=opencypher_query
    )

    if expected_count["message"] == 0:
        assert "Messages" not in receive_message
        return

    message_body = json.loads(receive_message["Messages"][0]["Body"])

    # Check expected number of messages
    assert len(receive_message["Messages"]) == expected_count["message"]

    # Check expected number of get_urls in message
    assert "get_urls" in message_body
    assert len(message_body["get_urls"]) == expected_count["get_url"]

    # Check label filter
    if webhook_item.get("accept_get_urls"):
        assert not any(
            True
            for get_url in message_body["get_urls"]
            if get_url["label"] not in webhook_item["accept_get_urls"]
        )

    # Check storage filter
    if webhook_item.get("accept_storage_ids"):
        assert not any(
            True
            for get_url in message_body["get_urls"]
            if get_url.get("storage_id", None) not in webhook_item["accept_storage_ids"]
        )

    # Check presigned option
    if webhook_item.get("presigned"):
        assert not any(
            True
            for get_url in message_body["get_urls"]
            if get_url["presigned"] != webhook_item["presigned"]
        )

    # Check get_url structure
    for get_url in message_body["get_urls"]:
        # Check mandatory fields
        assert "url" in get_url
        assert "label" in get_url
        # Check verbose option
        if webhook_item.get("verbose_storage", False):
            assert "record_type" in get_url
            assert "id" in get_url
            assert "provider" in get_url
            assert "region" in get_url
            assert "store_product" in get_url
            assert "store_type" in get_url
            assert "default_storage" in get_url
            assert "storage_id" in get_url
            assert "controlled" in get_url

    assert message_body["event"] == event

    # Check item structure
    for key, value in message_body["item"].items():
        if value:
            assert value == webhook_item[key]


@pytest.mark.parametrize(
    "webhook_item,query_data,resources,expected_count",
    [
        pytest.param(
            {"events": ["flows/segments_added"], "url": "test-url"},
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="basic",
        ),
        pytest.param(
            {"events": ["flows/segments_added"], "url": "test-url", "presigned": True},
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="presigned-true",
        ),
        pytest.param(
            {"events": ["flows/segments_added"], "url": "test-url", "presigned": False},
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="presigned-false",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "verbose_storage": True,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="verbose-true",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "verbose_storage": False,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="verbose-false",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_storage_ids": [],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_storage_ids-empty",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_storage_ids": [INVALID_STORAGE_ID],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_storage_ids-unmatched",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": [],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-empty",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": ["dummy"],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-single-unmatched",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": ["test"],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-single-matched",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": ["test", "dummy"],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-double-one",
        ),
        pytest.param(
            None,
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 0, "get_url": 0},
            id="flow_ids-no-match",
        ),
        pytest.param(
            {
                "events": ["flows/segments_added"],
                "url": "test-url",
                "flow_ids": [SAMPLE_FLOW_ID],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="flow_ids-match",
        ),
    ],
)
# pylint: disable=redefined-outer-name
def test_segments_added_using_external_storage(
    lambda_context,
    webhooks,
    mock_neptune_client,
    webhook_item,
    query_data,
    resources,
    expected_count,
):
    # Arrange
    detail = {
        "flow_id": SAMPLE_FLOW_ID,
        "segments": [
            {
                "object_id": str(uuid.uuid4()),
                "timerange": "[0:0_6:0)",
                "timerange_start": 0,
                "timerange_end": 5999999999,
                "get_urls": [{"label": "test", "url": "external-url"}],
            }
        ],
    }
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [{"webhook": serialise_dict(webhook_item)}] if webhook_item else []
    }
    event = {
        "detail-type": "flows/segments_added",
        "resources": resources,
        "detail": detail,
    }

    # Act
    webhooks.lambda_handler(event, lambda_context)
    client = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])
    receive_message = client.receive_message(QueueUrl=os.environ["WEBHOOKS_QUEUE_URL"])
    opencypher_query = generate_opencyher_query(
        query_data["event_type"],
        query_data["conditions"],
    )

    # Assert
    mock_neptune_client.execute_open_cypher_query.assert_called_with(
        openCypherQuery=opencypher_query
    )

    if expected_count["message"] == 0:
        assert "Messages" not in receive_message
        return

    message_body = json.loads(receive_message["Messages"][0]["Body"])

    # Check expected number of messages
    assert len(receive_message["Messages"]) == expected_count["message"]

    # Check expected number of get_urls in message
    assert "get_urls" in message_body
    assert len(message_body["get_urls"]) == expected_count["get_url"]

    # Check label filter
    if webhook_item.get("accept_get_urls"):
        assert not any(
            True
            for get_url in message_body["get_urls"]
            if get_url["label"] not in webhook_item["accept_get_urls"]
        )

    # Check storage filter
    if webhook_item.get("accept_storage_ids"):
        assert not any(
            True
            for get_url in message_body["get_urls"]
            if get_url.get("storage_id", None) not in webhook_item["accept_storage_ids"]
        )

    # Check presigned option
    if webhook_item.get("presigned"):
        assert not any(
            True
            for get_url in message_body["get_urls"]
            if get_url["presigned"] != webhook_item["presigned"]
        )

    # Check get_url structure
    for get_url in message_body["get_urls"]:
        # Check mandatory fields
        assert "url" in get_url
        assert "label" in get_url
        # Check verbose option
        if webhook_item.get("verbose_storage", False):
            # None of these should be present as non-managed storage is being used
            assert "record_type" not in get_url
            assert "id" not in get_url
            assert "provider" not in get_url
            assert "region" not in get_url
            assert "store_product" not in get_url
            assert "store_type" not in get_url
            assert "default_storage" not in get_url
            assert "storage_id" not in get_url
            assert "controlled" not in get_url

    assert message_body["event"] == event

    # Check item structure
    for key, value in message_body["item"].items():
        if value:
            assert value == webhook_item[key]
