# pylint: disable=too-many-lines
import json
import os
import uuid

import boto3
import pytest

# pylint: disable=no-name-in-module
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
        module: The webhooks Lambda handler module
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
        r"MATCH (webhook: webhook)-[: has_tags]->(t: tags) WHERE "
        + rf'webhook.status IN ["created", "started"] AND webhook.SERIALISE_events CONTAINS "\"{event_type}\""'
        + where_expression
        + r" RETURN webhook {.*, tags: t {.*}}"
    )


def get_message_attributes_from_queue(queue_url):
    """Helper to receive message and extract SQS attributes."""
    client = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])
    response = client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=["All"],
        MessageAttributeNames=["All"],
    )

    if "Messages" not in response:
        return None

    message = response["Messages"][0]
    return {
        "body": json.loads(message["Body"]),
        "attributes": message.get("Attributes", {}),
        "message_group_id": message.get("Attributes", {}).get("MessageGroupId"),
        "message_deduplication_id": message.get("Attributes", {}).get(
            "MessageDeduplicationId"
        ),
    }


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


@pytest.mark.parametrize(
    "webhook_item,query_data,resources,expected_count",
    [
        pytest.param(
            {
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 0, "get_url": 0},
            id="flow_ids-no-match",
        ),
        pytest.param(
            {
                "status": "started",
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
    # Generate unique webhook ID to avoid deduplication in FIFO queue
    if webhook_item:
        webhook_item = {**webhook_item, "id": str(uuid.uuid4())}

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
            {
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
    # Generate unique webhook ID to avoid deduplication in FIFO queue
    if webhook_item:
        webhook_item = {**webhook_item, "id": str(uuid.uuid4())}

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
            {
                "status": "started",
                "events": ["flows/segments_added"],
                "url": "test-url",
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="basic",
        ),
        pytest.param(
            {
                "status": "started",
                "events": ["flows/segments_added"],
                "url": "test-url",
                "presigned": True,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="presigned-true",
        ),
        pytest.param(
            {
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
                "status": "started",
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
    # Generate unique webhook ID to avoid deduplication in FIFO queue
    if webhook_item:
        webhook_item = {**webhook_item, "id": str(uuid.uuid4())}

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


@pytest.mark.parametrize(
    "event_type,detail,resources,expected_resource_id",
    [
        pytest.param(
            "flows/created",
            {"flow": {"id": "flow-123"}},
            ["tams:flow:flow-123", "tams:source:source-456"],
            "flow-123",
            id="flows_created",
        ),
        pytest.param(
            "flows/updated",
            {"flow": {"id": "flow-456"}},
            ["tams:flow:flow-456", "tams:source:source-789"],
            "flow-456",
            id="flows_updated",
        ),
        pytest.param(
            "flows/deleted",
            {"flow_id": "flow-789"},
            ["tams:flow:flow-789"],
            "flow-789",
            id="flows_deleted",
        ),
        pytest.param(
            "flows/segments_added",
            {
                "flow_id": "flow-abc",
                "segments": [
                    {
                        "object_id": str(uuid.uuid4()),
                        "timerange": "[0:0_6:0)",
                        "timerange_start": 0,
                        "timerange_end": 5999999999,
                    }
                ],
            },
            ["tams:flow:flow-abc"],
            "flow-abc",
            id="flows_segments_added",
        ),
        pytest.param(
            "sources/created",
            {"source": {"id": "source-123"}},
            ["tams:source:source-123"],
            "source-123",
            id="sources_created",
        ),
        pytest.param(
            "sources/updated",
            {"source": {"id": "source-456"}},
            ["tams:source:source-456"],
            "source-456",
            id="sources_updated",
        ),
        pytest.param(
            "sources/deleted",
            {"source_id": "source-789"},
            ["tams:source:source-789"],
            "source-789",
            id="sources_deleted",
        ),
    ],
)
# pylint: disable=redefined-outer-name
def test_message_group_id_set_correctly(
    lambda_context,
    webhooks,
    mock_neptune_client,
    event_type,
    detail,
    resources,
    expected_resource_id,
):
    """Test that MessageGroupId is set correctly for each event type."""
    # Arrange
    webhook_id = str(uuid.uuid4())
    webhook_item = {
        "id": webhook_id,
        "status": "started",
        "events": [event_type],
        "url": "https://hook.example.com",
    }

    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [{"webhook": serialise_dict(webhook_item)}]
    }

    event = {
        "detail-type": event_type,
        "resources": resources,
        "detail": detail,
    }

    # Act
    webhooks.lambda_handler(event, lambda_context)

    # Assert
    message_data = get_message_attributes_from_queue(os.environ["WEBHOOKS_QUEUE_URL"])

    assert message_data is not None, "No message found in queue"
    assert (
        message_data["message_group_id"] is not None
    ), "MessageGroupId not set on SQS message"

    expected_group_id = f"{webhook_id}:{expected_resource_id}"
    assert (
        message_data["message_group_id"] == expected_group_id
    ), f"Expected MessageGroupId {expected_group_id}, got {message_data['message_group_id']}"


# pylint: disable=redefined-outer-name
def test_multiple_webhooks_get_different_group_ids(
    lambda_context, webhooks, mock_neptune_client
):
    """Test that different webhooks for the same resource get different MessageGroupIds."""
    # Arrange
    webhook_id_1 = str(uuid.uuid4())
    webhook_id_2 = str(uuid.uuid4())
    flow_id = str(uuid.uuid4())

    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "webhook": serialise_dict(
                    {
                        "id": webhook_id_1,
                        "status": "started",
                        "events": ["flows/created"],
                        "url": "https://hook1.example.com",
                    }
                )
            },
            {
                "webhook": serialise_dict(
                    {
                        "id": webhook_id_2,
                        "status": "started",
                        "events": ["flows/created"],
                        "url": "https://hook2.example.com",
                    }
                )
            },
        ]
    }

    event = {
        "detail-type": "flows/created",
        "resources": [f"tams:flow:{flow_id}"],
        "detail": {"flow": {"id": flow_id}},
    }

    # Act
    webhooks.lambda_handler(event, lambda_context)

    # Assert - should have 2 messages with different MessageGroupIds
    client = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])

    messages = []
    for _ in range(2):
        response = client.receive_message(
            QueueUrl=os.environ["WEBHOOKS_QUEUE_URL"],
            AttributeNames=["All"],
        )
        if "Messages" in response:
            messages.append(response["Messages"][0])

    assert len(messages) == 2, "Expected 2 messages in queue"

    group_ids = {msg["Attributes"]["MessageGroupId"] for msg in messages}
    assert len(group_ids) == 2, "Expected 2 different MessageGroupIds"
    assert f"{webhook_id_1}:{flow_id}" in group_ids
    assert f"{webhook_id_2}:{flow_id}" in group_ids


# pylint: disable=redefined-outer-name
def test_same_webhook_same_resource_gets_same_group_id(
    lambda_context, webhooks, mock_neptune_client
):
    """Test that events for same webhook+resource get the same MessageGroupId."""
    # Arrange
    webhook_id = str(uuid.uuid4())
    flow_id = str(uuid.uuid4())

    webhook_item = {
        "id": webhook_id,
        "status": "started",
        "events": ["flows/created", "flows/updated"],
        "url": "https://hook.example.com",
    }

    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [{"webhook": serialise_dict(webhook_item)}]
    }

    # Act - Send two different events for the same flow
    for event_type in ["flows/created", "flows/updated"]:
        event = {
            "detail-type": event_type,
            "resources": [f"tams:flow:{flow_id}"],
            "detail": {"flow": {"id": flow_id}},
        }
        webhooks.lambda_handler(event, lambda_context)

    # Assert - both messages should have the same MessageGroupId
    client = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])

    # For FIFO queues, receive multiple messages in one call
    # (messages in same MessageGroup are delivered sequentially)
    response = client.receive_message(
        QueueUrl=os.environ["WEBHOOKS_QUEUE_URL"],
        AttributeNames=["All"],
        MaxNumberOfMessages=10,
    )

    assert "Messages" in response, "No messages found in queue"
    messages = response["Messages"]
    assert len(messages) == 2, f"Expected 2 messages in queue, got {len(messages)}"

    group_ids = {msg["Attributes"]["MessageGroupId"] for msg in messages}
    assert len(group_ids) == 1, "Expected same MessageGroupId for both messages"

    expected_group_id = f"{webhook_id}:{flow_id}"
    assert list(group_ids)[0] == expected_group_id
