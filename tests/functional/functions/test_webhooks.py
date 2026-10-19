# pylint: disable=too-many-lines
import json
import os
import uuid

import boto3
import pytest

# pylint: disable=no-name-in-module
from conftest import (
    ALTERNATIVE_STORAGE_ID,
    DEFAULT_STORAGE_ID,
    STORE_NAME,
    serialise_dict,
)

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
    # Add collected_by conditions at the end (to match actual query order). An
    # absent filter (NULL) and an empty list ("[]") both match an event carrying
    # no collected_by resources -- an empty list means "collected by nothing".
    for k in ["flow_collected_by_ids", "source_collected_by_ids"]:
        if k not in where_conditions:
            where_expression += (
                " AND "
                + rf'(webhook.SERIALISE_{k} IS NULL OR webhook.SERIALISE_{k} = "[]")'
            )
    return (
        r"MATCH (webhook: webhook)-[: has_tags]->(t: tags) WHERE "
        + rf'webhook.status IN ["created", "started"] AND webhook.SERIALISE_events CONTAINS "\"{event_type}\""'
        + where_expression
        + r" RETURN webhook {.*, tags: t {.*}}"
    )


#########
# TESTS #
#########


# Constants for test parameters
SAMPLE_FLOW_ID = str(uuid.uuid4())
# Both controlled get_urls (direct and pre-signed) carry the Storage Backend label
SAMPLE_LABEL = f"aws.eu-west-1:s3:{STORE_NAME}"
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
                "accept_storage_ids": [DEFAULT_STORAGE_ID],
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
                "accept_get_urls": [SAMPLE_LABEL],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_get_urls-single-matched",
        ),
        pytest.param(
            {
                "status": "started",
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": [SAMPLE_LABEL],
                "presigned": True,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-matched-presigned-only",
        ),
        pytest.param(
            {
                "status": "started",
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": [SAMPLE_LABEL, "dummy"],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
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
                "storage_ids": [DEFAULT_STORAGE_ID],
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
    if webhook_item.get("accept_storage_ids") and webhook_item.get("verbose_storage"):
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
            assert "provider" in get_url
            assert "region" in get_url
            assert "store_product" in get_url
            assert "store_type" in get_url
            assert "storage_id" in get_url
            assert "controlled" in get_url
            # Internal storage backend record fields must not leak into get_urls,
            # as they fail the get_url schema and break delivery
            for internal in ("record_type", "id", "default_storage", "bucket_name"):
                assert internal not in get_url

    assert message_body["event"] == event

    # Check item structure
    for key, value in message_body["item"].items():
        if value:
            assert value == webhook_item[key]


# pylint: disable=redefined-outer-name
def test_segments_added_init_urls_include_verbose_storage_metadata(
    lambda_context, webhooks, mock_neptune_client, monkeypatch
):
    """Verbose webhook URLs include metadata for an init Object's backend."""
    # Arrange
    webhook_item = {
        "id": str(uuid.uuid4()),
        "status": "started",
        "events": ["flows/segments_added"],
        "url": "test-url",
        "verbose_storage": True,
    }
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [{"webhook": serialise_dict(webhook_item)}]
    }
    captured_messages = []

    def capture_post_event(
        _event,
        _item,
        get_urls=None,
        init_get_urls=None,
        include_object_timerange=False,
    ):
        captured_messages.append(
            {
                "get_urls": get_urls,
                "init_get_urls": init_get_urls,
                "include_object_timerange": include_object_timerange,
            }
        )

    monkeypatch.setattr(webhooks, "post_event", capture_post_event)
    event = {
        "detail-type": "flows/segments_added",
        "resources": [f"tams:flow:{SAMPLE_FLOW_ID}"],
        "detail": {
            "flow_id": SAMPLE_FLOW_ID,
            "segments": [
                {
                    "object_id": str(uuid.uuid4()),
                    "init_object_id": str(uuid.uuid4()),
                    "timerange": "[0:0_6:0)",
                    "timerange_start": 0,
                    "timerange_end": 5999999999,
                    "storage_ids": [DEFAULT_STORAGE_ID],
                    "init_storage_ids": [ALTERNATIVE_STORAGE_ID],
                }
            ],
        },
    }

    # Act
    webhooks.lambda_handler(event, lambda_context)

    # Assert
    assert len(captured_messages) == 1
    init_get_urls = captured_messages[0]["init_get_urls"]
    assert len(init_get_urls) == 2
    for get_url in init_get_urls:
        assert get_url["storage_id"] == ALTERNATIVE_STORAGE_ID
        assert get_url["provider"] == "aws"
        assert get_url["region"] == "alternative-region"
        assert get_url["store_product"] == "s3"
        assert get_url["store_type"] == "http_object_store"
        assert get_url["controlled"] is True
        for internal in ("record_type", "id", "default_storage", "bucket_name"):
            assert internal not in get_url


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
                "accept_storage_ids": [DEFAULT_STORAGE_ID],
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
                "accept_get_urls": [SAMPLE_LABEL],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_get_urls-single-matched",
        ),
        pytest.param(
            {
                "status": "started",
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": [SAMPLE_LABEL],
                "presigned": True,
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-matched-presigned-only",
        ),
        pytest.param(
            {
                "status": "started",
                "events": ["flows/segments_added"],
                "url": "test-url",
                "accept_get_urls": [SAMPLE_LABEL, "dummy"],
            },
            {
                "event_type": "flows/segments_added",
                "conditions": {"flow_ids": SAMPLE_FLOW_ID},
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
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
    if webhook_item.get("accept_storage_ids") and webhook_item.get("verbose_storage"):
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
            assert "provider" in get_url
            assert "region" in get_url
            assert "store_product" in get_url
            assert "store_type" in get_url
            assert "storage_id" in get_url
            assert "controlled" in get_url
            # Internal storage backend record fields must not leak into get_urls,
            # as they fail the get_url schema and break delivery
            for internal in ("record_type", "id", "default_storage", "bucket_name"):
                assert internal not in get_url

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
    if webhook_item.get("accept_storage_ids") and webhook_item.get("verbose_storage"):
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


# These clauses are written out rather than built by generate_opencyher_query.
# That helper mirrors the production logic, so a change made identically in both
# would leave every test above green while changing which webhooks match.
FLOW_UNCOLLECTED_CLAUSE = (
    "(webhook.SERIALISE_flow_collected_by_ids IS NULL "
    'OR webhook.SERIALISE_flow_collected_by_ids = "[]")'
)
SOURCE_UNCOLLECTED_CLAUSE = (
    "(webhook.SERIALISE_source_collected_by_ids IS NULL "
    'OR webhook.SERIALISE_source_collected_by_ids = "[]")'
)


# pylint: disable=redefined-outer-name
def test_uncollected_event_matches_empty_collected_by_filter(
    lambda_context, webhooks, mock_neptune_client
):
    """An event carrying no collected_by resources must reach webhooks whose
    collected_by filter is an empty array as well as those with no filter.

    From TAMS API v8.2 an empty array is not "no filter": it limits events to
    resources collected by nothing, so both are valid matches here. The two stay
    distinguishable in the graph because serialise_neptune_obj stores an empty
    list as the text "[]" rather than collapsing it to NULL.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {"results": []}
    # flows/deleted needs no Segment or storage setup and no webhook is returned,
    # so only the query is exercised.
    event = {
        "detail-type": "flows/deleted",
        "resources": [f"tams:flow:{SAMPLE_FLOW_ID}"],
        "detail": {"flow_id": SAMPLE_FLOW_ID},
    }

    # Act
    webhooks.lambda_handler(event, lambda_context)
    query = mock_neptune_client.execute_open_cypher_query.call_args.kwargs[
        "openCypherQuery"
    ]

    # Assert
    assert FLOW_UNCOLLECTED_CLAUSE in query
    assert SOURCE_UNCOLLECTED_CLAUSE in query


def test_collected_event_excludes_empty_collected_by_filter(
    lambda_context, webhooks, mock_neptune_client
):
    """An event carrying collected_by resources must not reach webhooks whose
    collected_by filter is an empty array.

    The exclusion is implicit: the id group is satisfied by IS NULL or by
    CONTAINS the id, and "[]" is neither. What is asserted is that the group is
    emitted without an empty-array alternative, since adding one there would
    deliver every collected resource's events to an uncollected-only webhook.
    """
    # Arrange
    flow_collection_id = str(uuid.uuid4())
    source_collection_id = str(uuid.uuid4())
    mock_neptune_client.execute_open_cypher_query.return_value = {"results": []}
    event = {
        "detail-type": "flows/deleted",
        "resources": [
            f"tams:flow:{SAMPLE_FLOW_ID}",
            f"tams:flow-collected-by:{flow_collection_id}",
            f"tams:source-collected-by:{source_collection_id}",
        ],
        "detail": {"flow_id": SAMPLE_FLOW_ID},
    }

    # Act
    webhooks.lambda_handler(event, lambda_context)
    query = mock_neptune_client.execute_open_cypher_query.call_args.kwargs[
        "openCypherQuery"
    ]

    # Assert
    for attr, collection_id in [
        ("flow_collected_by_ids", flow_collection_id),
        ("source_collected_by_ids", source_collection_id),
    ]:
        assert (
            rf'(webhook.SERIALISE_{attr} IS NULL OR webhook.SERIALISE_{attr} CONTAINS "\"{collection_id}\"")'
            in query
        )
        assert f'webhook.SERIALISE_{attr} = "[]"' not in query
