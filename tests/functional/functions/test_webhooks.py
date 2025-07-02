# pylint: disable=too-many-lines
import json
import os
import uuid

import boto3
import pytest
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


@pytest.fixture
def webhook_item_factory(webhooks_table):
    """
    Factory to create and cleanup webhook items for testing.
    """
    created_items = []

    def create_item(item_data):
        webhooks_table.put_item(Item=item_data)
        # Extract key fields for deletion
        key = {k: v for k, v in item_data.items() if k in ["event", "url"]}
        created_items.append(key)
        return key

    yield create_item

    # Cleanup all created items
    for key in created_items:
        webhooks_table.delete_item(Key=key)


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
    "webhook_item,resources,expected_count",
    [
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url"},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="basic",
        ),
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url", "presigned": True},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="presigned-true",
        ),
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url", "presigned": False},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="presigned-false",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "verbose_storage": True,
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="verbose-true",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "verbose_storage": False,
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="verbose-false",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_storage_ids": [],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_storage_ids-empty",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_storage_ids": [INVALID_STORAGE_ID],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_storage_ids-unmatched",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_storage_ids": [lf("default_storage_id")],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_storage_ids-matched",
        ),
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url", "accept_get_urls": []},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-empty",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_get_urls": ["dummy"],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-single-unmatched",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_get_urls": SAMPLE_LABELS[:1],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-single-matched",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_get_urls": SAMPLE_LABELS,
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_get_urls-double-both",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_get_urls": [SAMPLE_LABELS[0], "dummy"],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-double-one",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "flow_ids": ["dummy"],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 0, "get_url": 0},
            id="flow_ids-no-match",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "flow_ids": [SAMPLE_FLOW_ID],
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
    webhook_item_factory,
    webhook_item,
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
    webhook_item_factory(webhook_item)
    event = {
        "detail-type": "flows/segments_added",
        "resources": resources,
        "detail": detail,
    }

    # Act
    webhooks.lambda_handler(event, lambda_context)
    client = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])
    receive_message = client.receive_message(QueueUrl=os.environ["WEBHOOKS_QUEUE_URL"])

    # Assert
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
            if key == "events":
                assert value == [webhook_item["event"]]
            else:
                assert value == webhook_item[key]


@pytest.mark.parametrize(
    "webhook_item,resources,expected_count",
    [
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url"},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="basic",
        ),
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url", "presigned": True},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="presigned-true",
        ),
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url", "presigned": False},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="presigned-false",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "verbose_storage": True,
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="verbose-true",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "verbose_storage": False,
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="verbose-false",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_storage_ids": [],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_storage_ids-empty",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_storage_ids": [INVALID_STORAGE_ID],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_storage_ids-unmatched",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_storage_ids": [lf("default_storage_id")],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_storage_ids-matched",
        ),
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url", "accept_get_urls": []},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-empty",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_get_urls": ["dummy"],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-single-unmatched",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_get_urls": SAMPLE_LABELS[:1],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-single-matched",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_get_urls": SAMPLE_LABELS,
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 2},
            id="accept_get_urls-double-both",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_get_urls": [SAMPLE_LABELS[0], "dummy"],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-double-one",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "flow_ids": ["dummy"],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 0, "get_url": 0},
            id="flow_ids-no-match",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "flow_ids": [SAMPLE_FLOW_ID],
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
    webhook_item_factory,
    webhook_item,
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
    webhook_item_factory(webhook_item)
    event = {
        "detail-type": "flows/segments_added",
        "resources": resources,
        "detail": detail,
    }

    # Act
    webhooks.lambda_handler(event, lambda_context)
    client = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])
    receive_message = client.receive_message(QueueUrl=os.environ["WEBHOOKS_QUEUE_URL"])

    # Assert
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
            if key == "events":
                assert value == [webhook_item["event"]]
            else:
                assert value == webhook_item[key]


@pytest.mark.parametrize(
    "webhook_item,resources,expected_count",
    [
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url"},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="basic",
        ),
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url", "presigned": True},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="presigned-true",
        ),
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url", "presigned": False},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="presigned-false",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "verbose_storage": True,
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="verbose-true",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "verbose_storage": False,
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="verbose-false",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_storage_ids": [],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_storage_ids-empty",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_storage_ids": [INVALID_STORAGE_ID],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_storage_ids-unmatched",
        ),
        pytest.param(
            {"event": "flows/segments_added", "url": "test-url", "accept_get_urls": []},
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-empty",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_get_urls": ["dummy"],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 0},
            id="accept_get_urls-single-unmatched",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_get_urls": ["test"],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-single-matched",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "accept_get_urls": ["test", "dummy"],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 1, "get_url": 1},
            id="accept_get_urls-double-one",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "flow_ids": ["dummy"],
            },
            [f"tams:flow:{SAMPLE_FLOW_ID}"],
            {"message": 0, "get_url": 0},
            id="flow_ids-no-match",
        ),
        pytest.param(
            {
                "event": "flows/segments_added",
                "url": "test-url",
                "flow_ids": [SAMPLE_FLOW_ID],
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
    webhook_item_factory,
    webhook_item,
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
    webhook_item_factory(webhook_item)
    event = {
        "detail-type": "flows/segments_added",
        "resources": resources,
        "detail": detail,
    }

    # Act
    webhooks.lambda_handler(event, lambda_context)
    client = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])
    receive_message = client.receive_message(QueueUrl=os.environ["WEBHOOKS_QUEUE_URL"])

    # Assert
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
            if key == "events":
                assert value == [webhook_item["event"]]
            else:
                assert value == webhook_item[key]
