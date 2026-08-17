import logging

import pytest
from conftest import WEBHOOK_VERIFICATION_ENABLED
from webhook_helpers import (
    get_event_key,
    get_resource_id,
    get_source_id_from_event,
    validate_webhook_body_matches,
)

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.usefixtures("webhook_events_collected"),
    pytest.mark.acceptance,
    pytest.mark.skipif(
        not WEBHOOK_VERIFICATION_ENABLED, reason="Webhook verification is disabled"
    ),
]


# pylint: disable=redefined-outer-name
def test_webhook_event_counts(webhook_test_data, webhook_expectations):
    """Validate that expected webhook event counts match actual deliveries."""
    # Arrange
    test_events_webhook = webhook_test_data["webhooks"].get("test-events")
    assert test_events_webhook is not None, "test-events webhook not found"
    actual_events = test_events_webhook.get("events", [])
    # Act
    expected_counts = {}
    for expectation in webhook_expectations:
        event_type = expectation["event_type"]
        expected_counts[event_type] = expected_counts.get(event_type, 0) + 1
    actual_counts = {}
    for event in actual_events:
        event_type = event["body"]["event_type"]
        actual_counts[event_type] = actual_counts.get(event_type, 0) + 1
    # Assert
    all_types = sorted(set(expected_counts.keys()) | set(actual_counts.keys()))
    for event_type in all_types:
        expected = expected_counts.get(event_type, 0)
        actual = actual_counts.get(event_type, 0)
        match = "✅" if expected == actual else "❌"
        logger.info(f"{match} {event_type}: expected={expected}, actual={actual}")
        assert expected == actual, f"{event_type}: expected {expected}, got {actual}"


def test_webhook_body_validation(webhook_test_data, webhook_expectations):
    """Validate webhook body content matches expectations."""
    # Arrange
    body_expectations = [e for e in webhook_expectations if "body" in e]
    test_events_webhook = webhook_test_data["webhooks"].get("test-events")
    assert test_events_webhook is not None, "test-events webhook not found"
    actual_events = test_events_webhook.get("events", [])
    # Act & Assert
    validate_webhook_body_matches(body_expectations, actual_events)
    logger.info(f"All {len(body_expectations)} webhook bodies matched!")


def test_flow_filter_webhook(webhook_test_data):
    """Validate flow_ids filter correctly limits events and filters get_urls."""
    # Arrange
    webhook_info = webhook_test_data["webhooks"].get("test-events-flow-filter")
    assert webhook_info is not None, "test-events-flow-filter webhook not found"
    events = webhook_info.get("events", [])
    assert len(events) > 0, "flow_filter webhook received no events"
    expected_flow_id = webhook_info["config"]["flow_ids"][0]
    # Act & Assert
    for event in events:
        body = event["body"]
        event_type = body["event_type"]
        # Check flow_id matches for Flow and Flow Segment events only
        if event_type.startswith("flows/"):
            actual_flow_id = get_resource_id(body, event_type)
            assert actual_flow_id == expected_flow_id, (
                f"{event_type}: Expected flow_id {expected_flow_id}, got {actual_flow_id}"
            )
        # Check accept_get_urls: [] worked (no get_urls in segments)
        if event_type == "flows/segments_added":
            for segment in body["event"]["segments"]:
                assert "get_urls" not in segment, (
                    "segments should not have get_urls with accept_get_urls: []"
                )
    logger.info(f"All {len(events)} events match flow_id filter and have no get_urls")


def test_source_filter_webhook(webhook_test_data):
    """Validate source_ids filter correctly limits Flow, Flow Segment and Source events."""
    # Arrange
    webhook_info = webhook_test_data["webhooks"].get("test-events-source-filter")
    assert webhook_info is not None, "test-events-source-filter webhook not found"
    events = webhook_info.get("events", [])
    assert len(events) > 0, "source_filter webhook received no events"
    expected_source_id = webhook_info["config"]["source_ids"][0]
    # Act & Assert
    for event in events:
        body = event["body"]
        event_type = body["event_type"]
        # Check source_id matches (source_ids filter applies to all event types)
        actual_source_id = get_source_id_from_event(body, event_type)
        assert actual_source_id == expected_source_id, (
            f"{event_type}: Expected source_id {expected_source_id}, got {actual_source_id}"
        )
        # Check accept_storage_ids filter with non-existent ID means no get_urls
        if event_type == "flows/segments_added":
            for segment in body["event"]["segments"]:
                assert "get_urls" not in segment, (
                    "Expected no get_urls with non-existent accept_storage_ids"
                )
    logger.info(f"All {len(events)} events match source_id filter")


def test_collected_flow_webhook(webhook_test_data, stub_multi_flow):
    """Validate flow_collected_by_ids filter limits Flow and Flow Segment events."""
    # Arrange
    webhook_info = webhook_test_data["webhooks"].get("test-events-collected-flow")
    assert webhook_info is not None, "test-events-collected-flow webhook not found"
    events = webhook_info.get("events", [])
    assert len(events) > 0, "collected_flow webhook received no events"
    # Act & Assert
    for event in events:
        body = event["body"]
        event_type = body["event_type"]
        if event_type.startswith("flows/"):
            actual_flow_id = get_resource_id(body, event_type)
            assert actual_flow_id != stub_multi_flow["id"], (
                f"{event_type}: Flow is not collected by flow_id {stub_multi_flow['id']}"
            )
        # Check presigned: true means only presigned URLs
        if event_type == "flows/segments_added":
            for segment in body["event"]["segments"]:
                if "get_urls" in segment:
                    for get_url in segment["get_urls"]:
                        assert get_url.get("presigned") is True, (
                            f"Expected presigned=true, got {get_url.get('presigned')}"
                        )
    logger.info(f"All {len(events)} events are collected flows with presigned URLs")


def test_collected_source_webhook(webhook_test_data, stub_multi_source):
    """Validate source_collected_by_ids filter limits Flow, Flow Segment and Source events."""
    # Arrange
    webhook_info = webhook_test_data["webhooks"].get("test-events-collected-source")
    assert webhook_info is not None, "test-events-collected-source webhook not found"
    events = webhook_info.get("events", [])
    assert len(events) > 0, "collected_source webhook received no events"
    # Act & Assert
    for event in events:
        body = event["body"]
        event_type = body["event_type"]
        # Check source_id for all event types (source_collected_by_ids applies to all)
        actual_source_id = get_source_id_from_event(body, event_type)
        assert actual_source_id != stub_multi_source["id"], (
            f"{event_type}: Source is not collected by source_id {stub_multi_source['id']}"
        )
        # Check verbose_storage: true means storage metadata included
        if event_type == "flows/segments_added":
            for segment in body["event"]["segments"]:
                if "get_urls" in segment:
                    for get_url in segment["get_urls"]:
                        # Verbose storage includes fields like store_type, provider, region, store_product, controlled
                        verbose_fields = [
                            "store_type",
                            "provider",
                            "region",
                            "store_product",
                            "controlled",
                        ]
                        has_verbose_field = any(
                            field in get_url for field in verbose_fields
                        )
                        assert has_verbose_field, (
                            f"Expected verbose storage fields (one of {verbose_fields}), got {get_url.keys()}"
                        )
    logger.info(f"All {len(events)} events are collected sources with verbose storage")


def test_uncollected_flow_webhook(webhook_test_data, stub_multi_flow):
    """Validate an empty flow_collected_by_ids limits events to Flows uncollected
    *at the time of the event*.

    An empty array is not "no filter": per 8.2 it means "only Flows not collected by
    any Flow Collection". Being collected is not a fixed property of a Flow, though --
    the four collected Flows are created before the collecting Flow exists, so their
    flows/created events are legitimately delivered here. So this does not assert a
    fixed set of Flow IDs. What must hold is that no single event reaches both this
    webhook and test-events-collected-flow: an event either carries flow-collected-by
    resources or it does not, and stub_multi_flow is the only Flow Collection in the
    fixture set. A regression treating [] as absent would deliver every Flow's events
    here and break that.

    Where the Flow object is in the body it carries the Flow's collected_by as it was
    at the time of the event, which is the property being filtered on, so that is
    asserted directly too.
    """
    # Arrange
    uncollected = webhook_test_data["webhooks"].get("test-events-uncollected-flow")
    collected = webhook_test_data["webhooks"].get("test-events-collected-flow")
    assert uncollected is not None, "test-events-uncollected-flow webhook not found"
    assert collected is not None, "test-events-collected-flow webhook not found"
    uncollected_events = uncollected.get("events", [])
    collected_events = collected.get("events", [])
    assert len(uncollected_events) > 0, "uncollected_flow webhook received no events"
    assert len(collected_events) > 0, "collected_flow webhook received no events"
    # Act
    collected_keys = {get_event_key(event["body"]) for event in collected_events}
    # Assert
    for event in uncollected_events:
        body = event["body"]
        event_type = body["event_type"]
        assert get_event_key(body) not in collected_keys, (
            f"{event_type}: event for Flow {get_resource_id(body, event_type)} was "
            f"delivered to both an empty and a populated flow_collected_by_ids "
            f"webhook, which are mutually exclusive"
        )
        flow = body.get("event", {}).get("flow", {})
        assert not flow.get("collected_by"), (
            f"{event_type}: Flow {flow.get('id')} reports collected_by "
            f"{flow.get('collected_by')}, so an empty flow_collected_by_ids must "
            f"exclude it"
        )
    # stub_multi_flow is never collected by anything, so its events arrive only here.
    assert any(
        get_resource_id(event["body"], event["body"]["event_type"])
        == stub_multi_flow["id"]
        for event in uncollected_events
        if event["body"]["event_type"].startswith("flows/")
    ), f"no events for the uncollected Flow {stub_multi_flow['id']}"
    logger.info(
        f"All {len(uncollected_events)} events were for Flows uncollected at the time"
    )


def test_uncollected_source_webhook(webhook_test_data, stub_multi_source):
    """Validate an empty source_collected_by_ids limits events to Sources uncollected
    *at the time of the event*.

    As for Flows, and for the same reason not an assertion over a fixed set of Source
    IDs. source_collected_by_ids applies to Flow and Flow Segment events too, so the
    mutual-exclusion check covers every event type, while the direct collected_by
    check applies only to sources/* events, whose body carries the Source object.
    """
    # Arrange
    uncollected = webhook_test_data["webhooks"].get("test-events-uncollected-source")
    collected = webhook_test_data["webhooks"].get("test-events-collected-source")
    assert uncollected is not None, "test-events-uncollected-source webhook not found"
    assert collected is not None, "test-events-collected-source webhook not found"
    uncollected_events = uncollected.get("events", [])
    collected_events = collected.get("events", [])
    assert len(uncollected_events) > 0, "uncollected_source webhook received no events"
    assert len(collected_events) > 0, "collected_source webhook received no events"
    # Act
    collected_keys = {get_event_key(event["body"]) for event in collected_events}
    # Assert
    for event in uncollected_events:
        body = event["body"]
        event_type = body["event_type"]
        assert get_event_key(body) not in collected_keys, (
            f"{event_type}: event for Source "
            f"{get_source_id_from_event(body, event_type)} was delivered to both an "
            f"empty and a populated source_collected_by_ids webhook, which are "
            f"mutually exclusive"
        )
        source = body.get("event", {}).get("source", {})
        assert not source.get("collected_by"), (
            f"{event_type}: Source {source.get('id')} reports collected_by "
            f"{source.get('collected_by')}, so an empty source_collected_by_ids must "
            f"exclude it"
        )
    # stub_multi_source is never collected, so its events arrive only here.
    assert any(
        get_source_id_from_event(event["body"], event["body"]["event_type"])
        == stub_multi_source["id"]
        for event in uncollected_events
    ), f"no events for the uncollected Source {stub_multi_source['id']}"
    logger.info(
        f"All {len(uncollected_events)} events were for Sources uncollected at the time"
    )
