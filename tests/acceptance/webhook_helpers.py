import copy
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone

from deepdiff import DeepDiff

logger = logging.getLogger(__name__)

# Standard exclude fields for each webhook event type
# event_timestamp is always excluded as it's dynamically generated
WEBHOOK_EXCLUDE_FIELDS = {
    "flows/created": [
        "event_timestamp",
        "event.flow.created_by",
        "event.flow.created",
    ],
    "flows/updated": [
        "event_timestamp",
        "event.flow.created_by",
        "event.flow.created",
        "event.flow.updated_by",
        "event.flow.metadata_updated",
    ],
    "flows/deleted": ["event_timestamp"],
    "flows/segments_added": ["event_timestamp"],
    "flows/segments_deleted": ["event_timestamp"],
    "sources/created": [
        "event_timestamp",
        "event.source.created_by",
        "event.source.created",
    ],
    "sources/updated": [
        "event_timestamp",
        "event.source.created_by",
        "event.source.created",
        "event.source.updated_by",
        "event.source.updated",
    ],
    "sources/deleted": ["event_timestamp"],
}

# Resource ID path mapping for each event type
# Maps event type to tuple of keys for extracting resource ID from webhook body
EVENT_TYPE_TO_ID_PATH = {
    "flows/created": ("event", "flow", "id"),
    "flows/updated": ("event", "flow", "id"),
    "flows/deleted": ("event", "flow_id"),
    "flows/segments_added": ("event", "flow_id"),
    "flows/segments_deleted": ("event", "flow_id"),
    "sources/created": ("event", "source", "id"),
    "sources/updated": ("event", "source", "id"),
    "sources/deleted": ("event", "source_id"),
}


def _remove_nested_field(obj, field_path: str) -> None:
    """Remove nested field using dot notation (e.g., 'event.flow.created_at').
    Supports arrays using [] notation (e.g., 'event.segments[].get_urls[].url').
    """
    # Normalize: remove [] and split on dots
    keys = field_path.replace("[]", "").split(".")

    def _remove_from_path(current, remaining_keys):
        if not remaining_keys:
            return
        key = remaining_keys[0]
        rest = remaining_keys[1:]
        if isinstance(current, list):
            # Remove from all items in the list
            for item in current:
                _remove_from_path(item, remaining_keys)
        elif isinstance(current, dict):
            if len(remaining_keys) == 1:
                # Last key - remove it
                if key in current:
                    del current[key]
            else:
                # Navigate deeper
                if key in current:
                    _remove_from_path(current[key], rest)

    _remove_from_path(obj, keys)


def _field_exists(obj, field_path: str) -> bool:
    """Check if a nested field exists using dot notation.
    Supports arrays using [] notation (e.g., 'event.segments[].get_urls[].url').
    """
    # Normalize: remove [] and split on dots
    keys = field_path.replace("[]", "").split(".")

    def _check_path(current, remaining_keys):
        if not remaining_keys:
            return True  # Reached the end of the path
        key = remaining_keys[0]
        rest = remaining_keys[1:]
        if isinstance(current, list):
            # Check all items in the list
            if not current:  # Empty list
                return False
            return all(_check_path(item, remaining_keys) for item in current)
        elif isinstance(current, dict):
            if key not in current:
                return False
            return _check_path(current[key], rest)
        else:
            return False

    return _check_path(obj, keys)


def parse_webhook_events(log_events):
    """Parse Lambda log events to extract webhook POST requests."""
    webhooks = defaultdict(list)
    for event in log_events:
        message = event.get("message", "")
        # Lambda function does: print(json.dumps(event))
        # So the message is the full event object, and body is nested inside
        try:
            api_event = json.loads(message)
            body = json.loads(api_event.get("body", "{}"))
        except json.JSONDecodeError:
            # Skip malformed JSON
            continue
        # Skip empty body objects
        if not body or body == {}:
            continue
        webhooks[api_event["pathParameters"]["identifier"]].append(
            {
                "timestamp": event.get("timestamp"),
                "body": body,
            }
        )
    return dict(webhooks)


def get_resource_id(body: dict, event_type: str) -> str | None:
    """Extract resource ID from webhook body based on event type."""
    path = EVENT_TYPE_TO_ID_PATH.get(event_type)
    if not path:
        return None
    current = body
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def validate_webhook_body_matches(body_expectations, actual_webhooks):
    """Validate webhook body content using matching algorithm."""
    # Track which webhooks have been matched (by index)
    matched_indices = set()
    validation_errors = []

    for expectation in body_expectations:
        # Extract expectation details
        expected_body = expectation["body"]
        extra_excludes = expectation.get("extra_excludes", [])
        test_name = expectation.get("test_name", "unknown")
        event_type = expectation["event_type"]

        # Get exclude fields from dictionary based on event type
        exclude_fields = WEBHOOK_EXCLUDE_FIELDS.get(event_type, [])

        # Merge with any extra excludes from the test
        exclude_fields = list(set(exclude_fields + extra_excludes))

        # Extract resource ID from expected body for filtering
        expected_resource_id = get_resource_id(expected_body, event_type)

        # Find all unmatched webhooks of this type with matching resource ID, sorted by timestamp
        candidates = sorted(
            [
                (idx, webhook)
                for idx, webhook in enumerate(actual_webhooks)
                if webhook["body"]["event_type"] == event_type
                and idx not in matched_indices
                and (
                    expected_resource_id is None
                    or get_resource_id(webhook["body"], event_type)
                    == expected_resource_id
                )
            ],
            key=lambda x: x[1]["timestamp"],
        )

        if not candidates:
            error = f"No unmatched webhook found for {event_type} [{test_name}]"
            logger.error(error)
            validation_errors.append(error)
            continue

        # Try to find a matching webhook
        match_found = False
        for idx, webhook in candidates:
            actual_body = webhook["body"]

            # Deep copy
            expected_copy = copy.deepcopy(expected_body)
            actual_copy = copy.deepcopy(actual_body)

            # Check excluded fields exist in actual body before removing
            missing_fields = []
            for field_path in exclude_fields:
                if not _field_exists(actual_copy, field_path):
                    missing_fields.append(field_path)

            if missing_fields:
                # Field is missing - this candidate doesn't match
                continue

            # All excluded fields present, remove them and compare
            for field_path in exclude_fields:
                _remove_nested_field(expected_copy, field_path)
                _remove_nested_field(actual_copy, field_path)

            # Check if this webhook matches
            diff = DeepDiff(expected_copy, actual_copy, ignore_order=True)

            if not diff:
                # Match found!
                matched_indices.add(idx)
                match_found = True
                logger.debug(f"✅ Matched {event_type} [{test_name}]")
                break

        if not match_found:
            # None of the candidates matched - log detailed error
            error = (
                f"{event_type} [{test_name}]: No webhook body matched expected content"
            )
            logger.error(error)
            logger.error(f"Expected body: {json.dumps(expected_body, indent=2)}")
            logger.error(f"Tried {len(candidates)} candidate(s):")

            # Show ALL candidates with their issues
            for idx, webhook in candidates:
                timestamp = (
                    datetime.fromtimestamp(webhook["timestamp"] / 1000, tz=timezone.utc)
                    .isoformat(timespec="milliseconds")
                    .replace("+00:00", "Z")
                )
                logger.error(f"\nWebhook at {timestamp}:")
                actual_body = webhook["body"]
                expected_copy = copy.deepcopy(expected_body)
                actual_copy = copy.deepcopy(actual_body)

                # Check if excluded fields exist
                missing_fields = []
                for field_path in exclude_fields:
                    if not _field_exists(actual_copy, field_path):
                        missing_fields.append(field_path)

                if missing_fields:
                    logger.error(f"  ❌ Missing excluded fields: {missing_fields}")
                else:
                    logger.error("  ✅ All excluded fields present")

                # Remove excluded fields and show diff
                for field_path in exclude_fields:
                    _remove_nested_field(expected_copy, field_path)
                    _remove_nested_field(actual_copy, field_path)

                diff = DeepDiff(expected_copy, actual_copy, ignore_order=True)
                if diff:
                    logger.error(f"  Diff: {diff}")
                else:
                    logger.error("  Diff: (empty - this should have matched!)")

            validation_errors.append(error)

    if validation_errors:
        raise AssertionError(
            f"{len(validation_errors)}/{len(body_expectations)} bodies failed validation"
        )


def get_source_id_from_event(body, event_type):
    """Determine source_id from webhook event body."""
    if event_type.startswith("sources/"):
        return get_resource_id(body, event_type)
    else:
        flow_id = get_resource_id(body, event_type)
        return "0" + flow_id[1:]
