"""
Helper functions for automated webhook delivery verification.

These functions handle:
- CloudFormation stack deployment and cleanup
- API Gateway API key retrieval
- CloudWatch Logs collection and parsing
- Webhook event validation
"""

import copy
import json

from deepdiff import DeepDiff


def deploy_webhook_stack(
    session, region: str, stack_name: str, template_path: str
) -> None:
    """Deploy the webhook test CloudFormation stack."""
    cf = session.client("cloudformation", region_name=region)

    with open(template_path, "r", encoding="utf-8") as f:
        template_body = f.read()

    cf.create_stack(
        StackName=stack_name,
        TemplateBody=template_body,
        Capabilities=["CAPABILITY_IAM", "CAPABILITY_AUTO_EXPAND"],
        Tags=[
            {"Key": "Purpose", "Value": "WebhookAcceptanceTesting"},
            {"Key": "ManagedBy", "Value": "pytest"},
        ],
    )


def wait_for_stack_create(
    session, region: str, stack_name: str, timeout: int = 600
) -> None:
    """Wait for CloudFormation stack to complete creation."""
    cf = session.client("cloudformation", region_name=region)
    waiter = cf.get_waiter("stack_create_complete")

    try:
        waiter.wait(
            StackName=stack_name,
            WaiterConfig={"Delay": 10, "MaxAttempts": timeout // 10},
        )
    except Exception as e:
        # Get stack events to help debug
        events = cf.describe_stack_events(StackName=stack_name)
        failed_events = [
            e for e in events["StackEvents"] if "FAILED" in e.get("ResourceStatus", "")
        ]
        if failed_events:
            reason = failed_events[0].get("ResourceStatusReason", "Unknown")
            # pylint: disable=broad-exception-raised
            raise Exception(f"Stack creation failed: {reason}") from e
        raise


def get_stack_outputs(session, region: str, stack_name: str) -> dict[str, str]:
    """Get outputs from CloudFormation stack."""
    cf = session.client("cloudformation", region_name=region)

    response = cf.describe_stacks(StackName=stack_name)
    stack = response["Stacks"][0]

    outputs = {}
    for output in stack.get("Outputs", []):
        outputs[output["OutputKey"]] = output["OutputValue"]

    return outputs


def get_api_key_value(session, region: str, api_key_id: str) -> str:
    """Retrieve API key value from API Gateway."""
    apigw = session.client("apigateway", region_name=region)

    response = apigw.get_api_key(apiKey=api_key_id, includeValue=True)
    return response["value"]


def collect_cloudwatch_logs(
    session, region: str, log_group_name: str, start_time: int
) -> list[dict[str, any]]:
    """Collect log events from CloudWatch Logs."""
    logs = session.client("logs", region_name=region)

    events = []
    kwargs = {
        "logGroupName": log_group_name,
        "startTime": start_time,
        "filterPattern": "",  # Get all events
    }

    try:
        while True:
            response = logs.filter_log_events(**kwargs)
            events.extend(response.get("events", []))

            # Check for pagination
            next_token = response.get("nextToken")
            if not next_token:
                break

            kwargs["nextToken"] = next_token
    except logs.exceptions.ResourceNotFoundException:
        # Log group doesn't exist yet (no Lambda invocations)
        return []

    return events


def parse_webhook_events(log_events: list[dict[str, any]]) -> list[dict[str, any]]:
    """Parse Lambda log events to extract webhook POST requests."""
    webhooks = []

    for event in log_events:
        message = event.get("message", "")

        # Skip empty messages
        if not message.strip():
            continue

        # Skip Lambda runtime messages (START/END/REPORT lines)
        if message.startswith(("START", "END", "REPORT")):
            continue

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

        webhooks.append(
            {
                "timestamp": event.get("timestamp"),
                "identifier": api_event.get("pathParameters", {}).get(
                    "identifier", "not-found"
                ),
                "body": body,
                "event_type": body.get(
                    "event_type", "unknown"
                ),  # added at this level to aid in summarisation
            }
        )

    return webhooks


def validate_webhook_events(webhook_events: list[dict[str, any]]) -> None:
    """Validate webhook events for correctness."""
    if not webhook_events:
        return

    for i, webhook in enumerate(webhook_events):
        # Check structure
        assert "body" in webhook, f"Webhook {i}: missing 'body'"
        assert "identifier" in webhook, f"Webhook {i}: missing 'identifier'"
        assert "timestamp" in webhook, f"Webhook {i}: missing 'timestamp'"
        assert "event_type" in webhook, f"Webhook {i}: missing 'event_type'"

        # Check body has expected webhook structure
        body = webhook["body"]
        assert body, f"Webhook {i}: body is empty"
        assert isinstance(body, dict), f"Webhook {i}: body is not a dict"
        assert "event_timestamp" in body, f"Webhook {i}: missing 'event_timestamp'"
        assert "event_type" in body, f"Webhook {i}: missing 'event_type'"
        assert "event" in body, f"Webhook {i}: missing 'event' field"

    print(f"      Validated {len(webhook_events)} webhook events")


def delete_webhook_stack(session, region: str, stack_name: str) -> None:
    """Delete the webhook test CloudFormation stack."""
    cf = session.client("cloudformation", region_name=region)
    cf.delete_stack(StackName=stack_name)


def wait_for_stack_delete(
    session, region: str, stack_name: str, timeout: int = 600
) -> None:
    """Wait for CloudFormation stack deletion to complete."""
    cf = session.client("cloudformation", region_name=region)
    waiter = cf.get_waiter("stack_delete_complete")

    waiter.wait(
        StackName=stack_name, WaiterConfig={"Delay": 10, "MaxAttempts": timeout // 10}
    )


def compare_webhook_counts(expected_events: list, actual_webhooks: list[dict]) -> None:
    """Compare expected webhook counts with actual received counts."""

    # Count expected events by type (handle both strings and tuples)
    expected_counts = {}
    for item in expected_events:
        if isinstance(item, str):
            event_type = item
        elif isinstance(item, tuple) and len(item) >= 1:
            # Check if this is the new format: ((body_dict, exclude_list), test_name)
            if (
                isinstance(item[0], tuple)
                and len(item[0]) >= 1
                and isinstance(item[0][0], dict)
            ):
                # New format with test name
                event_type = item[0][0].get("event_type")
            elif isinstance(item[0], dict):
                # Old format: (body_dict, exclude_list)
                event_type = item[0].get("event_type")
            else:
                # Very old format: ("event_type", body, exclude)
                event_type = item[0]
        else:
            continue

        if event_type:
            expected_counts[event_type] = expected_counts.get(event_type, 0) + 1

    # Count actual events by type
    actual_counts = {}
    for webhook in actual_webhooks:
        event_type = webhook["event_type"]
        actual_counts[event_type] = actual_counts.get(event_type, 0) + 1

    # Get all event types
    all_types = set(expected_counts.keys()) | set(actual_counts.keys())

    # Compare and report
    mismatches = []
    for event_type in sorted(all_types):
        expected = expected_counts.get(event_type, 0)
        actual = actual_counts.get(event_type, 0)

        status = "✅" if expected == actual else "❌"
        print(f"      {status} {event_type}: expected {expected}, got {actual}")

        if expected != actual:
            mismatches.append(f"{event_type}: expected {expected}, got {actual}")

    if mismatches:
        print(f"\n      ⚠️  {len(mismatches)} mismatch(es) found")
    else:
        print("\n      ✅ All counts match!")


def validate_webhook_bodies(expected_events: list, actual_webhooks: list[dict]) -> None:
    """Validate webhook body content using matching algorithm."""
    # Extract expectations with body validation
    # Format: ((body_dict, exclude_list), test_name) or old format (body_dict, exclude_list)
    body_expectations = []
    for item in expected_events:
        if isinstance(item, tuple) and len(item) >= 1:
            # Check if this is body validation
            first_elem = item[0]
            if isinstance(first_elem, dict):
                # Old format: (body_dict, exclude_list)
                body_expectations.append((item, "unknown"))
            elif isinstance(first_elem, tuple) and isinstance(first_elem[0], dict):
                # New format: ((body_dict, exclude_list), test_name)
                body_expectations.append(item)

    if not body_expectations:
        print("\n      ℹ️  No body validations requested")
        return

    print(f"\n      🔍 Validating {len(body_expectations)} webhook bodies:")

    # Track which webhooks have been matched (by index)
    matched_indices = set()
    validation_errors = []

    for expectation in body_expectations:
        # Extract body tuple and test name
        if isinstance(expectation[0], tuple):
            body_tuple = expectation[0]
            test_name = expectation[1] if len(expectation) > 1 else "unknown"
        else:
            body_tuple = expectation
            test_name = "unknown"

        expected_body = body_tuple[0]
        exclude_fields = body_tuple[1] if len(body_tuple) > 1 else []

        # Extract event_type from the body
        event_type = expected_body.get("event_type")

        if not event_type:
            error = "Expected body missing 'event_type' field"
            print(f"      ❌ {error}")
            validation_errors.append(error)
            continue

        # Find all unmatched webhooks of this type
        candidates = [
            (idx, webhook)
            for idx, webhook in enumerate(actual_webhooks)
            if webhook["event_type"] == event_type and idx not in matched_indices
        ]

        if not candidates:
            error = f"No unmatched webhook found for {event_type} [{test_name}]"
            print(f"      ❌ {error}")
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
            for field_path in exclude_fields:
                if not _field_exists(actual_copy, field_path):
                    # Field is missing - this candidate doesn't match
                    diff = {"missing_excluded_field": field_path}
                    break
            else:
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
                break

        if not match_found:
            # None of the candidates matched
            error = (
                f"{event_type} [{test_name}]: No webhook body matched expected content"
            )
            print(f"      ❌ {error}")
            print("         Expected body:")
            print(f"         {json.dumps(expected_body, indent=10)}")
            print(f"         Tried {len(candidates)} candidate(s):")

            # Show ALL candidates with their issues
            for i, (idx, webhook) in enumerate(candidates):
                print(f"\n         Candidate #{i + 1} (webhook #{idx}):")
                actual_body = webhook["body"]
                expected_copy = copy.deepcopy(expected_body)
                actual_copy = copy.deepcopy(actual_body)

                # Check if excluded fields exist
                missing_fields = []
                for field_path in exclude_fields:
                    if not _field_exists(actual_copy, field_path):
                        missing_fields.append(field_path)

                if missing_fields:
                    print(f"           ❌ Missing excluded fields: {missing_fields}")
                else:
                    print("           ✅ All excluded fields present")

                # Remove excluded fields and show diff
                for field_path in exclude_fields:
                    _remove_nested_field(expected_copy, field_path)
                    _remove_nested_field(actual_copy, field_path)

                diff = DeepDiff(expected_copy, actual_copy, ignore_order=True)
                if diff:
                    print(f"           Diff: {diff}")
                else:
                    print("           Diff: (empty - this should have matched!)")

            validation_errors.append(error)

    if validation_errors:
        print(
            f"\n      ❌ {len(validation_errors)}/{len(body_expectations)} bodies failed validation"
        )
        raise AssertionError(
            "Webhook body validation failed:\n" + "\n".join(validation_errors)
        )
    else:
        print(f"      ✅ All {len(body_expectations)} bodies matched!")


def _remove_nested_field(obj: dict, field_path: str) -> None:
    """Remove nested field using dot notation (e.g., 'event.flow.created_at')."""
    keys = field_path.split(".")
    current = obj

    # Navigate to parent
    for key in keys[:-1]:
        if not isinstance(current, dict) or key not in current:
            return  # Path doesn't exist
        current = current[key]

    # Remove final key
    if isinstance(current, dict) and keys[-1] in current:
        del current[keys[-1]]


def _field_exists(obj: dict, field_path: str) -> bool:
    """Check if a nested field exists using dot notation."""
    keys = field_path.split(".")
    current = obj

    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return False
        current = current[key]

    return True
