"""
Helper functions for automated webhook delivery verification.

These functions handle:
- CloudFormation stack deployment and cleanup
- API Gateway API key retrieval
- CloudWatch Logs collection and parsing
- Webhook event validation
"""

import json


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

    # Print summary by event type
    event_types = {}
    for webhook in webhook_events:
        event_type = webhook["event_type"]
        event_types[event_type] = event_types.get(event_type, 0) + 1

    print("      Event breakdown:")
    for event_type, count in sorted(event_types.items()):
        print(f"        - {event_type}: {count}")


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


def compare_webhook_counts(
    expected_events: list[str], actual_webhooks: list[dict]
) -> None:
    """Compare expected webhook counts with actual received counts."""

    # Count expected events by type
    expected_counts = {}
    for event_type in expected_events:
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
