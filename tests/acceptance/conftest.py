import inspect
import logging
import os
import time
from copy import deepcopy

import boto3
import pytest
import requests
from deepdiff import DeepDiff
from webhook_helpers import EVENT_TYPE_TO_ID_PATH, parse_webhook_events

logger = logging.getLogger(__name__)

STACK_NAME = os.environ["TAMS_STACK_NAME"]
REGION = os.environ["TAMS_REGION"]
PROFILE = os.environ["AWS_PROFILE"]
STORE_NAME = "Example TAMS"
DYNAMIC_PROPS = [
    "created",
    "created_by",
    "updated",
    "updated_by",
    "metadata_updated",
    "segments_updated",
]
ID_404 = "00000000-0000-1000-8000-00000000000a"
WEBHOOK_VERIFICATION_ENABLED = (
    os.getenv("WEBHOOK_VERIFICATION_ENABLED", "true").lower() == "true"
)

############
# FIXTURES #
############


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def session():
    return boto3.Session(profile_name=PROFILE)


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def stack(session):
    cloudformation = session.resource("cloudformation", region_name=REGION)
    get_stack = cloudformation.Stack(STACK_NAME)
    return {
        "outputs": {o["OutputKey"]: o["OutputValue"] for o in get_stack.outputs},
        "parameters": {
            p["ParameterKey"]: p["ParameterValue"] for p in get_stack.parameters
        },
    }


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def api_endpoint(stack):
    return stack["outputs"]["ApiEndpoint"]


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def token_factory(stack, session):
    """Create tokens with specific scopes (cached per scope combination)."""
    cache = {}

    def _get_token(scopes=None):
        cache_key = tuple(sorted(scopes)) if scopes else None
        if cache_key in cache:
            return cache[cache_key]

        user_pool_id = stack["outputs"]["UserPoolId"]
        client_id = stack["outputs"]["UserPoolClientId"]
        client_secret = get_client_secret(session, user_pool_id, client_id, REGION)
        form_data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        }
        if scopes:
            form_data["scope"] = " ".join(scopes)
        # If no scopes supplied Cognito will automatically add all allowed scopes
        resp = requests.post(stack["outputs"]["TokenUrl"], data=form_data, timeout=30)
        resp.raise_for_status()
        token = resp.json()["access_token"]
        cache[cache_key] = token
        return token

    return _get_token


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def api_client_factory(api_endpoint, token_factory):
    """Factory to create API clients with specific scopes (defaults to all scopes)."""

    def _get_client(scopes=None):
        token = token_factory(scopes)

        class ApiGwSession(requests.Session):
            def __init__(self, base_url=None, default_headers=None):
                if default_headers is None:
                    default_headers = {}
                self.base_url = base_url
                self.default_headers = default_headers
                super(ApiGwSession, self).__init__()

            def request(
                self, method, url, *args, params=None, data=None, headers=None, **kwargs
            ):
                url = f"{self.base_url}{url}"
                merged_headers = deepcopy(self.default_headers)
                if isinstance(headers, dict):
                    merged_headers.update(headers)
                return super(ApiGwSession, self).request(
                    method,
                    url,
                    params,
                    data,
                    headers=merged_headers,
                    timeout=30,
                    *args,
                    **kwargs,
                )

        return ApiGwSession(api_endpoint, {"Authorization": f"Bearer {token}"})

    return _get_client


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def api_client_cognito(api_client_factory):
    """API client with all scopes for backward compatibility."""
    return api_client_factory()


@pytest.fixture(scope="session")
def webhook_ids():
    return []


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def default_storage_id(stack):
    return stack["outputs"]["DefaultStorageId"]


@pytest.fixture(scope="session")
def media_objects():
    return []


@pytest.fixture(scope="session")
def delete_requests():
    return []


@pytest.fixture(scope="session")
def stub_video_flow():
    return {
        "id": "10000000-0000-1000-8000-000000000000",
        "source_id": "00000000-0000-1000-8000-000000000000",
        "format": "urn:x-nmos:format:video",
        "generation": 0,
        "label": "pytest - video",
        "description": "pytest - video",
        "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
        "codec": "video/h264",
        "container": "video/mp2t",
        "avg_bit_rate": 5000000,
        "max_bit_rate": 5000000,
        "essence_parameters": {
            "frame_rate": {"numerator": 50, "denominator": 1},
            "frame_width": 1920,
            "frame_height": 1080,
            "bit_depth": 8,
            "interlace_mode": "progressive",
            "component_type": "YCbCr",
            "horiz_chroma_subs": 2,
            "vert_chroma_subs": 1,
            "avc_parameters": {"profile": 122, "level": 42, "flags": 0},
        },
        "collected_by": ["10000000-0000-1000-8000-000000000003"],
    }


@pytest.fixture(scope="session")
def stub_audio_flow():
    return {
        "id": "10000000-0000-1000-8000-000000000001",
        "source_id": "00000000-0000-1000-8000-000000000001",
        "format": "urn:x-nmos:format:audio",
        "generation": 0,
        "label": "pytest - audio",
        "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
        "codec": "audio/aac",
        "container": "video/mp2t",
        "essence_parameters": {
            "sample_rate": 48000,
            "channels": 2,
            "bit_depth": 32,
            "codec_parameters": {"coded_frame_size": 1024, "mp4_oti": 2},
        },
        "collected_by": ["10000000-0000-1000-8000-000000000003"],
    }


@pytest.fixture(scope="session")
def stub_data_flow():
    return {
        "id": "10000000-0000-1000-8000-000000000002",
        "source_id": "00000000-0000-1000-8000-000000000002",
        "format": "urn:x-nmos:format:data",
        "generation": 0,
        "label": "pytest - data",
        "description": "pytest - data",
        "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
        "codec": "text/plain",
        "essence_parameters": {
            "data_type": "text",
        },
        "read_only": True,
        "collected_by": ["10000000-0000-1000-8000-000000000003"],
    }


@pytest.fixture(scope="session")
def stub_image_flow():
    return {
        "id": "10000000-0000-1000-8000-000000000004",
        "source_id": "00000000-0000-1000-8000-000000000004",
        "format": "urn:x-tam:format:image",
        "generation": 0,
        "label": "pytest - image",
        "description": "pytest - image",
        "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
        "codec": "video/jpeg",
        "container": "video/jpeg",
        "essence_parameters": {
            "frame_width": 320,
            "frame_height": 180,
        },
        "collected_by": ["10000000-0000-1000-8000-000000000003"],
    }


@pytest.fixture(scope="session")
def stub_multi_flow():
    return {
        "id": "10000000-0000-1000-8000-000000000003",
        "source_id": "00000000-0000-1000-8000-000000000003",
        "format": "urn:x-nmos:format:multi",
        "generation": 0,
        "label": "pytest",
        "description": "pytest",
        "tags": {
            "input_quality": "contribution",
            "flow_status": "ingesting",
            "test": "this",
        },
        "container": "video/mp2t",
        "flow_collection": [
            {"id": "10000000-0000-1000-8000-000000000000", "role": "video"},
            {"id": "10000000-0000-1000-8000-000000000001", "role": "audio"},
            {"id": "10000000-0000-1000-8000-000000000002", "role": "data"},
            {"id": "10000000-0000-1000-8000-000000000004", "role": "image"},
        ],
    }


@pytest.fixture(scope="session")
def stub_video_source():
    return {
        "id": "00000000-0000-1000-8000-000000000000",
        "format": "urn:x-nmos:format:video",
        "label": "pytest - video",
        "description": "pytest - video",
        "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
        "collected_by": ["00000000-0000-1000-8000-000000000003"],
    }


@pytest.fixture(scope="session")
def stub_audio_source():
    return {
        "id": "00000000-0000-1000-8000-000000000001",
        "format": "urn:x-nmos:format:audio",
        "label": "pytest - audio",
        "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
        "collected_by": ["00000000-0000-1000-8000-000000000003"],
    }


@pytest.fixture(scope="session")
def stub_data_source():
    return {
        "id": "00000000-0000-1000-8000-000000000002",
        "format": "urn:x-nmos:format:data",
        "label": "pytest - data",
        "description": "pytest - data",
        "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
        "collected_by": ["00000000-0000-1000-8000-000000000003"],
    }


@pytest.fixture(scope="session")
def stub_image_source():
    return {
        "id": "00000000-0000-1000-8000-000000000004",
        "format": "urn:x-tam:format:image",
        "label": "pytest - image",
        "description": "pytest - image",
        "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
        "collected_by": ["00000000-0000-1000-8000-000000000003"],
    }


@pytest.fixture(scope="session")
def stub_multi_source():
    return {
        "id": "00000000-0000-1000-8000-000000000003",
        "format": "urn:x-nmos:format:multi",
        "label": "pytest",
        "description": "pytest",
        "tags": {
            "input_quality": "contribution",
            "flow_status": "ingesting",
            "test": "this",
        },
        "source_collection": [
            {"id": "00000000-0000-1000-8000-000000000000", "role": "video"},
            {"id": "00000000-0000-1000-8000-000000000001", "role": "audio"},
            {"id": "00000000-0000-1000-8000-000000000002", "role": "data"},
            {"id": "00000000-0000-1000-8000-000000000004", "role": "image"},
        ],
    }


@pytest.fixture(scope="session")
def stub_webhook_basic():
    return {
        "url": "https://hook.example.com",
        "api_key_name": "Authorization",
        "events": ["flows/created", "flows/updated", "flows/deleted"],
    }


@pytest.fixture(scope="session")
def stub_webhook_tags():
    return {
        "url": "https://hook.example.com",
        "api_key_name": "Authorization",
        "events": ["sources/created", "sources/updated", "sources/deleted"],
        "tags": {"auth_classes": ["news", "sports"]},
    }


@pytest.fixture(scope="session")
def webhook_test_data():
    """Storage for webhook verification data across session."""
    return {
        "metadata": {},
        "webhooks": {},
    }


# pylint: disable=redefined-outer-name
@pytest.fixture(scope="session", autouse=True)
def webhook_verification_lifecycle(
    session,
    api_client_cognito,
    webhook_test_data,
    stub_video_flow,
    stub_video_source,
    stub_multi_flow,
    stub_multi_source,
):
    """
    Automated webhook delivery verification lifecycle.

    Enabled by default. Disable with: WEBHOOK_VERIFICATION_ENABLED=false

    Flow:
    1. Deploy webhook-api-gateway stack
    2. Get stack outputs (API URL, API Key, Log Group)
    3. Get API key value from API Gateway
    4. Register webhooks with TAMS API (5 webhooks with different filters)
    5. [Tests run]
    6. Cleanup webhook registrations
    7. Delete CloudFormation stack

    Note: Event collection happens in webhook_events_collected fixture
    """

    if not WEBHOOK_VERIFICATION_ENABLED:
        yield
        return

    logger.info("🧪 Setting up webhook verification...")
    stack_name = f"tams-webhook-test-{int(time.time())}"

    try:
        # 1. Deploy CloudFormation stack
        logger.info(f"Deploying stack: {stack_name}...")
        deploy_cloudformation_stack(session, stack_name, "webhook-api-gateway.yaml")
        wait_for_stack_status(session, stack_name, "stack_create_complete")
        logger.info("✅ Stack deployed")

        # 2. Get stack outputs
        outputs = get_stack_outputs(session, stack_name)
        base_webhook_url = outputs["ApiUrl"]
        api_key_id = outputs["ApiKeyId"]
        # Store for later collection by webhook_events_collected fixture
        webhook_test_data["metadata"] = {
            "log_group_name": outputs["LogGroupName"],
            "start_time": int(time.time() * 1000),
        }

        # 3. Get API key value
        api_key_value = get_api_key_value(session, api_key_id)

        # 4. Register webhooks with TAMS API
        webhooks_config = [
            {
                "identifier": "test-events",
                "config": {},
            },
            {
                "identifier": "test-events-flow-filter",
                "config": {
                    "flow_ids": [stub_video_flow["id"]],
                    "accept_get_urls": [],
                },
            },
            {
                "identifier": "test-events-source-filter",
                "config": {
                    "source_ids": [stub_video_source["id"]],
                    "accept_storage_ids": [ID_404],
                },
            },
            {
                "identifier": "test-events-collected-flow",
                "config": {
                    "flow_collected_by_ids": [stub_multi_flow["id"]],
                    "presigned": True,
                },
            },
            {
                "identifier": "test-events-collected-source",
                "config": {
                    "source_collected_by_ids": [stub_multi_source["id"]],
                    "verbose_storage": True,
                },
            },
        ]

        # Register all webhooks
        for webhook_def in webhooks_config:
            url = f'{base_webhook_url}/{webhook_def["identifier"]}'
            logger.info(f"Registering webhook: {url}...")
            response = api_client_cognito.request(
                "POST",
                "/service/webhooks",
                json={
                    "url": url,
                    "api_key_name": "x-api-key",
                    "api_key_value": api_key_value,
                    "events": list(EVENT_TYPE_TO_ID_PATH.keys()),
                    "tags": {"_test_infrastructure": ["webhook_verification"]},
                    **webhook_def["config"],
                },
            )
            response.raise_for_status()
            webhook_id = response.json()["id"]
            webhook_test_data["webhooks"][webhook_def["identifier"]] = {
                "id": webhook_id,
                "config": webhook_def["config"],
            }
            logger.info(
                f'✅ Webhook {webhook_def["identifier"]} registered: {webhook_id}'
            )

        logger.info("🧪 Running tests with webhook verification enabled...")

    # pylint: disable=broad-exception-caught
    except Exception as e:
        logger.error(f"❌ Webhook verification setup failed: {e}")
        # Cleanup on setup failure
        try:
            delete_cloudformation_stack(session, stack_name)
        except Exception:
            pass
        pytest.exit(f"Webhook verification setup failed: {e}", returncode=1)

    # ========== 5. [Tests run] ==========
    yield
    # =====================================

    logger.info("🧹 Cleaning up webhook infrastructure...")

    # 6. Cleanup webhook registrations
    if webhook_test_data["webhooks"]:
        for identifier, webhook_info in webhook_test_data["webhooks"].items():
            try:
                webhook_id = webhook_info["id"]
                logger.info(f"Deleting webhook {identifier}: {webhook_id}...")
                api_client_cognito.request("DELETE", f"/service/webhooks/{webhook_id}")
                logger.info(f"✅ Webhook {identifier} deleted")
            # pylint: disable=broad-exception-caught
            except Exception as e:
                logger.warning(f"⚠️  Failed to delete webhook {identifier}: {e}")

    # 7. Delete CloudFormation stack
    try:
        logger.info(f"Deleting stack: {stack_name}...")
        delete_cloudformation_stack(session, stack_name)
        logger.info("✅ Stack deletion initiated (async)")
    # pylint: disable=broad-exception-caught
    except Exception as e:
        logger.warning(f"⚠️  Failed to delete stack: {e}")


@pytest.fixture(scope="session")
def webhook_expectations():
    """Track expected webhook events from tests."""
    return []


@pytest.fixture(scope="session")
def webhook_events_collected(session, webhook_test_data):
    """Collect webhook events from CloudWatch."""
    if not WEBHOOK_VERIFICATION_ENABLED:
        return

    logger.info("🔍 Collecting webhook deliveries for validation...")

    # Get the log group name and start time stored during setup
    metadata = webhook_test_data["metadata"]
    log_group_name = metadata.get("log_group_name")
    start_time = metadata.get("start_time")

    if not log_group_name or not start_time:
        logger.info("⚠️  Missing log group info, cannot collect events")
        return

    try:
        # Wait for async webhook delivery
        logger.info("Waiting 30s for async webhook delivery...")
        time.sleep(30)

        # Collect CloudWatch logs
        logger.info(f"Collecting logs from {log_group_name}...")
        log_events = collect_cloudwatch_logs(session, log_group_name, start_time)
        logger.info(f"Found {len(log_events)} log events")

        # Parse webhook events from logs
        webhook_events = parse_webhook_events(log_events)
        total_count = sum(len(events) for events in webhook_events.values())
        logger.info(
            f"Received {total_count} webhook deliveries across {len(webhook_events)} identifiers"
        )
        for identifier, events in webhook_events.items():
            logger.info(f"  - {identifier}: {len(events)} deliveries")

        # Store events in webhook_test_data
        for identifier, webhook_info in webhook_test_data["webhooks"].items():
            webhook_info["events"] = webhook_events.get(identifier, [])

        logger.info("✅ Webhook events collected and stored\n")

    # pylint: disable=broad-exception-caught
    except Exception as e:
        logger.error(f"⚠️  Failed to collect webhook logs: {e}")


@pytest.fixture
def expect_webhooks(webhook_expectations):
    """Register expected webhooks from a test."""

    def _expect(*event_types):
        """Register one or more expected webhook events."""
        if WEBHOOK_VERIFICATION_ENABLED:
            frame = inspect.currentframe().f_back
            test_name = frame.f_code.co_name

            for item in event_types:
                expectation = {"test_name": test_name}

                if isinstance(item, str):
                    # Simple event type for counting
                    expectation["event_type"] = item
                elif (
                    isinstance(item, tuple)
                    and len(item) == 2
                    and isinstance(item[0], dict)
                ):
                    # (body_dict, extra_excludes)
                    expectation["event_type"] = item[0]["event_type"]
                    expectation["body"] = deepcopy(item[0])
                    expectation["extra_excludes"] = item[1]
                elif isinstance(item, dict):
                    # body_dict only
                    expectation["event_type"] = item["event_type"]
                    expectation["body"] = deepcopy(item)
                    expectation["extra_excludes"] = []

                webhook_expectations.append(expectation)

    return _expect


#############
# FUNCTIONS #
#############


# pylint: disable=redefined-outer-name
def get_client_secret(session, user_pool_id, client_id, client_region):
    idp = session.client("cognito-idp", region_name=client_region)
    user_pool_client = idp.describe_user_pool_client(
        UserPoolId=user_pool_id, ClientId=client_id
    )
    return user_pool_client["UserPoolClient"]["ClientSecret"]


def assert_equal_unordered(obj1, obj2):
    """Assert that two objects are equal, ignoring order of lists and sets."""
    diff = DeepDiff(obj1, obj2, ignore_order=True)
    if diff:
        raise AssertionError(f"Objects are not equal:\n{diff}")


def assert_json_response(response, status_code, empty_body=False):
    """Assert that response has expected status code and JSON content-type."""
    assert status_code == response.status_code
    headers = {k.lower(): v for k, v in response.headers.items()}
    assert "content-type" in headers
    assert "application/json" == headers["content-type"]
    if empty_body:
        assert "" == response.content.decode("utf-8")


def assert_headers_present(response, *headers):
    """Assert that specified headers are present in the response (case-insensitive)."""
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    for header in headers:
        assert header in response_headers_lower, f"Header '{header}' not found"


def remove_fields(record, *fields):
    """Remove specified fields from a dict without mutating the original."""
    return {k: v for k, v in record.items() if k not in fields}


def remove_dynamic_props(records):
    """Remove dynamic properties from records (single dict or list of dicts)."""
    # Handle single dict
    if isinstance(records, dict):
        for prop in DYNAMIC_PROPS:
            if prop in records:
                del records[prop]
        return records

    # Handle list of dicts
    for prop in DYNAMIC_PROPS:
        for record in records:
            if prop in record:
                del record[prop]
    return records


def create_storage_label(suffix=""):
    return f"aws.{REGION}:s3{f'.{suffix}' if suffix else ''}:{STORE_NAME}"


def default_get_urls():
    """Return the standard get_urls structure for webhook expectations."""
    return [
        {
            "label": create_storage_label(),
        },
        {
            "presigned": True,
            "label": create_storage_label("presigned"),
        },
    ]


def deploy_cloudformation_stack(session, stack_name, template_file):
    """Deploy a CloudFormation stack."""
    template_path = os.path.join(os.path.dirname(__file__), template_file)
    cf = session.client("cloudformation", region_name=REGION)
    with open(template_path, "r", encoding="utf-8") as f:
        template_body = f.read()
    cf.create_stack(
        StackName=stack_name,
        TemplateBody=template_body,
        Capabilities=["CAPABILITY_IAM", "CAPABILITY_AUTO_EXPAND"],
    )


def wait_for_stack_status(session, stack_name, status, timeout=600):
    """Wait for CloudFormation stack to complete creation."""
    cf = session.client("cloudformation", region_name=REGION)
    waiter = cf.get_waiter(status)
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
            raise Exception(f"Stack failed: {reason}") from e
        raise


def get_stack_outputs(session, stack_name):
    """Get outputs from CloudFormation stack."""
    cf = session.client("cloudformation", region_name=REGION)
    response = cf.describe_stacks(StackName=stack_name)
    stack = response["Stacks"][0]
    outputs = {}
    for output in stack.get("Outputs", []):
        outputs[output["OutputKey"]] = output["OutputValue"]
    return outputs


def get_api_key_value(session, api_key_id):
    """Retrieve API key value from API Gateway."""
    apigw = session.client("apigateway", region_name=REGION)
    response = apigw.get_api_key(apiKey=api_key_id, includeValue=True)
    return response["value"]


def delete_cloudformation_stack(session, stack_name):
    """Delete the webhook test CloudFormation stack."""
    cf = session.client("cloudformation", region_name=REGION)
    cf.delete_stack(StackName=stack_name)


def collect_cloudwatch_logs(session, log_group_name, start_time):
    """Collect log events from CloudWatch Logs."""
    logs = session.client("logs", region_name=REGION)
    events = []
    kwargs = {
        "logGroupName": log_group_name,
        "startTime": start_time,
        "endTime": int(time.time() * 1000),
        "filterPattern": "{ $.pathParameters.identifier = * }",
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
