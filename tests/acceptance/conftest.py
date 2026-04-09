import os
import time
from copy import deepcopy

import boto3
import pytest
import requests
from deepdiff import DeepDiff
from webhook_helpers import (
    collect_cloudwatch_logs,
    compare_webhook_counts,
    delete_webhook_stack,
    deploy_webhook_stack,
    get_api_key_value,
    get_stack_outputs,
    parse_webhook_events,
    validate_webhook_events,
    wait_for_stack_create,
)

STACK_NAME = os.environ["TAMS_STACK_NAME"]
REGION = os.environ["TAMS_REGION"]
PROFILE = os.environ["AWS_PROFILE"]


############
# FIXTURES #
############


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def profile():
    return PROFILE


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def region():
    return REGION


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def session(profile):
    return boto3.Session(profile_name=profile)


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def stack(region, session):
    cloudformation = session.resource("cloudformation", region_name=region)
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
def token_factory(stack, session, region):
    """Create tokens with specific scopes (cached per scope combination)."""
    cache = {}

    def _get_token(scopes=None):
        cache_key = tuple(sorted(scopes)) if scopes else None
        if cache_key in cache:
            return cache[cache_key]

        user_pool_id = stack["outputs"]["UserPoolId"]
        client_id = stack["outputs"]["UserPoolClientId"]
        client_secret = get_client_secret(session, user_pool_id, client_id, region)
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
def storage_backends():
    return []


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def default_storage_id(storage_backends):
    return next(
        (
            storage_backend["id"]
            for storage_backend in storage_backends
            if storage_backend.get("default_storage", False)
        ),
        None,
    )


@pytest.fixture(scope="session")
def media_objects():
    return []


@pytest.fixture(scope="session")
def delete_requests():
    return []


@pytest.fixture(scope="session")
def dynamic_props():
    return [
        "created_by",
        "updated_by",
        "created",
        "metadata_updated",
        "updated",
        "source_collection",
        "segments_updated",
    ]


@pytest.fixture(scope="session")
def id_404():
    return "00000000-0000-1000-8000-00000000000a"


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
    }


@pytest.fixture(scope="session")
def stub_audio_source():
    return {
        "id": "00000000-0000-1000-8000-000000000001",
        "format": "urn:x-nmos:format:audio",
        "label": "pytest - audio",
        "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
    }


@pytest.fixture(scope="session")
def stub_data_source():
    return {
        "id": "00000000-0000-1000-8000-000000000002",
        "format": "urn:x-nmos:format:data",
        "label": "pytest - data",
        "description": "pytest - data",
        "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
    }


@pytest.fixture(scope="session")
def stub_image_source():
    return {
        "id": "00000000-0000-1000-8000-000000000004",
        "format": "urn:x-tam:format:image",
        "label": "pytest - image",
        "description": "pytest - image",
        "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
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
        "stack_name": None,
        "webhook_id": None,
        "start_time": None,
        "enabled": False,
    }


# pylint: disable=redefined-outer-name
@pytest.fixture(scope="session", autouse=True)
def webhook_verification_lifecycle(
    session, region, api_client_cognito, webhook_test_data, webhook_expectations
):
    """
    Automated webhook delivery verification.

    Enabled by default. Disable with: WEBHOOK_VERIFICATION_ENABLED=false

    Flow:
    1. Deploy webhook-api-gateway stack
    2. Get stack outputs (API URL, API Key, Log Group)
    3. Get API key value from API Gateway
    4. Register webhook with TAMS API
    5. [Tests run]
    6. Wait for async webhook delivery (30s)
    7. Collect CloudWatch logs
    8. Parse webhook events from logs
    9. Validate webhook structure and compare counts with expectations
    10. Cleanup webhook registration
    11. Delete CloudFormation stack
    """

    # Check if enabled (defaults to true, set to "false" to disable)
    enabled = os.getenv("WEBHOOK_VERIFICATION_ENABLED", "true").lower() == "true"
    webhook_test_data["enabled"] = enabled

    if not enabled:
        yield
        return

    print("\n🚀 Setting up webhook verification...")

    # Record start time for log filtering
    webhook_test_data["start_time"] = int(time.time() * 1000)

    try:
        # 1. Deploy CloudFormation stack
        stack_name = f"tams-webhook-test-{int(time.time())}"
        webhook_test_data["stack_name"] = stack_name

        template_path = os.path.join(
            os.path.dirname(__file__), "webhook-api-gateway.yaml"
        )

        print(f"   Deploying stack: {stack_name}...")
        deploy_webhook_stack(session, region, stack_name, template_path)
        wait_for_stack_create(session, region, stack_name)
        print("   ✅ Stack deployed")

        # 2. Get stack outputs
        outputs = get_stack_outputs(session, region, stack_name)
        webhook_url = outputs["ApiUrl"]
        api_key_id = outputs["ApiKeyId"]
        log_group_name = outputs["LogGroupName"]

        # 3. Get API key value
        api_key_value = get_api_key_value(session, region, api_key_id)

        # 4. Register webhook with TAMS
        print(f"   Registering webhook: {webhook_url}/test-events...")
        response = api_client_cognito.request(
            "POST",
            "/service/webhooks",
            json={
                "url": f"{webhook_url}/test-events",
                "api_key_name": "x-api-key",
                "api_key_value": api_key_value,
                "events": [
                    "flows/created",
                    "flows/updated",
                    "flows/deleted",
                    "flows/segments_added",
                    "flows/segments_deleted",
                    "sources/created",
                    "sources/updated",
                    "sources/deleted",
                ],
                "tags": {"_test_infrastructure": ["webhook_verification"]},
            },
        )
        response.raise_for_status()
        webhook_id = response.json()["id"]
        webhook_test_data["webhook_id"] = webhook_id
        print(f"   ✅ Webhook registered: {webhook_id}")

        print("🧪 Running tests with webhook verification enabled...\n")

    # pylint: disable=broad-exception-caught
    except Exception as e:
        print(f"\n❌ Webhook verification setup failed: {e}")
        print("   Continuing with tests anyway...\n")
        # Still yield to run tests
        yield
        # Cleanup on setup failure
        if webhook_test_data.get("stack_name"):
            try:
                delete_webhook_stack(session, region, webhook_test_data["stack_name"])
            # pylint: disable=broad-exception-caught
            except Exception:
                pass
        return

    # ========== TESTS RUN HERE ==========
    yield
    # =====================================

    print("\n🔍 Verifying webhook deliveries...")

    try:
        # 5. Wait for async webhook delivery
        print("   Waiting 30s for async webhook delivery...")
        time.sleep(30)

        # 6. Collect CloudWatch logs
        print(f"   Collecting logs from {log_group_name}...")
        log_events = collect_cloudwatch_logs(
            session, region, log_group_name, webhook_test_data["start_time"]
        )
        print(f"   Found {len(log_events)} log events")

        # 7. Parse webhook events
        webhook_events = parse_webhook_events(log_events)
        print(f"   📬 Received {len(webhook_events)} webhook deliveries")

        # 8. Validate webhooks
        if len(webhook_events) > 0:
            validate_webhook_events(webhook_events)
            print("   ✅ Webhook verification passed!")

            # Compare expected vs actual counts
            if webhook_expectations:
                print("\n   📊 Comparing expected vs actual webhook counts:")
                compare_webhook_counts(webhook_expectations, webhook_events)
            else:
                print(
                    "\n   ℹ️  No expectations registered (add expect_webhooks to tests)"
                )
        else:
            print("   ⚠️  No webhooks received")

    # pylint: disable=broad-exception-caught
    except Exception as e:
        print(f"\n⚠️  Webhook verification failed: {e}")
        print("   (Tests may have passed, but webhook delivery verification failed)")

    finally:
        # 9. Cleanup webhook registration
        if webhook_test_data.get("webhook_id"):
            try:
                print(f"   Deleting webhook: {webhook_id}...")
                api_client_cognito.request("DELETE", f"/service/webhooks/{webhook_id}")
                print("   ✅ Webhook deleted")
            # pylint: disable=broad-exception-caught
            except Exception as e:
                print(f"   ⚠️  Failed to delete webhook: {e}")

        # 10. Delete CloudFormation stack
        if webhook_test_data.get("stack_name"):
            try:
                print(f"   Deleting stack: {stack_name}...")
                delete_webhook_stack(session, region, stack_name)
                print("   ⏳ Stack deletion initiated (async)")
            # pylint: disable=broad-exception-caught
            except Exception as e:
                print(f"   ⚠️  Failed to delete stack: {e}")


# pylint: disable=redefined-outer-name
@pytest.fixture(scope="session")
def webhook_verification_enabled(webhook_test_data):
    """Check if webhook verification is enabled (useful for conditional test logic)."""
    return webhook_test_data["enabled"]


@pytest.fixture(scope="session")
def webhook_expectations():
    """Track expected webhook events from tests."""
    return []


@pytest.fixture
def expect_webhooks(webhook_expectations, webhook_test_data):
    """Register expected webhooks from a test."""

    def _expect(*event_types):
        """Register one or more expected webhook events."""
        if webhook_test_data["enabled"]:
            webhook_expectations.extend(event_types)

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
