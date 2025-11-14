import os
from copy import deepcopy

import boto3
import pytest
import requests
from deepdiff import DeepDiff

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
def access_token(stack, session, region):
    user_pool_id = stack["outputs"]["UserPoolId"]
    client_id = stack["outputs"]["UserPoolClientId"]
    client_secret = get_client_secret(session, user_pool_id, client_id, region)
    form_data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "tams-api/admin tams-api/read tams-api/write tams-api/delete",
    }
    resp = requests.post(stack["outputs"]["TokenUrl"], data=form_data, timeout=30)
    resp.raise_for_status()
    token_response = resp.json()
    return token_response["access_token"]


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def api_client_cognito(access_token, api_endpoint):
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

    hds = {"Authorization": f"Bearer {access_token}"}

    return ApiGwSession(api_endpoint, hds)


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
