from copy import deepcopy

import boto3
import pytest
import requests

STACK_NAME = "tams-api"
REGION = "eu-west-1"


############
# FIXTURES #
############


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def region():
    return REGION


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def stack(region):
    cloudformation = boto3.resource("cloudformation", region_name=region)
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
def webhooks_enabled(stack):
    enable_webHooks = stack["parameters"]["EnableWebhooks"]
    return enable_webHooks.lower() == "yes"


@pytest.fixture(scope="session")
# pylint: disable=redefined-outer-name
def access_token(stack, region):
    user_pool_id = stack["outputs"]["UserPoolId"]
    client_id = stack["outputs"]["UserPoolClientId"]
    client_secret = get_client_secret(user_pool_id, client_id, region)
    form_data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "tams-api/read tams-api/write tams-api/delete",
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
def media_objects():
    return []


@pytest.fixture(scope="session")
def delete_requests():
    return []


#############
# FUNCTIONS #
#############


def get_client_secret(user_pool_id, client_id, client_region):
    idp = boto3.client("cognito-idp", region_name=client_region)
    user_pool_client = idp.describe_user_pool_client(
        UserPoolId=user_pool_id, ClientId=client_id
    )
    return user_pool_client["UserPoolClient"]["ClientSecret"]
