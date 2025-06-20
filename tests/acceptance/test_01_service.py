import pytest
import requests

pytestmark = [
    pytest.mark.acceptance,
]


@pytest.mark.parametrize(
    "path, verb",
    [
        ("/", "GET"),
        ("/", "HEAD"),
        ("/service", "GET"),
        ("/service", "HEAD"),
        ("/service", "POST"),
        ("/service/webhooks", "GET"),
        ("/service/webhooks", "HEAD"),
        ("/service/webhooks", "POST"),
    ],
)
def test_auth_401(verb, path, api_endpoint):
    # Arrange
    url = f"{api_endpoint}{path}"
    # Act
    response = requests.request(
        verb,
        url=url,
        timeout=30,
    )
    # Assert
    assert 401 == response.status_code


def test_List_Root_Endpoints_HEAD_200(api_client_cognito):
    # Arrange
    path = "/"
    # Act
    response = api_client_cognito.request("HEAD", path)
    # Assert
    assert 200 == response.status_code


def test_List_Root_Endpoints_GET_200(api_client_cognito):
    # Arrange
    path = "/"
    # Act
    response = api_client_cognito.request("GET", path)
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert set(
        ["service", "flows", "sources", "objects", "flow-delete-requests"]
    ) == set(response.json())


def test_Service_Information_HEAD_200(api_client_cognito):
    # Arrange
    path = "/service"
    # Act
    response = api_client_cognito.request("HEAD", path)
    # Assert
    assert 200 == response.status_code


def test_Service_Information_GET_200(api_client_cognito, webhooks_enabled):
    # Arrange
    path = "/service"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "type" in response.json()
    assert "api_version" in response.json()
    assert "service_version" in response.json()
    assert "media_store" in response.json()
    if webhooks_enabled:
        assert "event_stream_mechanisms" in response.json()
    else:
        assert "event_stream_mechanisms" not in response.json()


def test_Update_Service_Information_POST_200(api_client_cognito):
    # Arrange
    path = "/service"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "name": "Example TAMS",
            "description": "An example Time Addressable Media Store",
        },
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert None is response.json()


def test_Update_Service_Information_POST_400(api_client_cognito):
    # Arrange
    path = "/service"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        data="bad data",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Register_Webhook_URL_POST_201_create(api_client_cognito, webhooks_enabled):
    if webhooks_enabled:
        # Arrange
        path = "/service/webhooks"
        webhook = {
            "url": "https://hook.example.com",
            "api_key_name": "Authorization",
            "api_key_value": "Bearer 21238dksdjqwpqscj9",
            "events": ["flows/created", "flows/updated", "flows/deleted"],
        }
        # Act
        response = api_client_cognito.request(
            "POST",
            path,
            json=webhook,
        )
        response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
        # Assert
        assert 201 == response.status_code
        assert "content-type" in response_headers_lower
        assert "application/json" == response_headers_lower["content-type"]
        assert webhook == response.json()


def test_Register_Webhook_URL_POST_201_update(api_client_cognito, webhooks_enabled):
    if webhooks_enabled:
        # Arrange
        path = "/service/webhooks"
        webhook = {
            "url": "https://hook.example.com",
            "api_key_name": "Authorization",
            "api_key_value": "Bearer 12138dksdjqwpqscj9",
            "events": ["flows/created", "flows/updated"],
        }
        # Act
        response = api_client_cognito.request(
            "POST",
            path,
            json=webhook,
        )
        response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
        # Assert
        assert 201 == response.status_code
        assert "content-type" in response_headers_lower
        assert "application/json" == response_headers_lower["content-type"]
        assert webhook == response.json()


def test_Register_Webhook_URL_POST_400(api_client_cognito, webhooks_enabled):
    if webhooks_enabled:
        # Arrange
        path = "/service/webhooks"
        # Act
        response = api_client_cognito.request(
            "POST",
            path,
            data="bad data",
        )
        response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
        # Assert
        assert 400 == response.status_code
        assert "content-type" in response_headers_lower
        assert "application/json" == response_headers_lower["content-type"]
        assert isinstance(response.json()["message"], list)
        assert 0 < len(response.json()["message"])


def test_Register_Webhook_URL_POST_404(api_client_cognito, webhooks_enabled):
    if not webhooks_enabled:
        # Arrange
        path = "/service/webhooks"
        webhook = {
            "url": "https://hook.example.com",
            "api_key_name": "Authorization",
            "api_key_value": "Bearer 21238dksdjqwpqscj9",
            "events": ["flows/created", "flows/updated", "flows/deleted"],
        }
        # Act
        response = api_client_cognito.request(
            "POST",
            path,
            json=webhook,
        )
        response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
        # Assert
        assert 404 == response.status_code
        assert "content-type" in response_headers_lower
        assert "application/json" == response_headers_lower["content-type"]
        assert (
            "Webhooks are not supported by this API implementation"
            == response.json()["message"]
        )


def test_List_Webhook_URLs_HEAD_200(api_client_cognito, webhooks_enabled):
    if webhooks_enabled:
        # Arrange
        path = "/service/webhooks"
        # Act
        response = api_client_cognito.request(
            "HEAD",
            path,
        )
        response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
        # Assert
        assert 200 == response.status_code
        assert "content-type" in response_headers_lower
        assert "application/json" == response_headers_lower["content-type"]
        assert "" == response.content.decode("utf-8")


def test_List_Webhook_URLs_HEAD_404(api_client_cognito, webhooks_enabled):
    if not webhooks_enabled:
        # Arrange
        path = "/service/webhooks"
        # Act
        response = api_client_cognito.request(
            "HEAD",
            path,
        )
        response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
        # Assert
        assert 404 == response.status_code
        assert "content-type" in response_headers_lower
        assert "application/json" == response_headers_lower["content-type"]
        assert "" == response.content.decode("utf-8")


def test_List_Webhook_URLs_GET_200(api_client_cognito, webhooks_enabled):
    if webhooks_enabled:
        # Arrange
        path = "/service/webhooks"
        webhook = {
            "url": "https://hook.example.com",
            "api_key_name": "Authorization",
            "events": ["flows/created", "flows/updated"],
        }
        # Act
        response = api_client_cognito.request(
            "GET",
            path,
        )
        response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
        # Assert
        assert 200 == response.status_code
        assert "content-type" in response_headers_lower
        assert "application/json" == response_headers_lower["content-type"]
        assert [webhook] == response.json()


def test_List_Webhook_URLs_GET_404(api_client_cognito, webhooks_enabled):
    if not webhooks_enabled:
        # Arrange
        path = "/service/webhooks"
        # Act
        response = api_client_cognito.request(
            "GET",
            path,
        )
        response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
        # Assert
        assert 404 == response.status_code
        assert "content-type" in response_headers_lower
        assert "application/json" == response_headers_lower["content-type"]
        assert (
            "Webhooks are not supported by this API implementation"
            == response.json()["message"]
        )


def test_Register_Webhook_URL_POST_204(api_client_cognito, webhooks_enabled):
    if webhooks_enabled:
        # Arrange
        path = "/service/webhooks"
        webhook = {
            "url": "https://hook.example.com",
            "events": [],
        }
        # Act
        response = api_client_cognito.request(
            "POST",
            path,
            json=webhook,
        )
        response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
        # Assert
        assert 204 == response.status_code
        assert "content-type" in response_headers_lower
        assert "application/json" == response_headers_lower["content-type"]
        assert "" == response.content.decode("utf-8")
