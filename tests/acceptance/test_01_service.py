import pytest
import requests
from conftest import (
    ID_404,
    REGION,
    STORE_NAME,
    assert_equal_unordered,
    assert_headers_present,
    assert_json_response,
    create_storage_label,
)

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
        ("/service/webhooks/{webhookId}", "GET"),
        ("/service/webhooks/{webhookId}", "HEAD"),
        ("/service/webhooks/{webhookId}", "PUT"),
        ("/service/webhooks/{webhookId}", "DELETE"),
        ("/service/storage-backends", "GET"),
        ("/service/storage-backends", "HEAD"),
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
    assert_json_response(response, 401)


def test_List_Root_Endpoints_HEAD_200(api_client_cognito):
    # Arrange
    path = "/"
    # Act
    response = api_client_cognito.request("HEAD", path)
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Root_Endpoints_GET_200(api_client_cognito):
    # Arrange
    path = "/"
    # Act
    response = api_client_cognito.request("GET", path)
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert set(
        ["service", "flows", "sources", "objects", "flow-delete-requests"]
    ) == set(response_json)


def test_Service_Information_HEAD_200(api_client_cognito):
    # Arrange
    path = "/service"
    # Act
    response = api_client_cognito.request("HEAD", path)
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_Service_Information_GET_200(api_client_cognito):
    # Arrange
    path = "/service"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "type" in response_json
    assert "api_version" in response_json
    assert "service_version" in response_json
    assert "event_stream_mechanisms" in response_json
    assert "min_object_timeout" in response_json
    assert "min_presigned_url_timeout" in response_json


def test_Update_Service_Information_POST_200(api_client_cognito):
    # Arrange
    path = "/service"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "name": STORE_NAME,
            "description": "An example Time Addressable Media Store",
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert None is response_json


def test_Update_Service_Information_POST_400(api_client_cognito):
    # Arrange
    path = "/service"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        data="bad data",
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


def test_Register_Webhook_URL_POST_201_create(
    api_client_cognito, stub_webhook_basic, webhook_ids
):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={**stub_webhook_basic, "api_key_value": "Bearer 21238dksdjqwpqscj9"},
    )
    # Assert
    assert_json_response(response, 201)
    response_json = response.json()
    assert "id" in response_json
    webhook_ids.append(response_json.pop("id"))
    assert {**stub_webhook_basic, "status": "created"} == response_json


def test_Register_Webhook_URL_POST_201_create_tags(
    api_client_cognito, stub_webhook_tags, webhook_ids
):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={**stub_webhook_tags, "api_key_value": "Bearer 21238dksdjqwpqscj9"},
    )
    # Assert
    assert_json_response(response, 201)
    response_json = response.json()
    assert "id" in response_json
    webhook_ids.append(response_json.pop("id"))
    assert {**stub_webhook_tags, "status": "created"} == response_json


def test_Register_Webhook_URL_POST_201_create_empty_events(
    api_client_cognito, stub_webhook_basic, webhook_ids
):
    # Arrange
    path = "/service/webhooks"
    webhook = {**stub_webhook_basic, "events": []}
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={**webhook, "api_key_value": "Bearer 21238dksdjqwpqscj9"},
    )
    # Assert
    assert_json_response(response, 201)
    response_json = response.json()
    assert "id" in response_json
    webhook_ids.append(response_json.pop("id"))
    assert {**webhook, "status": "created"} == {**response_json, "events": []}


def test_Register_Webhook_URL_POST_400_invalid_json(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        data="bad data",
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])
    assert response_json["message"][0]["type"] == "json_invalid"


def test_Register_Webhook_URL_POST_400_missing_url(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "events": ["flows/created", "flows/updated", "flows/deleted"],
        },
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])
    assert response_json["message"][0]["type"] == "missing"
    assert response_json["message"][0]["loc"] == ["body", "url"]


def test_Register_Webhook_URL_POST_400_invalid_events(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "url": "https://hook.example.com",
            "events": ["invalid"],
        },
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])
    assert response_json["message"][0]["type"] == "enum"


def test_List_Webhook_URLs_HEAD_200(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Webhook_URLs_HEAD_200_tag_name(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"tag.auth_classes": "news"}
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Webhook_URLs_HEAD_200_tag_name_not_found(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"tag.auth_classes": "dummy"}
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Webhook_URLs_HEAD_200_tag_exists_name_true(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"tag_exists.auth_classes": "true"}
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Webhook_URLs_HEAD_200_tag_exists_name_false(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"tag_exists.auth_classes": "false"}
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Webhook_URLs_HEAD_200_limit(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request("HEAD", path, params={"limit": "1"})
    # Assert
    assert_json_response(response, 200, empty_body=True)
    assert_headers_present(response, "link", "x-paging-limit", "x-paging-nextkey")


def test_List_Webhook_URLs_HEAD_200_page(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request("HEAD", path, params={"page": "1"})
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Webhook_URLs_GET_200(
    api_client_cognito,
    webhook_ids,
    stub_webhook_basic,
    stub_webhook_tags,
    webhook_verification_enabled,
):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    expected_count = len(webhook_ids) + (1 if webhook_verification_enabled else 0)
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert expected_count == len(response_json)
    assert {
        **stub_webhook_basic,
        "id": webhook_ids[0],
        "status": "created",
    } in response_json
    assert {
        **stub_webhook_tags,
        "id": webhook_ids[1],
        "status": "created",
    } in response_json


def test_List_Webhook_URLs_GET_200_tag_name(
    api_client_cognito, webhook_ids, stub_webhook_tags
):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag.auth_classes": "news"}
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 1 == len(response_json)
    assert {
        **stub_webhook_tags,
        "id": webhook_ids[1],
        "status": "created",
    } in response_json


def test_List_Webhook_URLs_GET_200_tag_name_partial(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag.auth_classes": "new"}
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 0 == len(response_json)


def test_List_Webhook_URLs_GET_200_tag_name_not_found(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag.auth_classes": "dummy"}
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 0 == len(response_json)


def test_List_Webhook_URLs_GET_200_tag_exists_name_true(
    api_client_cognito, webhook_ids, stub_webhook_tags
):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag_exists.auth_classes": "true"}
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 1 == len(response_json)
    assert {
        **stub_webhook_tags,
        "id": webhook_ids[1],
        "status": "created",
    } in response_json


def test_List_Webhook_URLs_GET_200_tag_exists_name_false(
    api_client_cognito,
    webhook_ids,
    stub_webhook_basic,
    webhook_verification_enabled,
):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag_exists.auth_classes": "false"}
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert (3 if webhook_verification_enabled else 2) == len(response_json)
    assert {
        **stub_webhook_basic,
        "id": webhook_ids[0],
        "status": "created",
    } in response_json


def test_List_Webhook_URLs_GET_400_tag_exists_name_bad(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag_exists.auth_classes": "bad"}
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])
    assert response_json["message"][0]["type"] == "bool_parsing"


def test_List_Webhook_URLs_GET_200_limit(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request("GET", path, params={"limit": "1"})
    # Assert
    assert_json_response(response, 200)
    assert_headers_present(response, "link", "x-paging-limit", "x-paging-nextkey")
    response_json = response.json()
    assert 1 == len(response_json)


def test_List_Webhook_URLs_GET_400_limit_bad(api_client_cognito):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request("GET", path, params={"limit": "a"})
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])
    assert response_json["message"][0]["type"] == "int_parsing"


def test_List_Webhook_URLs_GET_200_page(
    api_client_cognito, webhook_verification_enabled
):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request("GET", path, params={"page": "1"})
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert (3 if webhook_verification_enabled else 2) == len(response_json)


def test_Webhook_Details_HEAD_200(api_client_cognito, webhook_ids):
    # Arrange
    path = f"/service/webhooks/{webhook_ids[0]}"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_Webhook_Details_HEAD_400(api_client_cognito):
    # Arrange
    path = "/service/webhooks/bad-id"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 400, empty_body=True)


def test_Webhook_Details_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/service/webhooks/{ID_404}"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 404, empty_body=True)


def test_Webhook_Details_GET_200(api_client_cognito, webhook_ids, stub_webhook_basic):
    # Arrange
    path = f"/service/webhooks/{webhook_ids[0]}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert {
        **stub_webhook_basic,
        "id": webhook_ids[0],
        "status": "created",
    } == response_json


def test_Webhook_Details_GET_400(api_client_cognito):
    # Arrange
    path = "/service/webhooks/bad-id"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])
    assert response_json["message"][0]["type"] == "string_pattern_mismatch"


def test_Webhook_Details_GET_404(api_client_cognito):
    # Arrange
    path = f"/service/webhooks/{ID_404}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert (
        "The requested Webhook ID in the path is invalid." == response_json["message"]
    )


def test_Register_Webhook_URL_PUT_201_update(
    api_client_cognito, webhook_ids, stub_webhook_basic
):
    # Arrange
    path = f"/service/webhooks/{webhook_ids[0]}"
    webhook = {
        **stub_webhook_basic,
        "events": ["flows/created", "flows/updated"],
        "id": webhook_ids[0],
        "status": "created",
    }
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json={**webhook, "api_key_value": "Bearer 21238dksdjqwpqscj9"},
    )
    # Assert
    assert_json_response(response, 201)
    response_json = response.json()
    assert webhook == response_json


def test_Register_Webhook_URL_PUT_400_update(api_client_cognito, stub_webhook_basic):
    # Arrange
    path = "/service/webhooks/bad-id"
    webhook = {**stub_webhook_basic, "id": "bad-id"}
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=webhook,
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])
    assert response_json["message"][0]["type"] == "string_pattern_mismatch"


def test_Register_Webhook_URL_PUT_404_update(api_client_cognito, stub_webhook_basic):
    # Arrange
    path = f"/service/webhooks/{ID_404}"
    webhook = {**stub_webhook_basic, "id": ID_404, "status": "created"}
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=webhook,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert (
        "The requested Webhook ID in the path is invalid." == response_json["message"]
    )


def test_Register_Webhook_URL_PUT_400_update_missing_fields(
    api_client_cognito, webhook_ids
):
    # Arrange
    path = f"/service/webhooks/{webhook_ids[0]}"
    missing_fields = {"url", "id", "events", "status"}
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json={},
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert len(missing_fields) == len(response_json["message"])
    assert ["missing"] * len(missing_fields) == [
        msg["type"] for msg in response_json["message"]
    ]
    assert missing_fields == set(msg["loc"][1] for msg in response_json["message"])


def test_Register_Webhook_URL_DELETE_204(api_client_cognito, webhook_ids):
    for webhook_id in webhook_ids:
        # Arrange
        path = f"/service/webhooks/{webhook_id}"
        # Act
        response = api_client_cognito.request(
            "DELETE",
            path,
        )
        # Assert
        assert_json_response(response, 204, empty_body=True)


def test_Service_StorageBackends_HEAD_200(api_client_cognito):
    # Arrange
    path = "/service/storage-backends"
    # Act
    response = api_client_cognito.request("HEAD", path)
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_Service_StorageBackends_GET_200(api_client_cognito, default_storage_id):
    # Arrange
    path = "/service/storage-backends"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert isinstance(response_json, list)
    assert 1 == len(response_json)
    assert_equal_unordered(
        {
            "store_type": "http_object_store",
            "provider": "aws",
            "region": REGION,
            "store_product": "s3",
            "id": default_storage_id,
            "label": create_storage_label(),
            "default_storage": True,
        },
        response_json[0],
    )
