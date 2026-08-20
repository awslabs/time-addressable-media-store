import pytest
import requests

# pylint: disable=no-name-in-module
from conftest import (
    ID_404,
    REGION,
    STORE_NAME,
    assert_equal_unordered,
    assert_headers_present,
    assert_json_response,
    create_storage_label,
    remove_dynamic_props,
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
        ("/service/profiles", "GET"),
        ("/service/profiles", "HEAD"),
        ("/service/profiles/{profileId}", "GET"),
        ("/service/profiles/{profileId}", "HEAD"),
        ("/service/profiles/{profileId}", "POST"),
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
    assert_headers_present(
        response, "link", "x-paging-limit", "x-paging-nextkey", "x-paging-count"
    )


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
    webhook_test_data,
):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    expected_count = len(webhook_ids) + len(webhook_test_data["webhooks"])
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


def test_List_Webhook_URLs_GET_200_sort_by_url(
    api_client_cognito, webhook_ids, webhook_test_data
):
    """List webhooks sorted by url (ascending alphabetical by default)"""
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request("GET", path)
    # Assert
    assert_json_response(response, 200)
    assert_headers_present(response, "x-paging-reverse-order")
    assert "False" == response.headers["X-Paging-Reverse-Order"]
    urls = [webhook["url"] for webhook in response.json()]
    assert urls == sorted(urls)


def test_List_Webhook_URLs_GET_200_reverse_order(
    api_client_cognito, webhook_ids, webhook_test_data
):
    """List webhooks sorted by url in reverse (descending alphabetical)"""
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request("GET", path, params={"reverse_order": "true"})
    # Assert
    assert_json_response(response, 200)
    assert_headers_present(response, "x-paging-reverse-order")
    assert "True" == response.headers["X-Paging-Reverse-Order"]
    urls = [webhook["url"] for webhook in response.json()]
    assert urls == sorted(urls, reverse=True)


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
    webhook_test_data,
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
    assert (2 + len(webhook_test_data["webhooks"])) == len(response_json)
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
    assert_headers_present(
        response, "link", "x-paging-limit", "x-paging-nextkey", "x-paging-count"
    )
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


def test_List_Webhook_URLs_GET_200_page(api_client_cognito, webhook_test_data):
    # Arrange
    path = "/service/webhooks"
    # Act
    response = api_client_cognito.request("GET", path, params={"page": "1"})
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert (2 + len(webhook_test_data["webhooks"])) == len(response_json)


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


def test_Service_StorageBackends_GET_200_sort_by_label(api_client_cognito):
    """Storage backends are sorted alphabetically by label by default"""
    # Arrange
    path = "/service/storage-backends"
    # Act
    response = api_client_cognito.request("GET", path)
    # Assert
    assert_json_response(response, 200)
    assert_headers_present(response, "x-paging-reverse-order")
    assert "False" == response.headers["X-Paging-Reverse-Order"]
    labels = [backend["label"] for backend in response.json()]
    assert labels == sorted(labels)


def test_Service_StorageBackends_GET_200_reverse_order(api_client_cognito):
    """Storage backends are sorted by label in reverse when reverse_order is set"""
    # Arrange
    path = "/service/storage-backends"
    # Act
    response = api_client_cognito.request("GET", path, params={"reverse_order": "true"})
    # Assert
    assert_json_response(response, 200)
    assert_headers_present(response, "x-paging-reverse-order")
    assert "True" == response.headers["X-Paging-Reverse-Order"]
    labels = [backend["label"] for backend in response.json()]
    assert labels == sorted(labels, reverse=True)


# Storage Backend tags are set out of band directly on the DynamoDB record (there
# is no API to set them), so a freshly deployed backend carries none. A positive
# tag match is therefore not provable here -- only that a filter correctly
# includes the untagged backend (tag_exists=false) or excludes it (any value
# filter, or tag_exists=true). Real discrimination is covered by the functional
# tier, whose fixture creates two tagged backends.
def test_Service_StorageBackends_HEAD_200_tag_exists_false(api_client_cognito):
    # Arrange
    path = "/service/storage-backends"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"tag_exists.anything": "false"}
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_Service_StorageBackends_GET_200_tag_exists_false(api_client_cognito):
    """tag_exists=false matches the untagged deployed backend, so it is returned."""
    # Arrange
    path = "/service/storage-backends"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag_exists.anything": "false"}
    )
    # Assert
    assert_json_response(response, 200)
    assert 1 == len(response.json())


def test_Service_StorageBackends_GET_200_tag_exists_true_empty(api_client_cognito):
    """tag_exists=true excludes the untagged deployed backend, giving an empty list."""
    # Arrange
    path = "/service/storage-backends"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag_exists.anything": "true"}
    )
    # Assert
    assert_json_response(response, 200)
    assert 0 == len(response.json())


def test_Service_StorageBackends_GET_200_tag_value_empty(api_client_cognito):
    """A tag value filter excludes the untagged deployed backend."""
    # Arrange
    path = "/service/storage-backends"
    # Act
    response = api_client_cognito.request("GET", path, params={"tag.anything": "value"})
    # Assert
    assert_json_response(response, 200)
    assert 0 == len(response.json())


def test_Service_StorageBackends_GET_400_bad_tag_exists(api_client_cognito):
    """A non-boolean tag_exists value is a 400."""
    # Arrange
    path = "/service/storage-backends"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag_exists.anything": "notabool"}
    )
    # Assert
    assert_json_response(response, 400)


# ---- Profiles (§3.2) ----
# stub_profile has a fresh random id and unique label per session; Profiles are
# immutable and cannot be deleted, so these must not assume an empty store or a
# fixed id. Tests below the create test depend on it having run (file order).
def test_Create_Profile_POST_201(api_client_cognito, stub_profile):
    # Arrange
    path = f"/service/profiles/{stub_profile['id']}"
    # Act
    response = api_client_cognito.request("POST", path, json=stub_profile)
    # Assert
    assert_json_response(response, 201)
    response_json = response.json()
    assert "created" in response_json
    assert "created_by" in response_json
    assert_equal_unordered(stub_profile, remove_dynamic_props(response_json))


def test_Create_Profile_POST_400_immutable(api_client_cognito, stub_profile):
    """Profiles are immutable: re-creating an existing one is a 400."""
    # Arrange
    path = f"/service/profiles/{stub_profile['id']}"
    # Act
    response = api_client_cognito.request("POST", path, json=stub_profile)
    # Assert
    assert_json_response(response, 400)


def test_Create_Profile_POST_404_id_mismatch(api_client_cognito, stub_profile):
    """A body id that differs from the path id is a 404."""
    # Arrange
    path = f"/service/profiles/{ID_404}"
    # Act
    response = api_client_cognito.request("POST", path, json=stub_profile)
    # Assert
    assert_json_response(response, 404)


def test_Create_Profile_POST_400_invalid(api_client_cognito):
    """A Profile body without flow_metadata is invalid."""
    # Arrange
    path = f"/service/profiles/{ID_404}"
    # Act
    response = api_client_cognito.request(
        "POST", path, json={"id": ID_404, "label": "no metadata"}
    )
    # Assert
    assert_json_response(response, 400)


def test_Get_Profile_HEAD_200(api_client_cognito, stub_profile):
    # Arrange
    path = f"/service/profiles/{stub_profile['id']}"
    # Act
    response = api_client_cognito.request("HEAD", path)
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_Get_Profile_GET_200(api_client_cognito, stub_profile):
    # Arrange
    path = f"/service/profiles/{stub_profile['id']}"
    # Act
    response = api_client_cognito.request("GET", path)
    # Assert
    assert_json_response(response, 200)
    assert_equal_unordered(stub_profile, remove_dynamic_props(response.json()))


def test_Get_Profile_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/service/profiles/{ID_404}"
    # Act
    response = api_client_cognito.request("HEAD", path)
    # Assert
    assert_json_response(response, 404, empty_body=True)


def test_Get_Profile_GET_404(api_client_cognito):
    # Arrange
    path = f"/service/profiles/{ID_404}"
    # Act
    response = api_client_cognito.request("GET", path)
    # Assert
    assert_json_response(response, 404)


def test_List_Profiles_HEAD_200(api_client_cognito):
    # Arrange
    path = "/service/profiles"
    # Act
    response = api_client_cognito.request("HEAD", path)
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Profiles_GET_200(api_client_cognito, stub_profile):
    """The created Profile appears in the unfiltered list."""
    # Arrange
    path = "/service/profiles"
    # Act
    response = api_client_cognito.request("GET", path)
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert isinstance(response_json, list)
    assert stub_profile["id"] in [profile["id"] for profile in response_json]


def test_List_Profiles_GET_200_label(api_client_cognito, stub_profile):
    """The Profile's session-unique label filters to exactly it."""
    # Arrange
    path = "/service/profiles"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"label": stub_profile["label"]}
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 1 == len(response_json)
    assert_equal_unordered(stub_profile, remove_dynamic_props(response_json[0]))


def test_List_Profiles_GET_200_format(api_client_cognito, stub_profile):
    """format filters Profiles: the video Profile matches video, not audio."""
    # Arrange
    path = "/service/profiles"
    # Act
    match = api_client_cognito.request(
        "GET", path, params={"format": "urn:x-nmos:format:video"}
    )
    no_match = api_client_cognito.request(
        "GET", path, params={"format": "urn:x-nmos:format:audio"}
    )
    # Assert
    assert_json_response(match, 200)
    assert_json_response(no_match, 200)
    assert stub_profile["id"] in [profile["id"] for profile in match.json()]
    assert stub_profile["id"] not in [profile["id"] for profile in no_match.json()]


def test_List_Profiles_GET_200_codec(api_client_cognito, stub_profile):
    """codec filters Profiles: the h264 Profile matches h264, not aac."""
    # Arrange
    path = "/service/profiles"
    # Act
    match = api_client_cognito.request("GET", path, params={"codec": "video/h264"})
    no_match = api_client_cognito.request("GET", path, params={"codec": "audio/aac"})
    # Assert
    assert_json_response(match, 200)
    assert_json_response(no_match, 200)
    assert stub_profile["id"] in [profile["id"] for profile in match.json()]
    assert stub_profile["id"] not in [profile["id"] for profile in no_match.json()]


def test_List_Profiles_GET_400_format(api_client_cognito):
    """An invalid format enum value is a 400."""
    # Arrange
    path = "/service/profiles"
    # Act
    response = api_client_cognito.request("GET", path, params={"format": "invalid"})
    # Assert
    assert_json_response(response, 400)
