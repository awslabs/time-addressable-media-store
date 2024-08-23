# pylint: disable=too-many-lines
# pylint: disable=no-name-in-module
import pytest
import requests
from constants import (
    AUDIO_FLOW,
    DATA_FLOW,
    DYNAMIC_PROPS,
    ID_404,
    MULTI_FLOW,
    VIDEO_FLOW,
)

pytestmark = [
    pytest.mark.flows,
]


@pytest.mark.no_auth
@pytest.mark.parametrize(
    "path, verb",
    [
        ("/flows", "GET"),
        ("/flows", "HEAD"),
        ("/flows/{flowId}", "DELETE"),
        ("/flows/{flowId}", "GET"),
        ("/flows/{flowId}", "HEAD"),
        ("/flows/{flowId}", "PUT"),
        ("/flows/{flowId}/description", "DELETE"),
        ("/flows/{flowId}/description", "GET"),
        ("/flows/{flowId}/description", "HEAD"),
        ("/flows/{flowId}/description", "PUT"),
        ("/flows/{flowId}/label", "DELETE"),
        ("/flows/{flowId}/label", "GET"),
        ("/flows/{flowId}/label", "HEAD"),
        ("/flows/{flowId}/label", "PUT"),
        ("/flows/{flowId}/read_only", "GET"),
        ("/flows/{flowId}/read_only", "HEAD"),
        ("/flows/{flowId}/read_only", "PUT"),
        ("/flows/{flowId}/tags", "GET"),
        ("/flows/{flowId}/tags", "HEAD"),
        ("/flows/{flowId}/tags/{name}", "DELETE"),
        ("/flows/{flowId}/tags/{name}", "GET"),
        ("/flows/{flowId}/tags/{name}", "HEAD"),
        ("/flows/{flowId}/tags/{name}", "PUT"),
        ("/flows/{flowId}/segments", "DELETE"),
        ("/flows/{flowId}/segments", "GET"),
        ("/flows/{flowId}/segments", "HEAD"),
        ("/flows/{flowId}/segments", "POST"),
        ("/flows/{flowId}/storage", "POST"),
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


def test_Create_or_Replace_Flow_PUT_201_VIDEO(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=VIDEO_FLOW,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 201 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    for prop in ["created_by", "created"]:
        assert prop in response_json
        del response_json[prop]
    assert VIDEO_FLOW == response_json


def test_Create_or_Replace_Flow_PUT_201_AUDIO(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=AUDIO_FLOW,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 201 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    for prop in ["created_by", "created"]:
        assert prop in response_json
        del response_json[prop]
    assert AUDIO_FLOW == response_json


def test_Create_or_Replace_Flow_PUT_201_DATA(api_client_cognito):
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=DATA_FLOW,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 201 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    for prop in ["created_by", "created"]:
        assert prop in response_json
        del response_json[prop]
    assert DATA_FLOW == response_json


def test_Create_or_Replace_Flow_PUT_201_MULTI(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=MULTI_FLOW,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 201 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    for prop in ["created_by", "created"]:
        assert prop in response_json
        del response_json[prop]
    assert MULTI_FLOW == response_json


def test_Create_or_Replace_Flow_PUT_204(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=MULTI_FLOW,
    )  # Assert
    assert 204 == response.status_code
    assert "" == response.content.decode("utf-8")


def test_Create_or_Replace_Flow_PUT_400(api_client_cognito):
    """Invalid body"""
    # Arrange
    flow_id = "10000000-0000-1000-8000-000000000004"
    path = "/flows/{flow_id}"
    body = {
        "id": flow_id,
    }
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=body,
    )
    # Assert
    assert 400 == response.status_code
    assert "Invalid request body" == response.json()["message"]


def test_Create_or_Replace_Flow_PUT_403(api_client_cognito):
    """Attempt to update a flow that is marked as read-only"""
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=DATA_FLOW,
    )
    # Assert
    assert 403 == response.status_code
    assert (
        "Forbidden. You do not have permission to modify this flow. It may be marked read-only."
        == response.json()["message"]
    )


def test_Create_or_Replace_Flow_PUT_404(api_client_cognito):
    """Flow Id in path does not match Flow Id in body"""
    # Arrange
    path = f"/flows/{ID_404}"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=MULTI_FLOW,
    )
    # Assert
    assert 404 == response.status_code
    assert "The requested Flow ID in the path is invalid." == response.json()["message"]


def test_List_Flows_HEAD_200(api_client_cognito):
    # Arrange
    path = "/flows"
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


def test_List_Flows_HEAD_200_codec(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("HEAD", path, params={"codec": "audio/aac"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_200_format(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"format": "urn:x-nmos:format:data"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_200_frame_height(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("HEAD", path, params={"frame_height": "1080"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_200_frame_width(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("HEAD", path, params={"frame_width": "1920"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_200_label(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("HEAD", path, params={"label": "pytest"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_200_limit(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("HEAD", path, params={"limit": "2"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "link" in response_headers_lower
    assert "x-paging-limit" in response_headers_lower
    assert "x-paging-nextkey" in response_headers_lower
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_200_page(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"page": VIDEO_FLOW["id"]}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_200_source_id(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"source_id": DATA_FLOW["source_id"]}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_200_tag_name(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("HEAD", path, params={"tag.test": "this"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_200_tag_exists_name(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"tag_exists.test": "false"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_200_timerange(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("HEAD", path, params={"timerange": "()"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_codec(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"codec": "audio/aac", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_format(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"format": "urn:x-nmos:format:data", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_frame_height(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"frame_height": "1080", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_frame_width(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"frame_width": "1920", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_label(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"label": "pytest", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_limit(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"limit": "2", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_page(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"page": VIDEO_FLOW["id"], "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_source_id(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"source_id": DATA_FLOW["source_id"], "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_tag_name(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"tag.test": "this", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_tag_exists_name(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"tag_exists.text": "false", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_timerange(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"timerange": "()", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_GET_200(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 4 == len(response.json())
    for prop in DYNAMIC_PROPS:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [
        {**VIDEO_FLOW, "collected_by": [MULTI_FLOW["id"]]},
        {**AUDIO_FLOW, "collected_by": [MULTI_FLOW["id"]]},
        {**DATA_FLOW, "collected_by": [MULTI_FLOW["id"]]},
        MULTI_FLOW,
    ] == response_json


def test_List_Flows_GET_200_codec(api_client_cognito):
    """List flows with codec query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"codec": "audio/aac"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in DYNAMIC_PROPS:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [{**AUDIO_FLOW, "collected_by": [MULTI_FLOW["id"]]}] == response_json


def test_List_Flows_GET_200_format(api_client_cognito):
    """List flows with format query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"format": "urn:x-nmos:format:data"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in DYNAMIC_PROPS:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [{**DATA_FLOW, "collected_by": [MULTI_FLOW["id"]]}] == response_json


def test_List_Flows_GET_200_frame_height(api_client_cognito):
    """List flows with frame_height query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"frame_height": "1080"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in DYNAMIC_PROPS:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [{**VIDEO_FLOW, "collected_by": [MULTI_FLOW["id"]]}] == response_json


def test_List_Flows_GET_200_frame_width(api_client_cognito):
    """List flows with frame_width query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"frame_width": "1920"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in DYNAMIC_PROPS:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [{**VIDEO_FLOW, "collected_by": [MULTI_FLOW["id"]]}] == response_json


def test_List_Flows_GET_200_label(api_client_cognito):
    """List flows with label query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"label": "pytest"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in DYNAMIC_PROPS:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [MULTI_FLOW] == response_json


def test_List_Flows_GET_200_limit(api_client_cognito):
    """List flows with limit query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"limit": "2"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "link" in response_headers_lower
    assert "x-paging-limit" in response_headers_lower
    assert "x-paging-nextkey" in response_headers_lower
    assert 2 == len(response.json())


def test_List_Flows_GET_200_page(api_client_cognito):
    """List flows with page query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"page": VIDEO_FLOW["id"]}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 3 == len(response.json())


def test_List_Flows_GET_200_source_id(api_client_cognito):
    """List flows with source_id query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"source_id": DATA_FLOW["source_id"]}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in DYNAMIC_PROPS:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [{**DATA_FLOW, "collected_by": [MULTI_FLOW["id"]]}] == response_json


def test_List_Flows_GET_200_tag_name(api_client_cognito):
    """List flows with tag.{name} query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"tag.test": "this"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in DYNAMIC_PROPS:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [MULTI_FLOW] == response_json


def test_List_Flows_GET_200_tag_exists_name(api_client_cognito):
    """List flows with tag_exists.{name} query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag_exists.test": "false"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 3 == len(response.json())


def test_List_Flows_GET_200_timerange(api_client_cognito):
    """List flows with timerange query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"timerange": "()"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 4 == len(response.json())


def test_List_Flows_GET_400(api_client_cognito):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"bad": "query"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_List_Flows_GET_400_codec(api_client_cognito):
    """List flows with codec query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"codec": "audio/aac", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_List_Flows_GET_400_format(api_client_cognito):
    """List flows with format query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"format": "urn:x-nmos:format:data", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_List_Flows_GET_400_frame_height(api_client_cognito):
    """List flows with frame_height query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"frame_height": "1080", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_List_Flows_GET_400_frame_width(api_client_cognito):
    """List flows with frame_width query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"frame_width": "1920", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_List_Flows_GET_400_label(api_client_cognito):
    """List flows with label query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"label": "pytest", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_List_Flows_GET_400_limit(api_client_cognito):
    """List flows with limit query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"limit": "2", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_List_Flows_GET_400_page(api_client_cognito):
    """List flows with page query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"page": VIDEO_FLOW["id"], "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_List_Flows_GET_400_source_id(api_client_cognito):
    """List flows with source_id query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"source_id": DATA_FLOW["source_id"], "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_List_Flows_GET_400_tag_name(api_client_cognito):
    """List flows with tag.{name} query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag.test": "this", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_List_Flows_GET_400_tag_exists_name(api_client_cognito):
    """List flows with tag_exists.{name} query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag_exists.text": "false", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_List_Flows_GET_400_timerange(api_client_cognito):
    """List flows with tag_exists.{name} query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"timerange": "()", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_Flow_Details_HEAD_200(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}'
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


def test_Flow_Details_HEAD_200_include_timerange(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"include_timerange": "true"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Flow_Details_HEAD_200_timerange(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}'
    # Act
    response = api_client_cognito.request("HEAD", path, params={"timerange": "()"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Flow_Details_HEAD_400(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}'
    # Act
    response = api_client_cognito.request("HEAD", path, params={"bad": "query"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Flow_Details_HEAD_400_include_timerange(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"include_timerange": "true", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Flow_Details_HEAD_400_timerange(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"include_timerange": "true", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Flow_Details_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}"
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


def test_Flow_Details_HEAD_404_include_timerange(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"include_timerange": "true"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Flow_Details_HEAD_404_timerange(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"include_timerange": "true"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Flow_Details_GET_200(api_client_cognito):
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    for prop in DYNAMIC_PROPS:
        if prop in response_json:
            del response_json[prop]
    assert {**DATA_FLOW, "collected_by": [MULTI_FLOW["id"]]} == response_json


def test_Flow_Details_GET_400(api_client_cognito):
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}'
    # Act
    response = api_client_cognito.request("GET", path, params={"bad": "query"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_Flow_Details_GET_400_include_timerange(api_client_cognito):
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"include_timerange": "true", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_Flow_Details_GET_400_timerange(api_client_cognito):
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"include_timerange": "true", "bad": "query"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


def test_Flow_Details_GET_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}"
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
    assert "The requested flow does not exist." == response.json()["message"]


def test_Flow_Details_GET_404_include_timerange(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"include_timerange": "true"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested flow does not exist." == response.json()["message"]


def test_Flow_Details_GET_404_timerange(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"include_timerange": "true"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested flow does not exist." == response.json()["message"]


def test_List_Flow_Tags_HEAD_200(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/tags'
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


def test_List_Flow_Tags_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/tags"
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


def test_List_Flow_Tags_GET_200(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/tags'
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
    assert MULTI_FLOW["tags"] == response.json()


def test_List_Flow_Tags_GET_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/tags"
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
    assert "The requested flow does not exist." == response.json()["message"]


def test_Flow_Tag_Value_HEAD_200(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/tags/flow_status'
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


def test_Flow_Tag_Value_HEAD_404_bad_tag(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/tags/does_not_exist'
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


def test_Flow_Tag_Value_HEAD_404_bad_flow_id(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/tags/flow_status"
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


def test_Flow_Tag_Value_GET_200(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/tags/flow_status'
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
    assert MULTI_FLOW["tags"]["flow_status"] == response.content.decode("utf-8")


def test_Flow_Tag_Value_GET_404_bad_tag(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/tags/does_not_exist'
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
    assert "The requested flow or tag does not exist." == response.json()["message"]


def test_Flow_Tag_Value_GET_404_bad_flow_id(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/tags/flow_status"
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
    assert "The requested flow or tag does not exist." == response.json()["message"]


def test_Create_or_Update_Flow_Tag_PUT_204_create(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/tags/pytest'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Tag_PUT_204_update(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/tags/test'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="something else",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Tag_PUT_400(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/tags/pytest'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        data="test this",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid flow tag value." == response.json()["message"]


def test_Create_or_Update_Flow_Tag_PUT_403(api_client_cognito):
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}/tags/pytest'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 403 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "Forbidden. You do not have permission to modify this flow. It may be marked read-only."
        == response.json()["message"]
    )


def test_Create_or_Update_Flow_Tag_PUT_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/tags/pytest"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested flow does not exist." == response.json()["message"]


def test_Delete_Flow_Tag_DELETE_204(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/tags/pytest'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Delete_Flow_Tag_DELETE_403(api_client_cognito):
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}/tags/test'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 403 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "Forbidden. You do not have permission to modify this flow. It may be marked read-only."
        == response.json()["message"]
    )


def test_Delete_Flow_Tag_DELETE_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/tags/test"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested flow ID in the path is invalid." == response.json()["message"]


def test_Flow_Description_HEAD_200(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/description'
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


def test_Flow_Description_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/description"
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


def test_Flow_Description_GET_200(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/description'
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
    assert MULTI_FLOW["description"] == response.content.decode("utf-8")


def test_Flow_Description_GET_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/description"
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
    assert "The requested flow does not exist." == response.json()["message"]


def test_Create_or_Update_Flow_Description_PUT_204_create(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="pytest - audio",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Description_PUT_204_update(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="pytest",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Description_PUT_400(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        data="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid flow description." == response.json()["message"]


def test_Create_or_Update_Flow_Description_PUT_403(api_client_cognito):
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 403 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "Forbidden. You do not have permission to modify this flow. It may be marked read-only."
        == response.json()["message"]
    )


def test_Create_or_Update_Flow_Description_PUT_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/description"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested flow does not exist." == response.json()["message"]


def test_Delete_Flow_Description_DELETE_204(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Delete_Flow_Description_DELETE_403(api_client_cognito):
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}/description'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 403 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "Forbidden. You do not have permission to modify this flow. It may be marked read-only."
        == response.json()["message"]
    )


def test_Delete_Flow_Description_DELETE_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/description"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested flow ID in the path is invalid." == response.json()["message"]


def test_Flow_Label_HEAD_200(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/label'
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


def test_Flow_Label_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/label"
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


def test_Flow_Label_GET_200(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/label'
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
    assert MULTI_FLOW["label"] == response.content.decode("utf-8")


def test_Flow_Label_GET_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/label"
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
        "The requested Flow does not exist, or does not have a label set."
        == response.json()["message"]
    )


def test_Create_or_Update_Flow_Label_PUT_204_create(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}/label'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="pytest - audio",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Label_PUT_204_update(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/label'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="pytest",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Label_PUT_400(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/label'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        data="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid flow label." == response.json()["message"]


def test_Create_or_Update_Flow_Label_PUT_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/label"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json="test",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested Flow does not exist." == response.json()["message"]


def test_Delete_Flow_Label_DELETE_204(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}/label'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Delete_Flow_Label_DELETE_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/label"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested flow ID in the path is invalid." == response.json()["message"]


def test_Flow_Read_Only_HEAD_200(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/read_only'
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


def test_Flow_Read_Only_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/read_only"
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


def test_Flow_Read_Only_GET_200(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/read_only'
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
    assert str(False).lower() == response.content.decode("utf-8")


def test_Flow_Read_Only_GET_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/read_only"
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
    assert "The requested flow does not exist." == response.json()["message"]


def test_Set_Flow_Read_Only_PUT_204_DATA(api_client_cognito):
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}/read_only'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=False,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Set_Flow_Read_Only_PUT_204_AUDIO(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}/read_only'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=True,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Set_Flow_Read_Only_PUT_400(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/read_only'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        data="invalid",
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid flow description." == response.json()["message"]


def test_Set_Flow_Read_Only_PUT_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/read_only"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=False,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested flow does not exist." == response.json()["message"]
