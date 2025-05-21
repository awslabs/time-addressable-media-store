# pylint: disable=too-many-lines
import pytest
import requests

pytestmark = [
    pytest.mark.acceptance,
]


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
        ("/flows/{flowId}/flow_collection", "DELETE"),
        ("/flows/{flowId}/flow_collection", "GET"),
        ("/flows/{flowId}/flow_collection", "HEAD"),
        ("/flows/{flowId}/flow_collection", "PUT"),
        ("/flows/{flowId}/max_bit_rate", "DELETE"),
        ("/flows/{flowId}/max_bit_rate", "GET"),
        ("/flows/{flowId}/max_bit_rate", "HEAD"),
        ("/flows/{flowId}/max_bit_rate", "PUT"),
        ("/flows/{flowId}/avg_bit_rate", "DELETE"),
        ("/flows/{flowId}/avg_bit_rate", "GET"),
        ("/flows/{flowId}/avg_bit_rate", "HEAD"),
        ("/flows/{flowId}/avg_bit_rate", "PUT"),
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


def test_Create_or_Replace_Flow_PUT_201_VIDEO(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=stub_video_flow,
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
    assert stub_video_flow == response_json


def test_Create_or_Replace_Flow_PUT_201_AUDIO(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=stub_audio_flow,
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
    assert stub_audio_flow == response_json


def test_Create_or_Replace_Flow_PUT_201_DATA(api_client_cognito, stub_data_flow):
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=stub_data_flow,
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
    assert stub_data_flow == response_json


def test_Create_or_Replace_Flow_PUT_201_MULTI(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=stub_multi_flow,
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
    assert stub_multi_flow == response_json


def test_Create_or_Replace_Flow_PUT_204(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=stub_multi_flow,
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
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Create_or_Replace_Flow_PUT_403(api_client_cognito, stub_data_flow):
    """Attempt to update a flow that is marked as read-only"""
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=stub_data_flow,
    )
    # Assert
    assert 403 == response.status_code
    assert (
        "Forbidden. You do not have permission to modify this flow. It may be marked read-only."
        == response.json()["message"]
    )


def test_Create_or_Replace_Flow_PUT_404(api_client_cognito, id_404, stub_multi_flow):
    """Flow Id in path does not match Flow Id in body"""
    # Arrange
    path = f"/flows/{id_404}"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=stub_multi_flow,
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
    response = api_client_cognito.request("HEAD", path, params={"page": "1"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_200_source_id(api_client_cognito, stub_data_flow):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"source_id": stub_data_flow["source_id"]}
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
        params={"timerange": "bad"},
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
        "HEAD", path, params={"codec": "audio/aac", "timerange": "bad"}
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
        "HEAD", path, params={"format": "urn:x-nmos:format:data", "timerange": "bad"}
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
        "HEAD", path, params={"frame_height": "1080", "timerange": "bad"}
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
        "HEAD", path, params={"frame_width": "1920", "timerange": "bad"}
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
        "HEAD", path, params={"label": "pytest", "timerange": "bad"}
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
        "HEAD", path, params={"limit": "2", "timerange": "bad"}
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
        "HEAD", path, params={"page": "1", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_HEAD_400_source_id(api_client_cognito, stub_data_flow):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"source_id": stub_data_flow["source_id"], "timerange": "bad"},
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
        "HEAD", path, params={"tag.test": "this", "timerange": "bad"}
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
        "HEAD", path, params={"tag_exists.text": "false", "timerange": "bad"}
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
        "HEAD", path, params={"timerange": "()", "format": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flows_GET_200(
    api_client_cognito,
    dynamic_props,
    stub_video_flow,
    stub_audio_flow,
    stub_data_flow,
    stub_multi_flow,
):
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    for record in response_json:
        if "flow_collection" in record:
            record["flow_collection"] = sorted(
                record["flow_collection"], key=lambda fc: fc["id"]
            )
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 4 == len(response.json())
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert {**stub_video_flow, "collected_by": [stub_multi_flow["id"]]} in response_json
    assert {**stub_audio_flow, "collected_by": [stub_multi_flow["id"]]} in response_json
    assert {**stub_data_flow, "collected_by": [stub_multi_flow["id"]]} in response_json
    assert stub_multi_flow in response_json


def test_List_Flows_GET_200_codec(
    api_client_cognito, dynamic_props, stub_audio_flow, stub_multi_flow
):
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
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [
        {**stub_audio_flow, "collected_by": [stub_multi_flow["id"]]}
    ] == response_json


def test_List_Flows_GET_200_format(
    api_client_cognito, dynamic_props, stub_data_flow, stub_multi_flow
):
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
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [
        {**stub_data_flow, "collected_by": [stub_multi_flow["id"]]}
    ] == response_json


def test_List_Flows_GET_200_frame_height(
    api_client_cognito, dynamic_props, stub_video_flow, stub_multi_flow
):
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
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [
        {**stub_video_flow, "collected_by": [stub_multi_flow["id"]]}
    ] == response_json


def test_List_Flows_GET_200_frame_width(
    api_client_cognito, dynamic_props, stub_video_flow, stub_multi_flow
):
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
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [
        {**stub_video_flow, "collected_by": [stub_multi_flow["id"]]}
    ] == response_json


def test_List_Flows_GET_200_label(api_client_cognito, dynamic_props, stub_multi_flow):
    """List flows with label query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"label": "pytest"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    for record in response_json:
        if "flow_collection" in record:
            record["flow_collection"] = sorted(
                record["flow_collection"], key=lambda fc: fc["id"]
            )
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [stub_multi_flow] == response_json


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
    response = api_client_cognito.request("GET", path, params={"page": "1"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 3 == len(response.json())


def test_List_Flows_GET_200_source_id(
    api_client_cognito, dynamic_props, stub_data_flow, stub_multi_flow
):
    """List flows with source_id query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"source_id": stub_data_flow["source_id"]}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [
        {**stub_data_flow, "collected_by": [stub_multi_flow["id"]]}
    ] == response_json


def test_List_Flows_GET_200_tag_name(
    api_client_cognito, dynamic_props, stub_multi_flow
):
    """List flows with tag.{name} query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"tag.test": "this"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    for record in response_json:
        if "flow_collection" in record:
            record["flow_collection"] = sorted(
                record["flow_collection"], key=lambda fc: fc["id"]
            )
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    for prop in dynamic_props:
        for record in response_json:
            if prop in record:
                del record[prop]
    assert [stub_multi_flow] == response_json


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
    response = api_client_cognito.request("GET", path, params={"timerange": "bad"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flows_GET_400_codec(api_client_cognito):
    """List flows with codec query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"codec": "audio/aac", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flows_GET_400_format(api_client_cognito):
    """List flows with format query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"format": "urn:x-nmos:format:data", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flows_GET_400_frame_height(api_client_cognito):
    """List flows with frame_height query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"frame_height": "1080", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flows_GET_400_frame_width(api_client_cognito):
    """List flows with frame_width query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"frame_width": "1920", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flows_GET_400_label(api_client_cognito):
    """List flows with label query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"label": "pytest", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flows_GET_400_limit(api_client_cognito):
    """List flows with limit query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"limit": "2", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flows_GET_400_page(api_client_cognito):
    """List flows with page query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"page": "1", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flows_GET_400_source_id(api_client_cognito, stub_data_flow):
    """List flows with source_id query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"source_id": stub_data_flow["source_id"], "timerange": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flows_GET_400_tag_name(api_client_cognito):
    """List flows with tag.{name} query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag.test": "this", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flows_GET_400_tag_exists_name(api_client_cognito):
    """List flows with tag_exists.{name} query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"tag_exists.text": "false", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flows_GET_400_timerange(api_client_cognito):
    """List flows with tag_exists.{name} query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"timerange": "()", "format": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Flow_Details_HEAD_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}'
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


def test_Flow_Details_HEAD_200_include_timerange(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}'
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


def test_Flow_Details_HEAD_200_timerange(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}'
    # Act
    response = api_client_cognito.request("HEAD", path, params={"timerange": "()"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Flow_Details_HEAD_400(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}'
    # Act
    response = api_client_cognito.request("HEAD", path, params={"timerange": "bad"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Flow_Details_HEAD_400_include_timerange(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"include_timerange": "true", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Flow_Details_HEAD_400_timerange(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "HEAD", path, params={"include_timerange": "true", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Flow_Details_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}"
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


def test_Flow_Details_HEAD_404_include_timerange(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}"
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


def test_Flow_Details_HEAD_404_timerange(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}"
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


def test_Flow_Details_GET_200(
    api_client_cognito, dynamic_props, stub_data_flow, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}'
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
    for prop in dynamic_props:
        if prop in response_json:
            del response_json[prop]
    assert {**stub_data_flow, "collected_by": [stub_multi_flow["id"]]} == response_json


def test_Flow_Details_GET_400(api_client_cognito, stub_data_flow):
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}'
    # Act
    response = api_client_cognito.request("GET", path, params={"timerange": "bad"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Flow_Details_GET_400_include_timerange(api_client_cognito, stub_data_flow):
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"include_timerange": "true", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Flow_Details_GET_400_timerange(api_client_cognito, stub_data_flow):
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"include_timerange": "true", "timerange": "bad"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Flow_Details_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}"
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


def test_Flow_Details_GET_404_include_timerange(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}"
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


def test_Flow_Details_GET_404_timerange(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}"
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


def test_List_Flow_Tags_HEAD_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/tags'
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


def test_List_Flow_Tags_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/tags"
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


def test_List_Flow_Tags_GET_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/tags'
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
    assert stub_multi_flow["tags"] == response.json()


def test_List_Flow_Tags_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/tags"
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


def test_Flow_Tag_Value_HEAD_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/tags/flow_status'
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


def test_Flow_Tag_Value_HEAD_404_bad_tag(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/tags/does_not_exist'
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


def test_Flow_Tag_Value_HEAD_404_bad_flow_id(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/tags/flow_status"
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


def test_Flow_Tag_Value_GET_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/tags/flow_status'
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
    assert stub_multi_flow["tags"]["flow_status"] == response.content.decode("utf-8")


def test_Flow_Tag_Value_GET_404_bad_tag(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/tags/does_not_exist'
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


def test_Flow_Tag_Value_GET_404_bad_flow_id(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/tags/flow_status"
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


def test_Create_or_Update_Flow_Tag_PUT_204_create(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/tags/pytest'
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


def test_Create_or_Update_Flow_Tag_PUT_204_update(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/tags/test'
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


def test_Create_or_Update_Flow_Tag_PUT_400(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/tags/pytest'
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
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Create_or_Update_Flow_Tag_PUT_403(api_client_cognito, stub_data_flow):
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}/tags/pytest'
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


def test_Create_or_Update_Flow_Tag_PUT_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/tags/pytest"
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


def test_Delete_Flow_Tag_DELETE_204(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/tags/pytest'
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


def test_Delete_Flow_Tag_DELETE_403(api_client_cognito, stub_data_flow):
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}/tags/test'
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


def test_Delete_Flow_Tag_DELETE_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/tags/test"
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


def test_Flow_Description_HEAD_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/description'
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


def test_Flow_Description_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/description"
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


def test_Flow_Description_GET_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/description'
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
    assert stub_multi_flow["description"] == response.content.decode("utf-8")


def test_Flow_Description_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/description"
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


def test_Create_or_Update_Flow_Description_PUT_204_create(
    api_client_cognito, stub_audio_flow
):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/description'
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


def test_Create_or_Update_Flow_Description_PUT_204_update(
    api_client_cognito, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/description'
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


def test_Create_or_Update_Flow_Description_PUT_400(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/description'
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
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Create_or_Update_Flow_Description_PUT_403(api_client_cognito, stub_data_flow):
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}/description'
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


def test_Create_or_Update_Flow_Description_PUT_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/description"
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


def test_Delete_Flow_Description_DELETE_204(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/description'
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


def test_Delete_Flow_Description_DELETE_403(api_client_cognito, stub_data_flow):
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}/description'
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


def test_Delete_Flow_Description_DELETE_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/description"
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


def test_Flow_Label_HEAD_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/label'
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


def test_Flow_Label_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/label"
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


def test_Flow_Label_GET_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/label'
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
    assert stub_multi_flow["label"] == response.content.decode("utf-8")


def test_Flow_Label_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/label"
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


def test_Create_or_Update_Flow_Label_PUT_204_create(
    api_client_cognito, stub_audio_flow
):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/label'
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


def test_Create_or_Update_Flow_Label_PUT_204_update(
    api_client_cognito, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/label'
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


def test_Create_or_Update_Flow_Label_PUT_400(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/label'
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
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Create_or_Update_Flow_Label_PUT_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/label"
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


def test_Delete_Flow_Label_DELETE_204(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/label'
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


def test_Delete_Flow_Label_DELETE_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/label"
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


def test_Flow_Flow_Collection_HEAD_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/flow_collection'
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


def test_Flow_Flow_Collection_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/flow_collection"
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


def test_Flow_Flow_Collection_GET_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/flow_collection'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    response_json = sorted(response_json, key=lambda fc: fc["id"])
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert stub_multi_flow["flow_collection"] == response_json


def test_Flow_Flow_Collection_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/flow_collection"
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
    assert "The requested Flow does not exist." == response.json()["message"]


def test_Delete_Flow_Flow_Collection_DELETE_204(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/flow_collection'
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


def test_Delete_Flow_Flow_Collection_DELETE_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/flow_collection"
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


def test_Create_or_Update_Flow_Flow_Collection_PUT_204_create(
    api_client_cognito, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/flow_collection'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=stub_multi_flow["flow_collection"][0:1],
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Flow_Collection_PUT_204_update(
    api_client_cognito, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/flow_collection'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=stub_multi_flow["flow_collection"],
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Flow_Collection_PUT_400(
    api_client_cognito, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/flow_collection'
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
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Create_or_Update_Flow_Flow_Collection_PUT_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/flow_collection"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=[],
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested Flow does not exist." == response.json()["message"]


def test_Flow_Max_Bit_Rate_HEAD_200(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/max_bit_rate'
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


def test_Flow_Max_Bit_Rate_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/max_bit_rate"
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


def test_Flow_Max_Bit_Rate_GET_200(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/max_bit_rate'
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
    assert stub_video_flow["max_bit_rate"] == int(response.content.decode("utf-8"))


def test_Flow_Max_Bit_Rate_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/max_bit_rate"
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
    assert "The requested Flow does not exist." == response.json()["message"]


def test_Create_or_Update_Flow_Max_Bit_Rate_PUT_204_create(
    api_client_cognito, stub_audio_flow
):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/max_bit_rate'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=6000000,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Max_Bit_Rate_PUT_204_update(
    api_client_cognito, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/max_bit_rate'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=6000000,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Max_Bit_Rate_PUT_400(
    api_client_cognito, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/max_bit_rate'
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
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Create_or_Update_Flow_Max_Bit_Rate_PUT_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/max_bit_rate"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=6000000,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested Flow does not exist." == response.json()["message"]


def test_Delete_Flow_Max_Bit_Rate_DELETE_204(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/max_bit_rate'
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


def test_Delete_Flow_Max_Bit_Rate_DELETE_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/max_bit_rate"
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


def test_Flow_Avg_Bit_Rate_HEAD_200(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/avg_bit_rate'
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


def test_Flow_Avg_Bit_Rate_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/avg_bit_rate"
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


def test_Flow_Avg_Bit_Rate_GET_200(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/avg_bit_rate'
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
    assert stub_video_flow["avg_bit_rate"] == int(response.content.decode("utf-8"))


def test_Flow_Avg_Bit_Rate_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/avg_bit_rate"
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
    assert "The requested Flow does not exist." == response.json()["message"]


def test_Create_or_Update_Flow_Avg_Bit_Rate_PUT_204_create(
    api_client_cognito, stub_audio_flow
):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/avg_bit_rate'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=6000000,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Avg_Bit_Rate_PUT_204_update(
    api_client_cognito, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/avg_bit_rate'
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=6000000,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Create_or_Update_Flow_Avg_Bit_Rate_PUT_400(
    api_client_cognito, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/avg_bit_rate'
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
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Create_or_Update_Flow_Avg_Bit_Rate_PUT_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/avg_bit_rate"
    # Act
    response = api_client_cognito.request(
        "PUT",
        path,
        json=6000000,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested Flow does not exist." == response.json()["message"]


def test_Delete_Flow_Avg_Bit_Rate_DELETE_204(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/avg_bit_rate'
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


def test_Delete_Flow_Avg_Bit_Rate_DELETE_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/avg_bit_rate"
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


def test_Flow_Read_Only_HEAD_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/read_only'
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


def test_Flow_Read_Only_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/read_only"
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


def test_Flow_Read_Only_GET_200(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/read_only'
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


def test_Flow_Read_Only_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/read_only"
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


def test_Set_Flow_Read_Only_PUT_204_DATA(api_client_cognito, stub_data_flow):
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}/read_only'
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


def test_Set_Flow_Read_Only_PUT_204_AUDIO(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/read_only'
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


def test_Set_Flow_Read_Only_PUT_400(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/read_only'
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
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Set_Flow_Read_Only_PUT_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/read_only"
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
