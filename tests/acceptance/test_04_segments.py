# pylint: disable=too-many-lines
import pytest
import requests

# pylint: disable=no-name-in-module
from constants import (
    AUDIO_FLOW,
    DATA_FLOW,
    DYNAMIC_PROPS,
    ID_404,
    MULTI_FLOW,
    VIDEO_FLOW,
)


@pytest.mark.storage
def test_Allocate_Flow_Storage_POST_201_default(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/storage'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 201 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "media_objects" in response.json()
    assert 100 == len(response.json()["media_objects"])


@pytest.mark.storage
def test_Allocate_Flow_Storage_POST_201(api_client_cognito, media_objects):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/storage'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"limit": 5},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    media_objects.extend(response.json()["media_objects"])
    # Assert
    assert 201 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "media_objects" in response.json()
    assert 5 == len(response.json()["media_objects"])
    for record in response.json()["media_objects"]:
        assert "object_id" in record
        assert "put_url" in record
        assert "url" in record["put_url"]
        assert "content-type" in record["put_url"]
        assert VIDEO_FLOW["container"] == record["put_url"]["content-type"]


@pytest.mark.storage
def test_Allocate_Flow_Storage_POST_400_request(api_client_cognito):
    """Bad request body"""
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/storage'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Invalid request body" == response.json()["message"]


@pytest.mark.storage
def test_Allocate_Flow_Storage_POST_400_container(api_client_cognito):
    """Flow missing container"""
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}/storage'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"limit": 5},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "Bad request. Invalid flow storage request JSON or the flow 'container' is not set."
        == response.json()["message"]
    )


@pytest.mark.storage
def test_Allocate_Flow_Storage_POST_403(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}/storage'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"limit": 5},
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


@pytest.mark.storage
def test_Allocate_Flow_Storage_POST_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/storage"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"limit": 5},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested flow does not exist." == response.json()["message"]


@pytest.mark.storage
def test_Presigned_PUT_URL_POST_200(media_objects):
    # Act
    for record in media_objects:
        put_file = requests.put(
            record["put_url"]["url"],
            headers={"Content-Type": record["put_url"]["content-type"]},
            data="test file content",
            timeout=30,
        )
        # Assert
        assert 200 == put_file.status_code


@pytest.mark.segments
def test_Create_Flow_Segment_POST_201_VIDEO(api_client_cognito, media_objects):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    for n, record in enumerate(media_objects):
        response = api_client_cognito.request(
            "POST",
            path,
            json={"object_id": record["object_id"], "timerange": f"[{n}:0_{n + 1}:0)"},
        )
        # Assert
        assert 201 == response.status_code
    # Bulk load 100 records to ensure pagination is testable
    # Act
    for n in range(5, 100):
        response = api_client_cognito.request(
            "POST",
            path,
            json={
                "object_id": f"20000000-0000-1000-8000-0000000{n:05}",
                "timerange": f"[{n}:0_{n + 1}:0)",
            },
        )
        # Assert
        assert 201 == response.status_code


@pytest.mark.segments
def test_Create_Flow_Segment_POST_201_MULTI(api_client_cognito, media_objects):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": media_objects[0]["object_id"],
            "timerange": "[0:0_1:0)",
        },
    )
    # Assert
    assert 201 == response.status_code


@pytest.mark.segments
def test_Create_Flow_Segment_POST_201_negative(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": "negative-timerange",
            "timerange": "[-60:0_-30:0)",
        },
    )
    # Assert
    assert 201 == response.status_code


@pytest.mark.flows
def test_Flow_Details_GET_200_include_timerange(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"include_timerange": "true"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "timerange" in response.json()
    assert "segments_updated" in response.json()


@pytest.mark.flows
def test_Flow_Details_GET_200_timerange(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"include_timerange": "true", "timerange": "[1:0_3:0)"}
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "segments_updated" in response.json()
    for prop in DYNAMIC_PROPS:
        if prop in response_json:
            del response_json[prop]
    assert {
        **VIDEO_FLOW,
        "label": "pytest",
        "description": "pytest",
        "collected_by": [MULTI_FLOW["id"]],
        "timerange": "[1:0_3:0)",
    } == response_json


@pytest.mark.flows
def test_List_Flows_GET_200_timerange_eternity(api_client_cognito):
    """List flows with timerange query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"timerange": "_"})
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 2 == len(response.json())


@pytest.mark.flows
def test_List_Flows_GET_200_timerange_never(api_client_cognito):
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
    assert 2 == len(response.json())


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_200(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
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


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_200_limit(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"limit": 2},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "link" in response_headers_lower
    assert "x-paging-count" in response_headers_lower
    assert "x-paging-nextkey" in response_headers_lower
    assert "x-paging-reverse-order" in response_headers_lower
    assert "x-paging-timerange" in response_headers_lower
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_200_object_id(api_client_cognito):
    """List segments with object_id query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"object_id": "20000000-0000-1000-8000-000000000005"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_200_page(api_client_cognito):
    """List flows with page query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"page": "200000000"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_200_reverse_order(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"reverse_order": "true"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_200_timerange(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"timerange": "[3:5_4:5)"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_400(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
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


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_400_limit(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"limit": 2, "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_400_object_id(api_client_cognito):
    """List segments with object_id query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"object_id": "20000000-0000-1000-8000-000000000005", "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_400_page(api_client_cognito):
    """List flows with page query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"page": "200000000", "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_400_reverse_order(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"reverse_order": "true", "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_400_timerange(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"timerange": "[3:5_4:5)", "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_404(api_client_cognito):
    # Arrange
    path = "/flows/invalid-id/segments"
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


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_404_limit(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"limit": 2},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_404_object_id(api_client_cognito):
    """List segments with object_id query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"object_id": "20000000-0000-1000-8000-000000000005"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_404_page(api_client_cognito):
    """List flows with page query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"page": "200000000"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_404_reverse_order(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"reverse_order": "true"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_HEAD_404_timerange(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"timerange": "[3:5_4:5)"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_List_Flow_Segments_GET_200(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    start_first = float(
        response.json()[0]["timerange"].split("_")[0][1:].replace(":", ".")
    )
    start_last = float(
        response.json()[-1]["timerange"].split("_")[0][1:].replace(":", ".")
    )
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 30 == len(response.json())
    assert start_first < start_last
    for record in response.json():
        assert "object_id" in record
        assert "timerange" in record
        assert "get_urls" in record
        assert 1 == len(record["get_urls"])


@pytest.mark.segments
def test_List_Flow_Segments_GET_200_non_existant(api_client_cognito):
    """A request for segments from a non-existent flow will return an empty list, not a 404."""
    # Arrange
    path = f"/flows/{ID_404}/segments"
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
    assert 0 == len(response.json())


@pytest.mark.segments
def test_List_Flow_Segments_GET_200_limit(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"limit": 2},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "link" in response_headers_lower
    assert "x-paging-count" in response_headers_lower
    assert "x-paging-nextkey" in response_headers_lower
    assert "x-paging-reverse-order" in response_headers_lower
    assert "x-paging-timerange" in response_headers_lower
    assert 2 == len(response.json())


@pytest.mark.segments
def test_List_Flow_Segments_GET_200_object_id(api_client_cognito):
    """List segments with object_id query specified"""
    # Arrange
    object_id = "20000000-0000-1000-8000-000000000005"
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"object_id": object_id},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())
    assert object_id == response.json()[0]["object_id"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_200_page(api_client_cognito):
    """List flows with page query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"page": "200000000"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 30 == len(response.json())


@pytest.mark.segments
def test_List_Flow_Segments_GET_200_reverse_order(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"reverse_order": "true"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    start_first = float(
        response.json()[0]["timerange"].split("_")[0][1:].replace(":", ".")
    )
    start_last = float(
        response.json()[-1]["timerange"].split("_")[0][1:].replace(":", ".")
    )
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 30 == len(response.json())
    assert start_first > start_last


@pytest.mark.segments
def test_List_Flow_Segments_GET_200_timerange(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"timerange": "[3:5_4:5)"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 2 == len(response.json())


@pytest.mark.segments
def test_List_Flow_Segments_GET_400(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_400_limit(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"limit": 2, "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_400_object_id(api_client_cognito):
    """List segments with object_id query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"object_id": "20000000-0000-1000-8000-000000000005", "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_400_page(api_client_cognito):
    """List flows with page query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"page": "200000000", "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_400_reverse_order(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"reverse_order": "true", "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_400_timerange(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"timerange": "[3:5_4:5)", "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_404(api_client_cognito):
    # Arrange
    path = "/flows/invalid-id/segments"
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
    assert "The flow ID in the path is invalid." == response.json()["message"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_404_limit(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"limit": 2},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The flow ID in the path is invalid." == response.json()["message"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_404_object_id(api_client_cognito):
    """List segments with object_id query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"object_id": "20000000-0000-1000-8000-000000000005"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The flow ID in the path is invalid." == response.json()["message"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_404_page(api_client_cognito):
    """List flows with page query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"page": "200000000"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The flow ID in the path is invalid." == response.json()["message"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_404_reverse_order(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"reverse_order": "true"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The flow ID in the path is invalid." == response.json()["message"]


@pytest.mark.segments
def test_List_Flow_Segments_GET_404_timerange(api_client_cognito):
    """List segments with limit query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"timerange": "[3:5_4:5)"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The flow ID in the path is invalid." == response.json()["message"]


@pytest.mark.segments
def test_Create_Flow_Segment_POST_400_request(api_client_cognito):
    """Bad request body"""
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"bad": "body"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Invalid request body" == response.json()["message"]


@pytest.mark.segments
def test_Create_Flow_Segment_POST_400_container(api_client_cognito):
    """Flow missing container"""
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"object_id": "dummy_id", "timerange": "[0:0_1:0)"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "Bad request. Invalid flow storage request JSON or the flow 'container' is not set."
        == response.json()["message"]
    )


@pytest.mark.segments
def test_Create_Flow_Segment_POST_400_overlap(api_client_cognito):
    """Timerange overlaps with existing segment"""
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": "dummy",
            "timerange": "[0:100_1:0)",
        },
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "Bad request. Invalid flow storage request JSON or the flow 'container' is not set."
        == response.json()["message"]
    )


@pytest.mark.segments
def test_Create_Flow_Segment_POST_403(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"object_id": "dummy_id", "timerange": "[0:0_1:0)"},
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


@pytest.mark.segments
def test_Create_Flow_Segment_POST_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/segments"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"object_id": "dummy_id", "timerange": "[0:0_1:0)"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The flow does not exist." == response.json()["message"]


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_202(
    api_client_cognito, delete_requests, api_endpoint
):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    delete_requests.append(response.json()["id"])
    # Assert
    assert 202 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "location" in response_headers_lower
    assert (
        f'{api_endpoint}/flow-delete-requests/{response.json()["id"]}'
        == response_headers_lower["location"]
    )
    assert "id" in response.json()
    assert "flow_id" in response.json()
    assert "timerange_to_delete" in response.json()
    assert "timerange_remaining" in response.json()
    assert "delete_flow" in response.json()
    assert "created" in response.json()
    assert "created_by" in response.json()
    assert "updated" in response.json()
    assert "status" in response.json()


@pytest.mark.skip("Delete requests always returned unless object_id used")
def test_Delete_Flow_Segment_DELETE_204():
    assert True


@pytest.mark.skip("Delete requests always returned unless object_id used")
def test_Delete_Flow_Segment_DELETE_202_object_id():
    assert True


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_204_object_id(api_client_cognito):
    """List segments with object_id query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={"object_id": "20000000-0000-1000-8000-000000000005"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 204 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_202_timerange(
    api_client_cognito, delete_requests, api_endpoint
):
    """Delete segments with timerange query specified"""
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={"timerange": "[3:5_4:5)"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    delete_requests.append(response.json()["id"])
    # Assert
    assert 202 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "location" in response_headers_lower
    assert (
        f'{api_endpoint}/flow-delete-requests/{response.json()["id"]}'
        == response_headers_lower["location"]
    )
    assert "id" in response.json()
    assert "flow_id" in response.json()
    assert "timerange_to_delete" in response.json()
    assert "timerange_remaining" in response.json()
    assert "delete_flow" in response.json()
    assert "created" in response.json()
    assert "created_by" in response.json()
    assert "updated" in response.json()
    assert "status" in response.json()


@pytest.mark.skip("Delete requests always returned unless object_id used")
def test_Delete_Flow_Segment_DELETE_204_timerange():
    assert True


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_400(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={"bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_400_object_id(api_client_cognito):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={"object_id": "20000000-0000-1000-8000-000000000005", "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_400_timerange(api_client_cognito):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={"timerange": "[3:5_4:5)", "bad": "query"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. Invalid query options." == response.json()["message"]


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_403(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}/segments'
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


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_403_object_id(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={"object_id": "20000000-0000-1000-8000-000000000005"},
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


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_403_timerange(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={"timerange": "[3:5_4:5)"},
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


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/segments"
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


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_404_object_id(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/segments"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={"object_id": "20000000-0000-1000-8000-000000000005"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested flow ID in the path is invalid." == response.json()["message"]


@pytest.mark.segments
def test_Delete_Flow_Segment_DELETE_404_timerange(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/segments"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={"timerange": "[3:5_4:5)"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The requested flow ID in the path is invalid." == response.json()["message"]


@pytest.mark.flows
def test_Delete_Flow_DELETE_202_VIDEO(
    api_client_cognito, delete_requests, api_endpoint
):
    # Arrange
    path = f'/flows/{VIDEO_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    delete_requests.append(response.json()["id"])
    # Assert
    assert 202 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "location" in response_headers_lower
    assert (
        f'{api_endpoint}/flow-delete-requests/{response.json()["id"]}'
        == response_headers_lower["location"]
    )
    assert "id" in response.json()
    assert "flow_id" in response.json()
    assert "timerange_to_delete" in response.json()
    assert "timerange_remaining" in response.json()
    assert "delete_flow" in response.json()
    assert "created" in response.json()
    assert "created_by" in response.json()
    assert "updated" in response.json()
    assert "status" in response.json()


@pytest.mark.flows
def test_Delete_Flow_DELETE_403(api_client_cognito):
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}'
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


@pytest.mark.flows
def test_Delete_Flow_DELETE_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}"
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
    assert "The requested Flow ID in the path is invalid." == response.json()["message"]


@pytest.mark.flows
def test_Delete_Flow_DELETE_204_AUDIO(api_client_cognito):
    """204 returned as AUDIO_FLOW has no segments"""
    # Need to set read_only to false prior to delete request
    api_client_cognito.request(
        "PUT",
        f'/flows/{AUDIO_FLOW["id"]}/read_only',
        json=False,
    )
    # Arrange
    path = f'/flows/{AUDIO_FLOW["id"]}'
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


@pytest.mark.flows
def test_Delete_Flow_DELETE_204_DATA(api_client_cognito):
    """204 returned as AUDIO_FLOW has no segments"""
    # Arrange
    path = f'/flows/{DATA_FLOW["id"]}'
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


@pytest.mark.flows
def test_Delete_Flow_DELETE_204_MULTI(api_client_cognito, delete_requests):
    # Arrange
    path = f'/flows/{MULTI_FLOW["id"]}'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    if response.status_code == 202:
        delete_requests.append(response.json()["id"])
    # Assert
    assert response.status_code in [202, 204]
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
