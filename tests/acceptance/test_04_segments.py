# pylint: disable=too-many-lines
import base64
import json

import pytest
import requests
from conftest import assert_equal_unordered

pytestmark = [
    pytest.mark.acceptance,
]


@pytest.mark.parametrize(
    "path, verb",
    [
        ("/objects", "GET"),
        ("/objects", "HEAD"),
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


def test_Allocate_Flow_Storage_POST_201_default(
    api_client_cognito, media_objects, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/storage'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    media_objects.extend(response.json()["media_objects"])
    # Assert
    assert 201 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "media_objects" in response.json()
    assert 100 == len(response.json()["media_objects"])


def test_Allocate_Flow_Storage_POST_201(
    api_client_cognito, media_objects, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/storage'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"limit": 7},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    media_objects.extend(response.json()["media_objects"])
    # Assert
    assert 201 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "media_objects" in response.json()
    assert 7 == len(response.json()["media_objects"])
    for record in response.json()["media_objects"]:
        assert "object_id" in record
        assert "put_url" in record
        assert "url" in record["put_url"]
        assert "content-type" in record["put_url"]
        assert stub_video_flow["container"] == record["put_url"]["content-type"]


def test_Allocate_Flow_Storage_POST_201_object_ids(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/storage'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"object_ids": ["test-1", "test-2"]},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 201 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "media_objects" in response.json()
    assert 2 == len(response.json()["media_objects"])
    for record in response.json()["media_objects"]:
        assert "object_id" in record
        assert "put_url" in record
        assert "url" in record["put_url"]
        assert "content-type" in record["put_url"]
        assert stub_video_flow["container"] == record["put_url"]["content-type"]


def test_Allocate_Flow_Storage_POST_400_request(api_client_cognito, stub_multi_flow):
    """Bad request body"""
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/storage'
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
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Allocate_Flow_Storage_POST_400_container(api_client_cognito, stub_data_flow):
    """Flow missing container"""
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}/storage'
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
        "Bad request. Invalid flow storage request JSON or the flow 'container' is not set. If object_ids supplied, some or all already exist."
        == response.json()["message"]
    )


def test_Allocate_Flow_Storage_POST_403(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/storage'
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


def test_Allocate_Flow_Storage_POST_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/storage"
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


def test_Presigned_PUT_URL_POST_200(media_objects):
    # Act
    for record in media_objects[:-1]:
        put_file = requests.put(
            record["put_url"]["url"],
            headers={"Content-Type": record["put_url"]["content-type"]},
            data="test file content",
            timeout=30,
        )
        # Assert
        assert 200 == put_file.status_code


def test_Create_Flow_Segment_POST_201_VIDEO_media_objects(
    api_client_cognito, media_objects, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    for n, record in enumerate(media_objects[:-2]):
        response = api_client_cognito.request(
            "POST",
            path,
            json={"object_id": record["object_id"], "timerange": f"[{n}:0_{n + 1}:0)"},
        )
        # Assert
        assert 201 == response.status_code


def test_Create_Flow_Segment_POST_201_MULTI(
    api_client_cognito, media_objects, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
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


def test_Create_Flow_Segment_POST_201_negative(
    api_client_cognito, media_objects, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": media_objects[5]["object_id"],
            "timerange": "[-60:0_-30:0)",
        },
    )
    # Assert
    assert 201 == response.status_code


def test_Create_Flow_Segment_POST_201_list_ok(
    api_client_cognito, media_objects, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json=[
            {
                "object_id": media_objects[6]["object_id"],
                "timerange": "[1:0_2:0)",
            },
            {
                "object_id": media_objects[7]["object_id"],
                "timerange": "[2:0_3:0)",
            },
        ],
    )
    # Assert
    assert 201 == response.status_code


def test_Create_Flow_Segment_POST_200_list_partial(
    api_client_cognito, media_objects, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json=[
            {
                "object_id": media_objects[8]["object_id"],
                "timerange": "[2:0_3:0)",
            },
            {
                "object_id": media_objects[9]["object_id"],
                "timerange": "[3:0_4:0)",
            },
        ],
    )
    response_json = response.json()
    # Assert
    assert 1 == len(response_json)
    assert "BadRequestError" == response_json[0]["error"]["type"]
    assert 200 == response.status_code


def test_Create_Flow_Segment_POST_200_list_failed(
    api_client_cognito, media_objects, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json=[
            {
                "object_id": media_objects[8]["object_id"],
                "timerange": "[2:0_3:0)",
            },
            {
                "object_id": "does-not-exist",
                "timerange": "[4:0_5:0)",
            },
        ],
    )
    response_json = response.json()
    # Assert
    assert 2 == len(response_json)
    assert 200 == response.status_code


def test_Create_Flow_Segment_POST_201_with_get_urls_same_store(
    api_client_cognito, region, stack, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    bucket_name = stack["outputs"]["MediaStorageBucket"]
    object_id = "test-123"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": object_id,
            "timerange": "[4:0_5:0)",
            "get_urls": [
                {
                    "label": f"aws.{region}:s3:Example TAMS",
                    "url": f"https://{bucket_name}.s3.{region}.amazonaws.com/{object_id}",
                }
            ],
        },
    )
    # Assert
    assert 201 == response.status_code


def test_Create_Flow_Segment_POST_201_with_get_urls_external(
    api_client_cognito, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    object_id = "test-456"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": object_id,
            "timerange": "[5:0_6:0)",
            "get_urls": [
                {
                    "label": "something-external",
                    "url": f"https://foo.bar/{object_id}",
                }
            ],
        },
    )
    # Assert
    assert 201 == response.status_code


def test_List_Flow_Segments_GET_200_with_get_urls_same_store(
    api_client_cognito, region, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "object_id": "test-123",
            "accept_get_urls": f"aws.{region}:s3:Example TAMS",
        },
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())


def test_List_Flow_Segments_GET_200_with_get_urls_external(
    api_client_cognito, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "object_id": "test-456",
            "accept_get_urls": "something-external",
        },
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response.json())


def test_Delete_Flow_Segment_DELETE_204_with_get_urls_same_store(
    api_client_cognito, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={
            "object_id": "test-123",
        },
    )
    # Assert
    assert 204 == response.status_code


def test_Delete_Flow_Segment_DELETE_204_with_get_urls_external(
    api_client_cognito, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={
            "object_id": "test-456",
        },
    )
    # Assert
    assert 204 == response.status_code


def test_List_Flow_Segments_HEAD_200(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_HEAD_200_accept_get_urls(
    api_client_cognito, stub_video_flow
):
    """List segments with accept_get_urls query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"accept_get_urls": ""},
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


def test_List_Flow_Segments_HEAD_200_limit(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_HEAD_200_object_id(
    api_client_cognito, media_objects, stub_video_flow
):
    """List segments with object_id query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"object_id": media_objects[5]["object_id"]},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flow_Segments_HEAD_200_page(api_client_cognito, stub_video_flow):
    """List flows with page query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_HEAD_200_reverse_order(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_HEAD_200_timerange(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_HEAD_400(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_HEAD_400_accept_get_urls(
    api_client_cognito, stub_video_flow
):
    """List segments with accept_get_urls query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"accept_get_urls": "", "timerange": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flow_Segments_HEAD_400_limit(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"limit": 2, "timerange": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flow_Segments_HEAD_400_object_id(
    api_client_cognito, media_objects, stub_video_flow
):
    """List segments with object_id query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={
            "object_id": media_objects[5]["object_id"],
            "timerange": "bad",
        },
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flow_Segments_HEAD_400_page(api_client_cognito, stub_video_flow):
    """List flows with page query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"page": "200000000", "timerange": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flow_Segments_HEAD_400_reverse_order(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"reverse_order": "true", "timerange": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flow_Segments_HEAD_400_timerange(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"timerange": "[3:5_4:5)", "limit": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


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


def test_List_Flow_Segments_HEAD_404_accept_get_urls(api_client_cognito):
    """List segments with accept_get_urls query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"accept_get_urls": ""},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_List_Flow_Segments_HEAD_404_object_id(api_client_cognito, media_objects):
    """List segments with object_id query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"object_id": media_objects[5]["object_id"]},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


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


def test_Flow_Details_GET_200_include_timerange(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}'
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


def test_Flow_Details_GET_200_timerange(
    api_client_cognito, dynamic_props, stub_video_flow, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}'
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
    for prop in dynamic_props:
        if prop in response_json:
            del response_json[prop]
    assert_equal_unordered(
        {
            **stub_video_flow,
            "label": "pytest",
            "description": "pytest",
            "collected_by": [stub_multi_flow["id"]],
            "timerange": "[1:0_3:0)",
            "avg_bit_rate": 6000000,
            "max_bit_rate": 6000000,
        },
        response_json,
    )


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
    assert 3 == len(response.json())


def test_List_Flow_Segments_GET_200(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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
        assert 2 == len(record["get_urls"])


def test_List_Flow_Segments_GET_200_non_existant(api_client_cognito, id_404):
    """A request for segments from a non-existent flow will return an empty list, not a 404."""
    # Arrange
    path = f"/flows/{id_404}/segments"
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


def test_List_Flow_Segments_GET_200_accept_get_urls_empty(
    api_client_cognito, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"accept_get_urls": ""},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 30 == len(response.json())
    for record in response.json():
        assert "object_id" in record
        assert "timerange" in record
        assert "get_urls" not in record


def test_List_Flow_Segments_GET_200_accept_get_urls_single(
    api_client_cognito, region, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"accept_get_urls": f"aws.{region}:s3:Example TAMS"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 30 == len(response.json())
    for record in response.json():
        assert "object_id" in record
        assert "timerange" in record
        assert "get_urls" in record
        assert 1 == len(record["get_urls"])


def test_List_Flow_Segments_GET_200_accept_get_urls_multiple(
    api_client_cognito, region, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "accept_get_urls": f"aws.{region}:s3:Example TAMS,aws.{region}:s3.presigned:Example TAMS"
        },
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 30 == len(response.json())
    for record in response.json():
        assert "object_id" in record
        assert "timerange" in record
        assert "get_urls" in record
        assert 2 == len(record["get_urls"])


def test_List_Flow_Segments_GET_200_limit(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_GET_200_object_id(
    api_client_cognito, media_objects, stub_video_flow
):
    """List segments with object_id query specified"""
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_GET_200_page(api_client_cognito, stub_video_flow):
    """List flows with page query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_GET_200_reverse_order(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_GET_200_timerange(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_GET_400(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"timerange": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flow_Segments_GET_400_limit(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"limit": 2, "timerange": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flow_Segments_GET_400_object_id(
    api_client_cognito, media_objects, stub_video_flow
):
    """List segments with object_id query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "object_id": media_objects[5]["object_id"],
            "timerange": "bad",
        },
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flow_Segments_GET_400_page(api_client_cognito, stub_video_flow):
    """List flows with page query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"page": "200000000", "timerange": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flow_Segments_GET_400_reverse_order(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"reverse_order": "true", "timerange": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_List_Flow_Segments_GET_400_timerange(api_client_cognito, stub_video_flow):
    """List segments with limit query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"timerange": "[3:5_4:5)", "limit": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


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


def test_List_Flow_Segments_GET_404_object_id(api_client_cognito, media_objects):
    """List segments with object_id query specified"""
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"object_id": media_objects[5]["object_id"]},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The flow ID in the path is invalid." == response.json()["message"]


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


def test_Create_Flow_Segment_POST_400_request(api_client_cognito, stub_multi_flow):
    """Bad request body"""
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
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
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Create_Flow_Segment_POST_400_container(
    api_client_cognito, media_objects, stub_data_flow
):
    """Flow missing container"""
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": media_objects[5]["object_id"],
            "timerange": "[0:0_1:0)",
        },
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "Bad request. The flow 'container' is not set." == response.json()["message"]


def test_Create_Flow_Segment_POST_400_overlap(
    api_client_cognito, media_objects, stub_multi_flow
):
    """Timerange overlaps with existing segment"""
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": media_objects[5]["object_id"],
            "timerange": "[0:100_1:0)",
        },
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "Bad request. The timerange of the segment MUST NOT overlap any other segment in the same Flow."
        == response.json()["message"]
    )


def test_Create_Flow_Segment_POST_400_incorrect_flow(
    api_client_cognito, media_objects, stub_multi_flow
):
    """Object must already exist in S3"""
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": media_objects[-2]["object_id"],
            "timerange": "[0:100_1:0)",
        },
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "Bad request. The object id is not valid to be used for the flow id supplied."
        == response.json()["message"]
    )


def test_Create_Flow_Segment_POST_403(
    api_client_cognito, media_objects, stub_audio_flow
):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"object_id": media_objects[-1]["object_id"], "timerange": "[0:0_1:0)"},
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


def test_Create_Flow_Segment_POST_404(api_client_cognito, media_objects, id_404):
    # Arrange
    path = f"/flows/{id_404}/segments"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"object_id": media_objects[-1]["object_id"], "timerange": "[0:0_1:0)"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 404 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "The flow does not exist." == response.json()["message"]


def test_Get_Media_Object_Information_HEAD_200(api_client_cognito, media_objects):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Get_Media_Object_Information_HEAD_200_limit(api_client_cognito, media_objects):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"limit": "1"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Get_Media_Object_Information_HEAD_200_page(
    api_client_cognito, media_objects, stub_video_flow
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    page = base64.b64encode(
        json.dumps(
            {
                "flow_id": stub_video_flow["id"],
                "timerange_end": 5999999999,
                "object_id": object_id,
            }
        ).encode("utf-8")
    ).decode("utf-8")
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"limit": "1", "page": page},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Get_Media_Object_Information_HEAD_400(api_client_cognito, media_objects):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"limit": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert "" == response.content.decode("utf-8")


def test_Get_Media_Object_Information_HEAD_404(api_client_cognito):
    # Arrange
    object_id = "does-not-exist"
    path = f"/objects/{object_id}"
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


def test_Get_Media_Object_Information_GET_200(
    api_client_cognito, media_objects, stub_video_flow
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
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
    assert object_id == response_json["id"]
    assert 2 == len(response_json["referenced_by_flows"])
    assert stub_video_flow["id"] == response_json["first_referenced_by_flow"]


def test_Get_Media_Object_Information_GET_200_limit(api_client_cognito, media_objects):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"limit": "1"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response_json["referenced_by_flows"])


def test_Get_Media_Object_Information_GET_200_page(
    api_client_cognito, media_objects, stub_video_flow
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    page = base64.b64encode(
        json.dumps(
            {
                "flow_id": stub_video_flow["id"],
                "timerange_end": 5999999999,
                "object_id": object_id,
            }
        ).encode("utf-8")
    ).decode("utf-8")
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"limit": "1", "page": page},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    response_json = response.json()
    # Assert
    assert 200 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert 1 == len(response_json["referenced_by_flows"])


def test_Get_Media_Object_Information_GET_400(api_client_cognito, media_objects):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"limit": "bad"},
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert isinstance(response.json()["message"], list)
    assert 0 < len(response.json()["message"])


def test_Get_Media_Object_Information_GET_404(api_client_cognito):
    # Arrange
    object_id = "does-not-exist"
    path = f"/objects/{object_id}"
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
    assert "The requested media object does not exist." == response.json()["message"]
