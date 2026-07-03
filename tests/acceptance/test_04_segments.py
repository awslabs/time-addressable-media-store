# pylint: disable=too-many-lines
import base64
import json

import pytest
import requests
from conftest import (
    ID_404,
    REGION,
    assert_equal_unordered,
    assert_headers_present,
    assert_json_response,
    create_storage_label,
    default_get_urls,
    remove_dynamic_props,
)

pytestmark = [
    pytest.mark.acceptance,
]


@pytest.mark.parametrize(
    "path, verb",
    [
        ("/objects/{objectId}", "GET"),
        ("/objects/{objectId}", "HEAD"),
        ("/objects/{objectId}/instances", "POST"),
        ("/objects/{objectId}/instances", "DELETE"),
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
    # Assert
    assert_json_response(response, 201)
    response_json = response.json()
    media_objects.extend(response_json["media_objects"])
    assert "media_objects" in response_json
    assert 100 == len(response_json["media_objects"])


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
    # Assert
    assert_json_response(response, 201)
    response_json = response.json()
    media_objects.extend(response_json["media_objects"])
    assert "media_objects" in response_json
    assert 7 == len(response_json["media_objects"])
    for record in response_json["media_objects"]:
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
    # Assert
    assert_json_response(response, 201)
    response_json = response.json()
    assert "media_objects" in response_json
    assert 2 == len(response_json["media_objects"])
    for record in response_json["media_objects"]:
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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert (
        "Bad request. Invalid flow storage request JSON or the flow 'container' is not set. If object_ids supplied, some or all already exist."
        == response_json["message"]
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
    # Assert
    assert_json_response(response, 403)
    response_json = response.json()
    assert (
        "Forbidden. You do not have permission to modify this flow. It may be marked read-only."
        == response_json["message"]
    )


def test_Allocate_Flow_Storage_POST_404(api_client_cognito):
    # Arrange
    path = f"/flows/{ID_404}/storage"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"limit": 5},
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The requested flow does not exist." == response_json["message"]


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
    api_client_cognito, media_objects, stub_video_flow, expect_webhooks
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
        assert_json_response(response, 201)
    expect_webhooks(
        *[
            expectation
            for n, record in enumerate(media_objects[:-2])
            for expectation in [
                (
                    {
                        "event_type": "flows/updated",
                        "event": {
                            "flow": stub_video_flow,
                        },
                    },
                    ["event.flow.segments_updated"],
                ),
                (
                    {
                        "event_type": "flows/segments_added",
                        "event": {
                            "flow_id": stub_video_flow["id"],
                            "segments": [
                                {
                                    "object_id": record["object_id"],
                                    "timerange": f"[{n}:0_{n + 1}:0)",
                                    "get_urls": default_get_urls(),
                                }
                            ],
                        },
                    },
                    ["event.segments[].get_urls[].url"],
                ),
            ]
        ]
    )


def test_Create_Flow_Segment_POST_201_MULTI(
    api_client_cognito, media_objects, stub_multi_flow, expect_webhooks
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
    assert_json_response(response, 201)
    expect_webhooks(
        (
            {
                "event_type": "flows/updated",
                "event": {
                    "flow": stub_multi_flow,
                },
            },
            ["event.flow.segments_updated"],
        ),
        (
            {
                "event_type": "flows/segments_added",
                "event": {
                    "flow_id": stub_multi_flow["id"],
                    "segments": [
                        {
                            "object_id": media_objects[0]["object_id"],
                            "timerange": "[0:0_1:0)",
                            "get_urls": default_get_urls(),
                        }
                    ],
                },
            },
            ["event.segments[].get_urls[].url"],
        ),
    )


def test_Create_Flow_Segment_POST_201_negative(
    api_client_cognito, media_objects, stub_multi_flow, expect_webhooks
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
    assert_json_response(response, 201)
    expect_webhooks(
        (
            {
                "event_type": "flows/updated",
                "event": {
                    "flow": stub_multi_flow,
                },
            },
            ["event.flow.segments_updated"],
        ),
        (
            {
                "event_type": "flows/segments_added",
                "event": {
                    "flow_id": stub_multi_flow["id"],
                    "segments": [
                        {
                            "object_id": media_objects[5]["object_id"],
                            "timerange": "[-60:0_-30:0)",
                            "get_urls": default_get_urls(),
                        }
                    ],
                },
            },
            ["event.segments[].get_urls[].url"],
        ),
    )


def test_Create_Flow_Segment_POST_201_list_ok(
    api_client_cognito, media_objects, stub_multi_flow, expect_webhooks
):
    # Arrange
    segments = [
        (media_objects[6]["object_id"], "[1:0_2:0)"),
        (media_objects[7]["object_id"], "[2:0_3:0)"),
    ]
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json=[
            {"object_id": object_id, "timerange": timerange}
            for object_id, timerange in segments
        ],
    )
    # Assert
    assert_json_response(response, 201)
    expect_webhooks(
        *[
            expectation
            for object_id, timerange in segments
            for expectation in [
                (
                    {
                        "event_type": "flows/updated",
                        "event": {
                            "flow": stub_multi_flow,
                        },
                    },
                    ["event.flow.segments_updated"],
                ),
                (
                    {
                        "event_type": "flows/segments_added",
                        "event": {
                            "flow_id": stub_multi_flow["id"],
                            "segments": [
                                {
                                    "object_id": object_id,
                                    "timerange": timerange,
                                    "get_urls": default_get_urls(),
                                }
                            ],
                        },
                    },
                    ["event.segments[].get_urls[].url"],
                ),
            ]
        ]
    )


def test_Create_Flow_Segment_POST_200_list_partial(
    api_client_cognito, media_objects, stub_multi_flow, expect_webhooks
):
    # Arrange
    segments = [
        (media_objects[8]["object_id"], "[2:0_3:0)"),
        (media_objects[9]["object_id"], "[3:0_4:0)"),
    ]
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json=[
            {"object_id": object_id, "timerange": timerange}
            for object_id, timerange in segments
        ],
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 1 == len(response_json)
    assert "BadRequestError" == response_json[0]["error"]["type"]
    expect_webhooks(
        (
            {
                "event_type": "flows/updated",
                "event": {
                    "flow": stub_multi_flow,
                },
            },
            ["event.flow.segments_updated"],
        ),
        (
            {
                "event_type": "flows/segments_added",
                "event": {
                    "flow_id": stub_multi_flow["id"],
                    "segments": [
                        {
                            "object_id": segments[1][0],
                            "timerange": segments[1][1],
                            "get_urls": default_get_urls(),
                        }
                    ],
                },
            },
            ["event.segments[].get_urls[].url"],
        ),
    )


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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 2 == len(response_json)


def test_Create_Flow_Segment_POST_201_with_get_urls_same_store(
    api_client_cognito, stack, stub_multi_flow, expect_webhooks
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    bucket_name = stack["outputs"]["MediaStorageBucket"]
    object_id = "test-123"
    segment = {
        "object_id": object_id,
        "timerange": "[4:0_5:0)",
        "get_urls": [
            {
                "label": create_storage_label(),
                "url": f"https://{bucket_name}.s3.{REGION}.amazonaws.com/{object_id}",
            }
        ],
    }
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json=segment,
    )
    # Assert
    assert_json_response(response, 201)
    expect_webhooks(
        (
            {
                "event_type": "flows/updated",
                "event": {
                    "flow": stub_multi_flow,
                },
            },
            ["event.flow.segments_updated"],
        ),
        {
            "event_type": "flows/segments_added",
            "event": {
                "flow_id": stub_multi_flow["id"],
                "segments": [segment],
            },
        },
    )


def test_Create_Flow_Segment_POST_201_with_get_urls_external(
    api_client_cognito, stub_multi_flow, expect_webhooks
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    object_id = "test-456"
    segment = {
        "object_id": object_id,
        "timerange": "[5:0_6:0)",
        "get_urls": [
            {
                "label": "something-external",
                "url": f"https://foo.bar/{object_id}",
            }
        ],
    }
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json=segment,
    )
    # Assert
    assert_json_response(response, 201)
    expect_webhooks(
        (
            {
                "event_type": "flows/updated",
                "event": {
                    "flow": stub_multi_flow,
                },
            },
            ["event.flow.segments_updated"],
        ),
        {
            "event_type": "flows/segments_added",
            "event": {
                "flow_id": stub_multi_flow["id"],
                "segments": [segment],
            },
        },
    )


def test_List_Flow_Segments_GET_200_with_get_urls_same_store(
    api_client_cognito, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "object_id": "test-123",
            "accept_get_urls": create_storage_label(),
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 1 == len(response_json)


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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 1 == len(response_json)


def test_Delete_Flow_Segment_DELETE_204_with_get_urls_same_store(
    api_client_cognito, stub_multi_flow, expect_webhooks
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
    assert_json_response(response, 204, empty_body=True)
    expect_webhooks(
        {
            "event_type": "flows/segments_deleted",
            "event": {"flow_id": stub_multi_flow["id"], "timerange": "[4:0_5:0)"},
        },
        (
            {
                "event_type": "flows/updated",
                "event": {
                    "flow": stub_multi_flow,
                },
            },
            ["event.flow.segments_updated"],
        ),
    )


def test_Delete_Flow_Segment_DELETE_204_with_get_urls_external(
    api_client_cognito, stub_multi_flow, expect_webhooks
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
    assert_json_response(response, 204, empty_body=True)
    expect_webhooks(
        {
            "event_type": "flows/segments_deleted",
            "event": {"flow_id": stub_multi_flow["id"], "timerange": "[5:0_6:0)"},
        },
        (
            {
                "event_type": "flows/updated",
                "event": {
                    "flow": stub_multi_flow,
                },
            },
            ["event.flow.segments_updated"],
        ),
    )


def test_List_Flow_Segments_HEAD_200(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


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
    # Assert
    assert_json_response(response, 200, empty_body=True)
    assert_headers_present(
        response,
        "link",
        "x-paging-count",
        "x-paging-nextkey",
        "x-paging-reverse-order",
        "x-paging-timerange",
    )


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
    # Assert
    assert_json_response(response, 200, empty_body=True)
    assert_headers_present(
        response,
        "link",
        "x-paging-count",
        "x-paging-nextkey",
        "x-paging-reverse-order",
        "x-paging-timerange",
    )


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
    # Assert
    assert_json_response(response, 200, empty_body=True)


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
    # Assert
    assert_json_response(response, 200, empty_body=True)


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
    # Assert
    assert_json_response(response, 200, empty_body=True)


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
    # Assert
    assert_json_response(response, 200, empty_body=True)


def test_List_Flow_Segments_HEAD_400(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={"timerange": "bad"},
    )
    # Assert
    assert_json_response(response, 400, empty_body=True)


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
    # Assert
    assert_json_response(response, 400, empty_body=True)


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
    # Assert
    assert_json_response(response, 400, empty_body=True)


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
    # Assert
    assert_json_response(response, 400, empty_body=True)


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
    # Assert
    assert_json_response(response, 400, empty_body=True)


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
    # Assert
    assert_json_response(response, 400, empty_body=True)


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
    # Assert
    assert_json_response(response, 400, empty_body=True)


def test_List_Flow_Segments_HEAD_404(api_client_cognito):
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 404, empty_body=True)


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
    # Assert
    assert_json_response(response, 404, empty_body=True)


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
    # Assert
    assert_json_response(response, 404, empty_body=True)


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
    # Assert
    assert_json_response(response, 404, empty_body=True)


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
    # Assert
    assert_json_response(response, 404, empty_body=True)


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
    # Assert
    assert_json_response(response, 404, empty_body=True)


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
    # Assert
    assert_json_response(response, 404, empty_body=True)


def test_Flow_Details_GET_200_include_timerange(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"include_timerange": "true"}
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "timerange" in response_json
    assert "segments_updated" in response_json


def test_Flow_Details_GET_200_timerange(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}'
    # Act
    response = api_client_cognito.request(
        "GET", path, params={"include_timerange": "true", "timerange": "[1:0_3:0)"}
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "segments_updated" in response_json
    response_json = remove_dynamic_props(response_json)
    assert_equal_unordered(
        {
            **stub_video_flow,
            "label": "pytest",
            "description": "pytest",
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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 2 == len(response_json)


def test_List_Flows_GET_200_timerange_never(api_client_cognito):
    """List flows with timerange query specified"""
    # Arrange
    path = "/flows"
    # Act
    response = api_client_cognito.request("GET", path, params={"timerange": "()"})
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 3 == len(response_json)


def test_List_Flow_Segments_GET_200(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    start_first = float(
        response_json[0]["timerange"].split("_")[0][1:].replace(":", ".")
    )
    start_last = float(
        response_json[-1]["timerange"].split("_")[0][1:].replace(":", ".")
    )
    assert 30 == len(response_json)
    assert start_first < start_last
    for record in response_json:
        assert "object_id" in record
        assert "timerange" in record
        assert "get_urls" in record
        assert 2 == len(record["get_urls"])


def test_List_Flow_Segments_GET_200_non_existant(api_client_cognito):
    """A request for segments from a non-existent flow will return an empty list, not a 404."""
    # Arrange
    path = f"/flows/{ID_404}/segments"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 0 == len(response_json)


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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 30 == len(response_json)
    for record in response_json:
        assert "object_id" in record
        assert "timerange" in record
        assert "get_urls" not in record


def test_List_Flow_Segments_GET_200_accept_get_urls_single(
    api_client_cognito, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"accept_get_urls": create_storage_label()},
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 30 == len(response_json)
    for record in response_json:
        assert "object_id" in record
        assert "timerange" in record
        assert "get_urls" in record
        assert 1 == len(record["get_urls"])


def test_List_Flow_Segments_GET_200_accept_get_urls_multiple(
    api_client_cognito, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "accept_get_urls": f"{create_storage_label()},{create_storage_label("presigned")}"
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 30 == len(response_json)
    for record in response_json:
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
    # Assert
    assert_json_response(response, 200)
    assert_headers_present(
        response,
        "link",
        "x-paging-count",
        "x-paging-nextkey",
        "x-paging-reverse-order",
        "x-paging-timerange",
    )
    response_json = response.json()
    assert 2 == len(response_json)


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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 1 == len(response_json)
    assert object_id == response_json[0]["object_id"]


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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 30 == len(response_json)


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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    start_first = float(
        response_json[0]["timerange"].split("_")[0][1:].replace(":", ".")
    )
    start_last = float(
        response_json[-1]["timerange"].split("_")[0][1:].replace(":", ".")
    )
    assert 30 == len(response_json)
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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 2 == len(response_json)


def test_List_Flow_Segments_GET_200_include_object_timerange(
    api_client_cognito, stub_multi_flow
):
    """List segment including object_timerange"""
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"timerange": "[-60:0_-30:0)", "include_object_timerange": "true"},
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 1 == len(response_json)
    assert "object_timerange" in response_json[0]
    assert "[5:0_6:0)" == response_json[0]["object_timerange"]


def test_List_Flow_Segments_GET_400(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={"timerange": "bad"},
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


def test_List_Flow_Segments_GET_404(api_client_cognito):
    # Arrange
    path = "/flows/invalid-id/segments"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The flow ID in the path is invalid." == response_json["message"]


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
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The flow ID in the path is invalid." == response_json["message"]


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
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The flow ID in the path is invalid." == response_json["message"]


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
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The flow ID in the path is invalid." == response_json["message"]


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
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The flow ID in the path is invalid." == response_json["message"]


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
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The flow ID in the path is invalid." == response_json["message"]


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert "Bad request. The flow 'container' is not set." == response_json["message"]


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert (
        "Bad request. The timerange of the segment MUST NOT overlap any other segment in the same Flow."
        == response_json["message"]
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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert (
        "Bad request. The object id is not valid to be used for the flow id supplied."
        == response_json["message"]
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
    # Assert
    assert_json_response(response, 403)
    response_json = response.json()
    assert (
        "Forbidden. You do not have permission to modify this flow. It may be marked read-only."
        == response_json["message"]
    )


def test_Create_Flow_Segment_POST_404(api_client_cognito, media_objects):
    # Arrange
    path = f"/flows/{ID_404}/segments"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={"object_id": media_objects[-1]["object_id"], "timerange": "[0:0_1:0)"},
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The flow does not exist." == response_json["message"]


def test_Get_Media_Object_Information_HEAD_200(api_client_cognito, media_objects):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 200, empty_body=True)


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
    # Assert
    assert_json_response(response, 200, empty_body=True)


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
    # Assert
    assert_json_response(response, 200, empty_body=True)


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
    # Assert
    assert_json_response(response, 400, empty_body=True)


def test_Get_Media_Object_Information_HEAD_404(api_client_cognito):
    # Arrange
    object_id = "does-not-exist"
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
    )
    # Assert
    assert_json_response(response, 404, empty_body=True)


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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert object_id == response_json["id"]
    assert 2 == len(response_json["referenced_by_flows"])
    assert stub_video_flow["id"] == response_json["first_referenced_by_flow"]
    assert "timerange" in response_json


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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
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
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 1 == len(response_json["referenced_by_flows"])


def test_Get_Media_Object_Information_GET_200_get_urls(
    api_client_cognito, media_objects
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    label = create_storage_label()
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "accept_get_urls": label,
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "get_urls" in response_json
    assert 1 == len(response_json["get_urls"])
    assert "label" in response_json["get_urls"][0]
    assert label == response_json["get_urls"][0]["label"]


def test_Get_Media_Object_Information_GET_200_get_urls_no_match(
    api_client_cognito, media_objects
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "accept_get_urls": "no_match",
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "get_urls" not in response_json


def test_Get_Media_Object_Information_GET_200_with_storage_ids_default(
    api_client_cognito, media_objects, default_storage_id
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "accept_storage_ids": default_storage_id,
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "get_urls" in response_json
    assert 2 == len(response_json["get_urls"])


def test_Get_Media_Object_Information_GET_200_with_storage_ids_multiple(
    api_client_cognito, media_objects, default_storage_id
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "accept_storage_ids": ",".join([default_storage_id, ID_404]),
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "get_urls" in response_json
    assert 2 == len(response_json["get_urls"])


def test_Get_Media_Object_Information_GET_200_with_storage_ids_missing(
    api_client_cognito, media_objects
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "accept_storage_ids": ID_404,
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "get_urls" not in response_json


def test_Get_Media_Object_Information_GET_400_with_storage_ids_bad(
    api_client_cognito, media_objects
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "accept_storage_ids": "bad",
        },
    )
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


def test_Get_Media_Object_Information_GET_200_tag_name(
    api_client_cognito, media_objects
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "tag.test": "something else",
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 1 == len(response_json["referenced_by_flows"])


def test_Get_Media_Object_Information_GET_200_tag_exists_name(
    api_client_cognito, media_objects
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "tag_exists.test": "true",
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert 1 == len(response_json["referenced_by_flows"])


def test_Get_Media_Object_Information_GET_200_with_presigned_true(
    api_client_cognito, media_objects
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "presigned": True,
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "get_urls" in response_json
    assert 1 == len(response_json["get_urls"])
    assert "presigned" in response_json["get_urls"][0]
    assert response_json["get_urls"][0]["presigned"]


def test_Get_Media_Object_Information_GET_200_with_presigned_false(
    api_client_cognito, media_objects
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "presigned": False,
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "get_urls" in response_json
    assert 1 == len(response_json["get_urls"])


def test_Get_Media_Object_Information_GET_200_with_verbose_true(
    api_client_cognito, media_objects, default_storage_id
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "verbose_storage": True,
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "get_urls" in response_json
    assert 2 == len(response_json["get_urls"])
    assert "store_type" in response_json["get_urls"][0]
    assert "provider" in response_json["get_urls"][0]
    assert "region" in response_json["get_urls"][0]
    assert "store_product" in response_json["get_urls"][0]
    assert "storage_id" in response_json["get_urls"][0]
    assert "controlled" in response_json["get_urls"][0]
    assert "http_object_store" == response_json["get_urls"][0]["store_type"]
    assert "aws" == response_json["get_urls"][0]["provider"]
    assert REGION == response_json["get_urls"][0]["region"]
    assert "s3" == response_json["get_urls"][0]["store_product"]
    assert default_storage_id == response_json["get_urls"][0]["storage_id"]
    assert response_json["get_urls"][0]["controlled"]


def test_Get_Media_Object_Information_GET_200_with_verbose_false(
    api_client_cognito, media_objects
):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "verbose_storage": False,
        },
    )
    # Assert
    assert_json_response(response, 200)
    response_json = response.json()
    assert "get_urls" in response_json
    assert 2 == len(response_json["get_urls"])
    assert "store_type" not in response_json["get_urls"][0]
    assert "provider" not in response_json["get_urls"][0]
    assert "region" not in response_json["get_urls"][0]
    assert "store_product" not in response_json["get_urls"][0]
    assert "storage_id" not in response_json["get_urls"][0]
    assert "controlled" not in response_json["get_urls"][0]


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
    # Assert
    assert_json_response(response, 400)
    response_json = response.json()
    assert isinstance(response_json["message"], list)
    assert 0 < len(response_json["message"])


def test_Get_Media_Object_Information_GET_404(api_client_cognito):
    # Arrange
    object_id = "does-not-exist"
    path = f"/objects/{object_id}"
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
    )
    # Assert
    assert_json_response(response, 404)
    response_json = response.json()
    assert "The requested Object does not exist." == response_json["message"]


def test_Register_Media_Object_Instance_POST_201(api_client_cognito, media_objects):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}/instances"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "label": "test-this",
            "url": "https://example.com/test",
        },
    )
    # Assert
    assert_json_response(response, 201)


def test_Register_Media_Object_Instance_POST_400(api_client_cognito, media_objects):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}/instances"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={},
    )
    # Assert
    assert_json_response(response, 400)


def test_Register_Media_Object_Instance_POST_404(api_client_cognito):
    # Arrange
    path = f"/objects/{ID_404}/instances"
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "label": "test-this",
            "url": "https://example.com/test",
        },
    )
    # Assert
    assert_json_response(response, 404)


def test_Delete_Media_Object_Instance_DELETE_204(api_client_cognito, media_objects):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}/instances"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={
            "label": "test-this",
        },
    )
    # Assert
    assert_json_response(response, 204, empty_body=True)


def test_Delete_Media_Object_Instance_DELETE_400(api_client_cognito, media_objects):
    # Arrange
    object_id = media_objects[5]["object_id"]
    path = f"/objects/{object_id}/instances"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={},
    )
    # Assert
    assert_json_response(response, 400)


def test_Delete_Media_Object_Instance_DELETE_404(api_client_cognito):
    # Arrange
    path = f"/objects/{ID_404}/instances"
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={
            "label": "test-this",
        },
    )
    # Assert
    assert_json_response(response, 404)


###############################################################################
# INIT SEGMENTS (TODO STUBS)
#
# These tests validate the init_segments feature end-to-end against the live
# deployment. They are intentionally left as stubs to be populated later.
#
# IMPORTANT NOTES FOR WHOEVER FILLS THESE IN:
#
# 1. SEQUENCING / PRECEDENCE
#    Acceptance tests run in definition order within this file (there is no
#    ordering decorator - pytest collects top-to-bottom and the test_04_ file
#    prefix orders the module after flows/sources). These init stubs sit at the
#    END of the file, so by the time they run every existing segment/object
#    test has already executed. Keep the write->read->instance->(cleanup) order
#    below: later stubs depend on state created by earlier ones (a registered
#    init object cannot be read/GET/duplicated before it is registered).
#
# 2. A FLOW WITH init_segments=true IS REQUIRED FIRST
#    The service now ENFORCES consistency (see process_single_segment): a
#    segment carrying init_object_id is rejected unless the flow's
#    essence_parameters.init_segments is true, and a segment WITHOUT an init
#    object is rejected when the flow's init_segments is true. None of the
#    existing stub_*_flow fixtures set init_segments, so you cannot reuse
#    stub_video_flow etc. for the happy path. You will need EITHER:
#      (a) a new session-scoped fixture in conftest.py, e.g. stub_init_flow,
#          a video flow with a unique id + source_id and
#          essence_parameters.init_segments = true, PUT via a new
#          test_Create_or_Replace_Flow_PUT_201 stub in test_02_flows.py
#          (its source must also exist - mirror the existing source stubs), OR
#      (b) create the flow inline at the top of the first init stub below.
#    Option (a) matches the existing pattern best (see stub_video_flow +
#    test_Create_or_Replace_Flow_PUT_201_VIDEO).
#
# 3. SHARED STATE ACROSS STUBS
#    Existing tests thread allocated objects through the session-scoped
#    `media_objects` list fixture (conftest.py). The init tests need their own
#    shared list(s) so the media object_id AND the init object_id allocated in
#    the storage/upload stubs are visible to the later register/GET/instance
#    stubs. Add session-scoped `init_media_objects` and `init_objects` list
#    fixtures to conftest.py (mirror the `media_objects` fixture), or store both
#    ids on a single shared dict. Do NOT rely on module globals.
#
# 4. WRITE WORKFLOW (per AppNote 0024 / ADR 0045)
#    a. Allocate storage for the INIT segment with content_type set to the init
#       mime type (e.g. differs from the media container): POST
#       /flows/{id}/storage with {"limit": N, "content_type": "<init-mime>"}.
#    b. Allocate storage for the MEDIA segments WITHOUT content_type (defaults
#       to the flow container).
#    c. PUT-upload both to their put_urls (see test_Presigned_PUT_URL_POST_200).
#    d. Register media segments with object_id + init_object_id.
#    e. Re-use: subsequent segments SHOULD omit init_object_id; the service
#       recovers it from the stored media object.
#
# 5. CROSS-USE REJECTION
#    An object registered as a media object cannot later be used as an
#    init_object_id, and vice versa. These need two distinct object_ids set up
#    in the correct roles first.
###############################################################################


# --- Write path: storage allocation with content_type --------------------- #


def test_Allocate_Flow_Storage_POST_201_init_content_type():
    """Allocate storage for init segment(s) with an explicit content_type that
    differs from the flow container. Assert the returned put_url content-type
    matches the requested content_type (NOT the flow container).
    Store the returned object_id(s) as the INIT object(s) in shared state."""
    pytest.skip("TODO: implement init segment storage allocation with content_type")


def test_Allocate_Flow_Storage_POST_201_init_media_objects():
    """Allocate storage for the MEDIA segments of the init flow WITHOUT
    content_type; assert put_url content-type == flow container. Store the
    returned object_id(s) as the MEDIA objects in shared state."""
    pytest.skip("TODO: implement media segment storage allocation for init flow")


def test_Presigned_PUT_URL_POST_200_init():
    """Upload content to every put_url returned above (both the init object and
    the media objects), mirroring test_Presigned_PUT_URL_POST_200."""
    pytest.skip("TODO: upload init + media objects to their put_urls")


# --- Write path: segment registration -------------------------------------- #


def test_Create_Flow_Segment_POST_201_init_object_id():
    """Register a media segment on the init flow with BOTH object_id and
    init_object_id set. Assert 201. (First use: claims the init object,
    flags it as an init object, records init_storage_ids.)"""
    pytest.skip("TODO: register segment with object_id + init_object_id")


def test_Create_Flow_Segment_POST_201_init_object_reuse():
    """Register a further media segment that RE-USES the same init object but
    OMITS init_object_id in the request (spec-recommended). Assert 201 and,
    via a later GET, that the segment still reports the init_object (recovered
    from the stored media object)."""
    pytest.skip("TODO: register reused segment omitting init_object_id")


def test_Create_Flow_Segment_POST_400_init_on_non_init_flow():
    """Registering a segment with init_object_id against a flow whose
    essence_parameters.init_segments is NOT true MUST be rejected (400).
    Uses stub_video_flow (no init_segments) - assert the consistency error
    message from process_single_segment."""
    pytest.skip("TODO: assert 400 when init_object_id set on non-init flow")


def test_Create_Flow_Segment_POST_400_missing_init_on_init_flow():
    """Registering a segment WITHOUT an init object against a flow whose
    init_segments is true MUST be rejected (400). Assert the consistency
    error message. NOTE: needs a media object_id that resolves to no init
    object (i.e. a genuinely fresh object, not one that reuses an init-bearing
    media object)."""
    pytest.skip("TODO: assert 400 when init object missing on init flow")


def test_Create_Flow_Segment_POST_400_init_object_as_media():
    """An object already registered as an init object (init_object_id) MUST be
    rejected (400) if subsequently used as a media object (object_id).
    Assert the 'initialisation segment Object cannot be used as a media
    segment' error."""
    pytest.skip("TODO: assert 400 init object reused as media object")


def test_Create_Flow_Segment_POST_400_media_object_as_init():
    """An object already registered as a media object MUST be rejected (400) if
    subsequently used as an init object (init_object_id). Assert the 'media
    segment Object cannot be used as an initialisation segment' error."""
    pytest.skip("TODO: assert 400 media object reused as init object")


def test_Create_Flow_Segment_POST_400_changed_init_object_id():
    """Re-using a media object while supplying a DIFFERENT init_object_id than
    the one it was first registered with MUST be rejected (400). Assert the
    'init_object_id must not change' error."""
    pytest.skip("TODO: assert 400 when init_object_id changes on media re-use")


# --- Read path: segment GET reports init_object ----------------------------- #


def test_List_Flow_Segments_GET_200_init_object():
    """GET /flows/{id}/segments for the init flow: assert each segment carries
    an init_object with the correct object_id and populated get_urls.
    Assert consecutive reused segments report the SAME init_object.id."""
    pytest.skip("TODO: assert init_object present on segment GET")


def test_List_Flow_Segments_GET_200_init_object_get_urls_filter():
    """Assert the get_urls filter query params (accept_get_urls,
    accept_storage_ids, presigned, verbose_storage) apply to
    init_object.get_urls as well as the segment get_urls.
    Include the accept_get_urls="" (empty) case -> init_object.get_urls empty."""
    pytest.skip("TODO: assert get_urls filters apply to init_object.get_urls")


# --- Read path: object GET for init objects --------------------------------- #


def test_Get_Media_Object_Information_GET_200_init_object():
    """GET /objects/{init_object_id}: assert 200, id == init object id,
    referenced_by_flows resolved indirectly via the media objects,
    and that timerange and key_frame_count are ABSENT (init objects have
    neither), and init_object is ABSENT (an init object has no init_object)."""
    pytest.skip("TODO: assert init object queryable via /objects with no timerange")


def test_Get_Media_Object_Information_HEAD_200_init_object():
    """HEAD /objects/{init_object_id}: assert 200 with empty body."""
    pytest.skip("TODO: assert HEAD 200 for init object")


def test_Get_Media_Object_Information_GET_200_init_object_paging():
    """The init object's referenced_by_flows list paginates like a media
    object. Request with limit=1, assert X-Paging-NextKey/Link, then replay the
    RETURNED token (it targets the init-object-id-index) and assert the next
    page resolves - this exercises page_targets_init_index routing."""
    pytest.skip("TODO: assert init object flow-reference pagination")


def test_Get_Media_Object_Information_GET_200_media_object_shows_init():
    """GET /objects/{media_object_id} for a media object that has an init
    object: assert the response includes a nested init_object with id +
    get_urls."""
    pytest.skip("TODO: assert media object GET includes nested init_object")


# --- Init object instance management (full parity) -------------------------- #


def test_Register_Object_Instance_POST_201_init_controlled():
    """POST /objects/{init_object_id}/instances with a storage_id (controlled
    duplication) to a DIFFERENT backend. Assert 201. Requires >1 storage
    backend configured; if only the default exists this may need to be skipped
    or a second backend provisioned - see how media object duplication tests
    handle storage_ids (they may rely on a second backend not present in all
    deployments)."""
    pytest.skip("TODO: assert controlled instance duplication of init object")


def test_Register_Object_Instance_POST_201_init_uncontrolled():
    """POST /objects/{init_object_id}/instances with a {url, label} body
    (uncontrolled). Assert 201, then GET the init object and assert the new
    uncontrolled URL appears in get_urls (init_get_urls surfaces in the
    response)."""
    pytest.skip("TODO: assert uncontrolled instance registration for init object")


def test_Register_Object_Instance_POST_400_init_duplicate():
    """POST an instance for a storage_id / label the init object already has:
    assert 400 (already available / label in use)."""
    pytest.skip("TODO: assert 400 duplicate init object instance")


def test_Delete_Object_Instance_DELETE_204_init():
    """DELETE /objects/{init_object_id}/instances by label (the uncontrolled
    instance added above). Assert 204. Then GET and assert the URL is gone."""
    pytest.skip("TODO: assert deletion of an init object instance")


def test_Delete_Object_Instance_DELETE_400_init_last():
    """Attempting to delete the LAST remaining instance of the init object MUST
    be rejected (400) - clients must delete via flow segment deletion instead."""
    pytest.skip("TODO: assert 400 when deleting last init object instance")


# --- Cleanup / garbage collection ------------------------------------------- #


def test_Delete_Flow_Segments_DELETE_init_object_retained_while_referenced():
    """Delete ONE segment that references a reused init object while other
    segments still reference it. Assert (via GET /objects/{init_object_id})
    that the init object is RETAINED (not garbage collected) because it is
    still referenced by the remaining segments. This validates the
    init-object-id-index GC protection.
    NOTE: object cleanup is asynchronous (SQS) - allow a delay/retry before
    asserting, similar to how webhook delivery is polled."""
    pytest.skip("TODO: assert init object retained while still referenced")


def test_Delete_Flow_Segments_DELETE_init_object_collected_when_unreferenced():
    """Delete the LAST segment referencing the init object. Assert (with async
    delay/retry) that the init object is eventually garbage collected:
    GET /objects/{init_object_id} returns 404."""
    pytest.skip("TODO: assert init object collected once unreferenced")
