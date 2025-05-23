# pylint: disable=too-many-lines
import pytest
import requests

pytestmark = [
    pytest.mark.acceptance,
]


def test_Allocate_Flow_Storage_POST_201_default(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/storage'
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


def test_Allocate_Flow_Storage_POST_201(
    api_client_cognito, media_objects, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/storage'
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
        "Bad request. Invalid flow storage request JSON or the flow 'container' is not set."
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
    for record in media_objects:
        put_file = requests.put(
            record["put_url"]["url"],
            headers={"Content-Type": record["put_url"]["content-type"]},
            data="test file content",
            timeout=30,
        )
        # Assert
        assert 200 == put_file.status_code


def test_S3_PUT_bulk_objects(session, region, stack):
    # Arrange
    bucket_name = stack["outputs"]["MediaStorageBucket"]
    s3 = session.resource("s3", region_name=region)
    bucket = s3.Bucket(bucket_name)
    # Act
    for n in range(5, 100):
        bucket.put_object(
            Key=f"20000000-0000-1000-8000-0000000{n:05}",
            Body="test file content",
        )
    # Assert
    assert True


def test_Create_Flow_Segment_POST_201_VIDEO_media_objects(
    api_client_cognito, media_objects, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    for n, record in enumerate(media_objects):
        response = api_client_cognito.request(
            "POST",
            path,
            json={"object_id": record["object_id"], "timerange": f"[{n}:0_{n + 1}:0)"},
        )
        # Assert
        assert 201 == response.status_code


def test_Create_Flow_Segment_POST_201_VIDEO_bulk(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_Create_Flow_Segment_POST_201_negative(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": "20000000-0000-1000-8000-000000000005",
            "timerange": "[-60:0_-30:0)",
        },
    )
    # Assert
    assert 201 == response.status_code


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


def test_List_Flow_Segments_HEAD_200_object_id(api_client_cognito, stub_video_flow):
    """List segments with object_id query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_List_Flow_Segments_HEAD_400_object_id(api_client_cognito, stub_video_flow):
    """List segments with object_id query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "HEAD",
        path,
        params={
            "object_id": "20000000-0000-1000-8000-000000000005",
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
    assert {
        **stub_video_flow,
        "label": "pytest",
        "description": "pytest",
        "collected_by": [stub_multi_flow["id"]],
        "timerange": "[1:0_3:0)",
        "avg_bit_rate": 6000000,
        "max_bit_rate": 6000000,
    } == response_json


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
    assert 2 == len(response.json())


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


def test_List_Flow_Segments_GET_200_object_id(api_client_cognito, stub_video_flow):
    """List segments with object_id query specified"""
    # Arrange
    object_id = "20000000-0000-1000-8000-000000000005"
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


def test_List_Flow_Segments_GET_400_object_id(api_client_cognito, stub_video_flow):
    """List segments with object_id query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "GET",
        path,
        params={
            "object_id": "20000000-0000-1000-8000-000000000005",
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


def test_Create_Flow_Segment_POST_400_container(api_client_cognito, stub_data_flow):
    """Flow missing container"""
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": "20000000-0000-1000-8000-000000000005",
            "timerange": "[0:0_1:0)",
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


def test_Create_Flow_Segment_POST_400_overlap(api_client_cognito, stub_multi_flow):
    """Timerange overlaps with existing segment"""
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": "20000000-0000-1000-8000-000000000005",
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


def test_Create_Flow_Segment_POST_400_missing_object(
    api_client_cognito, stub_multi_flow
):
    """Object must already exist in S3"""
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "POST",
        path,
        json={
            "object_id": "dummy_id",
            "timerange": "[0:100_1:0)",
        },
    )
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    # Assert
    assert 400 == response.status_code
    assert "content-type" in response_headers_lower
    assert "application/json" == response_headers_lower["content-type"]
    assert (
        "Bad request. The object id provided for a segment MUST exist."
        == response.json()["message"]
    )


def test_Create_Flow_Segment_POST_403(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/segments'
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


def test_Create_Flow_Segment_POST_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/segments"
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


def test_Delete_Flow_Segment_DELETE_202(
    api_client_cognito, delete_requests, api_endpoint, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
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


def test_Delete_Flow_Segment_DELETE_204_object_id(api_client_cognito, stub_video_flow):
    """List segments with object_id query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_Delete_Flow_Segment_DELETE_202_timerange(
    api_client_cognito, delete_requests, api_endpoint, stub_video_flow
):
    """Delete segments with timerange query specified"""
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
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


def test_Delete_Flow_Segment_DELETE_400(api_client_cognito, stub_multi_flow):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
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


def test_Delete_Flow_Segment_DELETE_400_object_id(api_client_cognito, stub_video_flow):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}/segments'
    # Act
    response = api_client_cognito.request(
        "DELETE",
        path,
        params={
            "object_id": "20000000-0000-1000-8000-000000000005",
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


def test_Delete_Flow_Segment_DELETE_403(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/segments'
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


def test_Delete_Flow_Segment_DELETE_403_object_id(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/segments'
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


def test_Delete_Flow_Segment_DELETE_403_timerange(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}/segments'
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


def test_Delete_Flow_Segment_DELETE_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/segments"
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


def test_Delete_Flow_Segment_DELETE_404_object_id(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/segments"
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


def test_Delete_Flow_Segment_DELETE_404_timerange(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}/segments"
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


def test_Delete_Flow_DELETE_202_VIDEO(
    api_client_cognito, delete_requests, api_endpoint, stub_video_flow
):
    # Arrange
    path = f'/flows/{stub_video_flow["id"]}'
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


def test_Delete_Flow_DELETE_403(api_client_cognito, stub_audio_flow):
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}'
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


def test_Delete_Flow_DELETE_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flows/{id_404}"
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


def test_Delete_Flow_DELETE_204_AUDIO(api_client_cognito, stub_audio_flow):
    """204 returned as stub_audio_flow has no segments"""
    # Need to set read_only to false prior to delete request
    api_client_cognito.request(
        "PUT",
        f'/flows/{stub_audio_flow["id"]}/read_only',
        json=False,
    )
    # Arrange
    path = f'/flows/{stub_audio_flow["id"]}'
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


def test_Delete_Flow_DELETE_204_DATA(api_client_cognito, stub_data_flow):
    """204 returned as stub_audio_flow has no segments"""
    # Arrange
    path = f'/flows/{stub_data_flow["id"]}'
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


def test_Delete_Flow_DELETE_204_MULTI(
    api_client_cognito, delete_requests, stub_multi_flow
):
    # Arrange
    path = f'/flows/{stub_multi_flow["id"]}'
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
