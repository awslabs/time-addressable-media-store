import time
from collections import deque

import pytest
import requests

pytestmark = [
    pytest.mark.acceptance,
]


@pytest.mark.parametrize(
    "path, verb",
    [
        ("/flow-delete-requests", "GET"),
        ("/flow-delete-requests", "HEAD"),
        ("/flow-delete-requests/{request-id}", "GET"),
        ("/flow-delete-requests/{request-id}", "HEAD"),
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
    """204 returned as stub_data_flow has no segments"""
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


def test_Delete_Flow_DELETE_204_IMAGE(api_client_cognito, stub_image_flow):
    """204 returned as stub_image_flow has no segments"""
    # Arrange
    path = f'/flows/{stub_image_flow["id"]}'
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


def test_Flow_Delete_Request_Details_GET_200(api_client_cognito, delete_requests):
    """Check that all delete requests complete"""
    queue = deque(delete_requests)
    while queue:
        request_id = queue.pop()
        # Arrange
        path = f"/flow-delete-requests/{request_id}"
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
        assert "id" in response.json()
        assert "flow_id" in response.json()
        assert "timerange_to_delete" in response.json()
        assert "delete_flow" in response.json()
        assert "status" in response.json()
        if response.json()["status"] not in ["done", "error"]:
            queue.append(request_id)
        else:
            assert "done" == response.json()["status"]


def test_List_Flow_Delete_Requests_HEAD_200(api_client_cognito):
    # Arrange
    path = "/flow-delete-requests"
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


def test_List_Flow_Delete_Requests_GET_200(api_client_cognito):
    # Arrange
    path = "/flow-delete-requests"
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
    assert 0 < len(response.json())
    for record in response.json():
        assert "id" in record
        assert "flow_id" in record
        assert "timerange_to_delete" in record
        assert "delete_flow" in record
        assert "status" in record


def test_Flow_Delete_Request_Details_HEAD_200(api_client_cognito, delete_requests):
    # Arrange
    path = f"/flow-delete-requests/{delete_requests[0]}"
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


def test_Flow_Delete_Request_Details_HEAD_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flow-delete-requests/{id_404}"
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


def test_Flow_Delete_Request_Details_GET_404(api_client_cognito, id_404):
    # Arrange
    path = f"/flow-delete-requests/{id_404}"
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
        "The requested flow delete request does not exist."
        == response.json()["message"]
    )


def test_Webhooks_Table_Empty(session, region, stack, webhooks_enabled):
    if webhooks_enabled:
        # Arrange
        dynamodb = session.resource("dynamodb", region_name=region)
        webhooks_table = dynamodb.Table(stack["outputs"]["WebhooksTable"])
        # Act
        scan = webhooks_table.scan(Select="COUNT")
        # Assert
        assert 0 == scan["Count"]


def test_FlowSegments_Table_Empty(session, region, stack):
    # Arrange
    dynamodb = session.resource("dynamodb", region_name=region)
    segments_table = dynamodb.Table(stack["outputs"]["FlowSegmentsTable"])
    # Act
    scan = segments_table.scan(Select="COUNT")
    # Assert
    assert 0 == scan["Count"]


def test_FlowStorage_Table_Empty(session, region, stack):
    # Arrange
    dynamodb = session.resource("dynamodb", region_name=region)
    storage_table = dynamodb.Table(stack["outputs"]["FlowStorageTable"])
    # Act
    scan = storage_table.scan(ProjectionExpression="id,flow_id")
    # Assert
    assert 4 == len(scan["Items"])
    with storage_table.batch_writer() as batch:
        for item in scan["Items"]:
            batch.delete_item(Key=item)


def test_S3_Bucket_Empty(session, region, stack):
    # Arrange
    sqs = session.client("sqs", region_name=region)
    queue_attributes = sqs.get_queue_attributes(
        QueueUrl=stack["outputs"]["CleanupS3QueueUrl"],
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )
    total_messages = sum(map(int, queue_attributes["Attributes"].values()))
    while total_messages > 0:
        time.sleep(1)  # Wait to allow for SQS queue to be processed.
        queue_attributes = sqs.get_queue_attributes(
            QueueUrl=stack["outputs"]["CleanupS3QueueUrl"],
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )
        total_messages = sum(map(int, queue_attributes["Attributes"].values()))
    bucket_name = stack["outputs"]["MediaStorageBucket"]
    s3 = session.resource("s3", region_name=region)
    bucket = s3.Bucket(bucket_name)
    # Act
    objects_list = bucket.objects.all()
    # Assert
    assert 1 == len(list(objects_list))
    objects_list.delete()
