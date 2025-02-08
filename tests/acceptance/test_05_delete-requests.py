import time
from collections import deque

import pytest
import requests

# pylint: disable=no-name-in-module
from constants import ID_404

pytestmark = [
    pytest.mark.delete,
]


@pytest.mark.no_auth
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


def test_Flow_Delete_Request_Details_HEAD_404(api_client_cognito):
    # Arrange
    path = f"/flow-delete-requests/{ID_404}"
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


def test_Flow_Delete_Request_Details_GET_404(api_client_cognito):
    # Arrange
    path = f"/flow-delete-requests/{ID_404}"
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
    assert 0 == len(list(objects_list))
