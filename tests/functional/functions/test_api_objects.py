import json
import os
from http import HTTPStatus

import boto3
import pytest

pytestmark = [
    pytest.mark.functional,
]

DEFAULT_TIMERANGE_END = 6000000000
DEFAULT_TIMERANGE = "[0:0_6:0)"


def test_GET_object_id_not_exists(lambda_context, api_event_factory):
    """Tests a GET call with an object_id that does not exist"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

    # Arrange
    object_id = "nonexistent-object"
    event = api_event_factory("GET", f"/objects/{object_id}")

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.NOT_FOUND.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("message") == "The requested media object does not exist."


def test_GET_object_id_exists(lambda_context, api_event_factory):
    """Tests a GET call with no query parameters and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

    # Arrange
    object_id = "test-object-123"
    flow_ids = [
        "10000000-0000-1000-8000-000000000000",
        "10000000-0000-1000-8000-000000000001",
    ]
    event = api_event_factory("GET", f"/objects/{object_id}")

    # Create the S3 object
    s3 = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
    s3.put_object(Bucket=os.environ["BUCKET"], Key=object_id, Body="test content")

    # Add items to DynamoDB tables
    dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
    segments_table = dynamodb.Table(os.environ["SEGMENTS_TABLE"])
    storage_table = dynamodb.Table(os.environ["STORAGE_TABLE"])
    for flow_id in flow_ids:
        segments_table.put_item(
            Item={
                "flow_id": flow_id,
                "timerange_end": DEFAULT_TIMERANGE_END,
                "object_id": object_id,
                "timerange": DEFAULT_TIMERANGE,
            }
        )
    storage_table.put_item(
        Item={
            "object_id": object_id,
            "flow_id": flow_ids[0],
            "expire_at": None,
        }
    )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("object_id") == object_id
    assert set(response_body.get("referenced_by_flows")) == set(flow_ids)
    assert response_body.get("first_referenced_by_flow") == flow_ids[0]


def test_GET_object_id_with_limit(lambda_context, api_event_factory):
    """Tests a GET call with limit query parameter and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

    # Arrange
    object_id = "test-object-123"
    flow_ids = [
        "10000000-0000-1000-8000-000000000000",
        "10000000-0000-1000-8000-000000000001",
    ]
    event = api_event_factory("GET", f"/objects/{object_id}", {"limit": "1"})

    # Create the S3 object
    s3 = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
    s3.put_object(Bucket=os.environ["BUCKET"], Key=object_id, Body="test content")

    # Add items to DynamoDB tables
    dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
    segments_table = dynamodb.Table(os.environ["SEGMENTS_TABLE"])
    storage_table = dynamodb.Table(os.environ["STORAGE_TABLE"])
    for flow_id in flow_ids:
        segments_table.put_item(
            Item={
                "flow_id": flow_id,
                "timerange_end": DEFAULT_TIMERANGE_END,
                "object_id": object_id,
                "timerange": DEFAULT_TIMERANGE,
            }
        )
    storage_table.put_item(
        Item={
            "object_id": object_id,
            "flow_id": flow_ids[0],
            "expire_at": None,
        }
    )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert len(response_headers.get("Link")) == 1
    assert len(response_headers.get("X-Paging-NextKey")) == 1
    assert response_body.get("object_id") == object_id
    assert len(response_body.get("referenced_by_flows")) == 1
    assert response_body.get("referenced_by_flows")[0] == flow_ids[0]
    assert response_body.get("first_referenced_by_flow") == flow_ids[0]


def test_GET_object_id_pagination(
    lambda_context, api_event_factory, create_pagination_token
):
    """Tests pagination with a GET call with limit and page query parameters and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

    # Arrange
    object_id = "test-object-123"
    flow_ids = [
        "10000000-0000-1000-8000-000000000000",
        "10000000-0000-1000-8000-000000000001",
    ]
    event = api_event_factory(
        "GET",
        f"/objects/{object_id}",
        {
            "limit": "1",
            "page": create_pagination_token(
                {
                    "flow_id": flow_ids[0],
                    "timerange_end": DEFAULT_TIMERANGE_END,
                    "object_id": object_id,
                }
            ),
        },
    )

    # Create the S3 object
    s3 = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
    s3.put_object(Bucket=os.environ["BUCKET"], Key=object_id, Body="test content")

    # Add items to DynamoDB tables
    dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
    segments_table = dynamodb.Table(os.environ["SEGMENTS_TABLE"])
    storage_table = dynamodb.Table(os.environ["STORAGE_TABLE"])
    for flow_id in flow_ids:
        segments_table.put_item(
            Item={
                "flow_id": flow_id,
                "timerange_end": DEFAULT_TIMERANGE_END,
                "object_id": object_id,
                "timerange": DEFAULT_TIMERANGE,
            }
        )
    storage_table.put_item(
        Item={
            "object_id": object_id,
            "flow_id": flow_ids[0],
            "expire_at": None,
        }
    )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_headers.get("Link") is None
    assert response_headers.get("X-Paging-NextKey") is None
    assert response_body.get("object_id") == object_id
    assert len(response_body.get("referenced_by_flows")) == 1
    assert response_body.get("referenced_by_flows")[0] == flow_ids[1]
    assert response_body.get("first_referenced_by_flow") == flow_ids[0]


def test_HEAD_object_id_exists(lambda_context, api_event_factory):
    """Tests a HEAD call with no query parameters and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

    # Arrange
    object_id = "test-object-123"
    flow_ids = [
        "10000000-0000-1000-8000-000000000000",
        "10000000-0000-1000-8000-000000000001",
    ]
    event = api_event_factory("HEAD", f"/objects/{object_id}")

    # Create the S3 object
    s3 = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
    s3.put_object(Bucket=os.environ["BUCKET"], Key=object_id, Body="test content")

    # Add items to DynamoDB tables
    dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
    segments_table = dynamodb.Table(os.environ["SEGMENTS_TABLE"])
    for flow_id in flow_ids:
        segments_table.put_item(
            Item={
                "flow_id": flow_id,
                "timerange_end": DEFAULT_TIMERANGE_END,
                "object_id": object_id,
                "timerange": DEFAULT_TIMERANGE,
            }
        )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body is None
