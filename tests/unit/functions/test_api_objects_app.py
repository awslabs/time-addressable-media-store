import base64
import json
import os
from http import HTTPStatus

import boto3
import pytest

pytestmark = [
    pytest.mark.unit,
]


def test_GET_object_id_not_exists(lambda_context):
    """Tests a GET call with an object_id that does not exist"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app

    # Arrange
    object_id = "nonexistent-object"
    event = {
        "httpMethod": "GET",
        "path": f"/objects/{object_id}",
        "queryStringParameters": None,
        "requestContext": {
            "httpMethod": "GET",
            "domainName": "test.com",
            "path": f"/objects/{object_id}",
        },
    }

    # Act
    response = app.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.NOT_FOUND.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("message") == "The requested media object does not exist."


def test_GET_object_id_exists(lambda_context):
    """Tests a GET call with no query parameters and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app

    # Arrange
    object_id = "test-object-123"
    flow_ids = [
        "10000000-0000-1000-8000-000000000000",
        "10000000-0000-1000-8000-000000000001",
    ]
    event = {
        "httpMethod": "GET",
        "path": f"/objects/{object_id}",
        "queryStringParameters": None,
        "requestContext": {
            "httpMethod": "GET",
            "domainName": "test.com",
            "path": f"/objects/{object_id}",
        },
    }

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
                "timerange_end": 6000000000,
                "object_id": object_id,
                "timerange": "[0:0_6:0)",
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
    response = app.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("object_id") == object_id
    assert set(response_body.get("referenced_by_flows")) == set(flow_ids)
    assert response_body.get("first_referenced_by_flow") == flow_ids[0]


def test_GET_object_id_with_limit(lambda_context):
    """Tests a GET call with limit query parameter and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app

    # Arrange
    object_id = "test-object-123"
    flow_ids = [
        "10000000-0000-1000-8000-000000000000",
        "10000000-0000-1000-8000-000000000001",
    ]
    event = {
        "httpMethod": "GET",
        "path": f"/objects/{object_id}",
        "queryStringParameters": {"limit": "1"},
        "requestContext": {
            "httpMethod": "GET",
            "domainName": "test.com",
            "path": f"/objects/{object_id}",
        },
    }

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
                "timerange_end": 6000000000,
                "object_id": object_id,
                "timerange": "[0:0_6:0)",
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
    response = app.lambda_handler(event, lambda_context)
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


def test_GET_object_id_pagination(lambda_context):
    """Tests pagination with a GET call with limit and page query parameters and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app

    # Arrange
    object_id = "test-object-123"
    flow_ids = [
        "10000000-0000-1000-8000-000000000000",
        "10000000-0000-1000-8000-000000000001",
    ]
    event = {
        "httpMethod": "GET",
        "path": f"/objects/{object_id}",
        "queryStringParameters": {
            "limit": "1",
            "page": base64.b64encode(
                json.dumps(
                    {
                        "flow_id": flow_ids[0],
                        "timerange_end": 6000000000,
                        "object_id": object_id,
                    },
                    default=int,
                ).encode("utf-8")
            ).decode("utf-8"),
        },
        "requestContext": {
            "httpMethod": "GET",
            "domainName": "test.com",
            "path": f"/objects/{object_id}",
        },
    }

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
                "timerange_end": 6000000000,
                "object_id": object_id,
                "timerange": "[0:0_6:0)",
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
    response = app.lambda_handler(event, lambda_context)
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


def test_HEAD_object_id_exists(lambda_context):
    """Tests a HEAD call with no query parameters and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app

    # Arrange
    object_id = "test-object-123"
    flow_ids = [
        "10000000-0000-1000-8000-000000000000",
        "10000000-0000-1000-8000-000000000001",
    ]
    event = {
        "httpMethod": "HEAD",
        "path": f"/objects/{object_id}",
        "queryStringParameters": None,
        "requestContext": {
            "httpMethod": "HEAD",
            "domainName": "test.com",
            "path": f"/objects/{object_id}",
        },
    }

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
                "timerange_end": 6000000000,
                "object_id": object_id,
                "timerange": "[0:0_6:0)",
            }
        )

    # Act
    response = app.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body is None
