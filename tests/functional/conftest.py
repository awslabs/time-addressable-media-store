import base64
import json
import logging
import os
import uuid
import warnings
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

os.environ["AWS_REGION"] = "eu-west-1"
os.environ["BUCKET"] = "test-bucket"
os.environ["BUCKET_REGION"] = "eu-west-1"
os.environ["NEPTUNE_ENDPOINT"] = "example.com"
os.environ["POWERTOOLS_LOG_LEVEL"] = "INFO"
os.environ["POWERTOOLS_METRICS_NAMESPACE"] = "TAMS"
os.environ["POWERTOOLS_SERVICE_NAME"] = "tams"
os.environ["SERVICE_TABLE"] = "service-table"
os.environ["SEGMENTS_TABLE"] = "segments-table"
os.environ["STORAGE_TABLE"] = "storage-table"
os.environ["DELETE_QUEUE_URL"] = "delete-queue-url"
os.environ["S3_QUEUE_URL"] = "s3-queue-url"

logger = logging.getLogger(__name__)


############
# FIXTURES #
############


@pytest.fixture(scope="session")
def default_storage_id():
    """
    Provides a default unique backend storage ID for testing.

    Returns:
        str: A UUID string representing a backend storage ID
    """
    yield str(uuid.uuid4())


@pytest.fixture(scope="session")
def alternative_storage_id():
    """
    Provides an alternative unique backend storage ID for testing.

    Returns:
        str: A UUID string representing a backend storage ID
    """
    yield str(uuid.uuid4())


@pytest.fixture(scope="module", autouse=True)
def mock_neptune_client():
    """
    Mock the Neptune client before any imports to isolate tests from actual Neptune service.
    This is required since at the time of writing moto does not support `execute_open_cypher_query` API on Netune client.

    Returns:
        MagicMock: A mock Neptune client that can be configured in tests
    """
    # Create a mock Neptune client
    mock_client = MagicMock()
    mock_client.execute_open_cypher_query.return_value = {"results": []}

    # Save original and patch
    original_client = boto3.client

    def patched_client(service_name, *args, **kwargs):
        if service_name == "neptunedata":
            return mock_client
        return original_client(service_name, *args, **kwargs)

    # Apply patch
    boto3.client = patched_client

    yield mock_client


@pytest.fixture(autouse=True)
# pylint: disable=redefined-outer-name
def reset_mock_neptune_client(mock_neptune_client):
    """
    Reset the Neptune mock before each test to ensure isolation between test cases.

    Args:
        mock_neptune_client: The mock Neptune client fixture
    """
    mock_neptune_client.execute_open_cypher_query.return_value = {"results": []}
    yield


@pytest.fixture
def api_event_factory():
    """
    Factory fixture to create API Gateway events for Lambda function testing.

    Returns:
        function: A factory function that creates API Gateway event dictionaries
    """

    def _create_event(
        http_method, path, webhooks_enabled="Yes", query_params=None, json_body=None
    ):
        event = {
            "httpMethod": http_method,
            "path": path,
            "queryStringParameters": query_params,
            "stageVariables": {
                "api_version": "1.0",
                "webhooks_enabled": webhooks_enabled,
                "service_version": "aws.1.0",
            },
            "requestContext": {
                "httpMethod": http_method,
                "domainName": "test.com",
                "path": path,
            },
        }
        if json_body is not None:
            event["body"] = json.dumps(json_body)
            event["headers"] = {"Content-Type": "application/json"}
        return event

    return _create_event


@pytest.fixture
def lambda_context():
    """Provides a mock Lambda context object for testing Lambda functions."""

    class LambdaContext:
        def __init__(self):
            self.function_name = "test-func"
            self.memory_limit_in_mb = 128
            self.invoked_function_arn = (
                "arn:aws:lambda:eu-west-1:809313241234:function:test-func"
            )
            self.aws_request_id = "52fdfc07-2182-154f-163f-5f0f9a621d72"

        def get_remaining_time_in_millis(self) -> int:
            return 1000

    return LambdaContext()


@pytest.fixture(scope="session", autouse=True)
def aws_credentials():
    """
    Set up mock AWS credentials for testing with moto.

    This fixture runs once per test session and configures the environment
    with mock AWS credentials for all tests.
    """
    with mock_aws():
        # Set up AWS credentials
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = os.environ["AWS_REGION"]
        yield


@pytest.fixture(scope="module", autouse=True)
def s3_bucket():
    """
    Create and manage a test S3 bucket for the test module.

    Creates a test bucket before tests run and cleans it up afterward.

    Returns:
        Bucket: An S3 bucket resource for test use
    """
    # Create S3 bucket
    client = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
    client.create_bucket(
        Bucket=os.environ["BUCKET"],
        CreateBucketConfiguration={"LocationConstraint": os.environ["BUCKET_REGION"]},
    )
    yield boto3.resource("s3", region_name=os.environ["AWS_DEFAULT_REGION"]).Bucket(
        os.environ["BUCKET"]
    )
    client.delete_bucket(
        Bucket=os.environ["BUCKET"],
    )


@pytest.fixture(scope="session", autouse=True)
# pylint: disable=redefined-outer-name
def service_table(default_storage_id, alternative_storage_id):
    """
    Create and manage a test DynamoDB service table for the test module.

    Creates a service table with appropriate schema before tests run and cleans it up afterward.

    Returns:
        Table: A DynamoDB table resource for test use
    """
    # Create DynamoDB table
    client = boto3.client("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
    client.create_table(
        TableName=os.environ["SERVICE_TABLE"],
        KeySchema=[
            {"AttributeName": "record_type", "KeyType": "HASH"},
            {"AttributeName": "id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "record_type", "AttributeType": "S"},
            {"AttributeName": "id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table = boto3.resource(
        "dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"]
    ).Table(os.environ["SERVICE_TABLE"])
    table.put_item(
        Item={
            "record_type": "service",
            "id": "1",
            "name": "Example TAMS",
            "description": "An example Time Addressable Media Store",
        }
    )
    table.put_item(
        Item={
            "record_type": "storage-backend",
            "id": default_storage_id,
            "label": os.environ["BUCKET"],
            "provider": "aws",
            "region": os.environ["BUCKET_REGION"],
            "store_product": "s3",
            "store_type": "http_object_store",
            "default_storage": True,
        }
    )
    table.put_item(
        Item={
            "record_type": "storage-backend",
            "id": alternative_storage_id,
            "label": "alternative-storage",
            "provider": "aws",
            "region": "alternative-region",
            "store_product": "s3",
            "store_type": "http_object_store",
        }
    )
    yield table
    client.delete_table(TableName=os.environ["SERVICE_TABLE"])


@pytest.fixture(scope="module", autouse=True)
def segments_table():
    """
    Create and manage a test DynamoDB segments table for the test module.

    Creates a segments table with appropriate schema before tests run and cleans it up afterward.

    Returns:
        Table: A DynamoDB table resource for test use
    """
    # Create DynamoDB table
    client = boto3.client("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
    client.create_table(
        TableName=os.environ["SEGMENTS_TABLE"],
        KeySchema=[
            {"AttributeName": "flow_id", "KeyType": "HASH"},
            {"AttributeName": "timerange_end", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "flow_id", "AttributeType": "S"},
            {"AttributeName": "timerange_end", "AttributeType": "N"},
            {"AttributeName": "object_id", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "object-id-index",
                "KeySchema": [
                    {"AttributeName": "object_id", "KeyType": "HASH"},
                    {"AttributeName": "flow_id", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield boto3.resource(
        "dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"]
    ).Table(os.environ["SEGMENTS_TABLE"])
    client.delete_table(TableName=os.environ["SEGMENTS_TABLE"])


@pytest.fixture(scope="module", autouse=True)
def storage_table():
    """
    Create and manage a test DynamoDB storage table for the test module.

    Creates a storage table with appropriate schema before tests run and cleans it up afterward.

    Returns:
        Table: A DynamoDB table resource for test use
    """
    # Create DynamoDB table
    client = boto3.client("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
    client.create_table(
        TableName=os.environ["STORAGE_TABLE"],
        KeySchema=[
            {"AttributeName": "object_id", "KeyType": "HASH"},
            {"AttributeName": "flow_id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "object_id", "AttributeType": "S"},
            {"AttributeName": "flow_id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield boto3.resource(
        "dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"]
    ).Table(os.environ["STORAGE_TABLE"])
    client.delete_table(TableName=os.environ["STORAGE_TABLE"])


@pytest.fixture(autouse=True)
def ignore_warnings():
    """
    Suppress specific warnings during test execution for cleaner test output.

    Currently ignores UserWarnings from the AWS Lambda Powertools metrics module.
    """
    warnings.filterwarnings(
        "ignore", category=UserWarning, module="aws_lambda_powertools.metrics"
    )


#############
# FUNCTIONS #
#############


def create_pagination_token(data):
    """
    Create a base64 encoded DynamoDB pagination token from the provided data.

    Args:
        data (dict): The data to encode as a pagination token

    Returns:
        str: Base64 encoded pagination token
    """
    return base64.b64encode(json.dumps(data, default=int).encode("utf-8")).decode(
        "utf-8"
    )
