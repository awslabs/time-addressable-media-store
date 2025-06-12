import base64
import json
import logging
import os
import warnings
from datetime import datetime, timedelta

import boto3
import pytest
from boto3.dynamodb.conditions import ConditionExpressionBuilder
from mediatimestamp.immutable import TimeRange, Timestamp
from moto import mock_aws

os.environ["NEPTUNE_ENDPOINT"] = "example.com"
os.environ["SEGMENTS_TABLE"] = "segments-table"
os.environ["STORAGE_TABLE"] = "storage-table"
os.environ["WEBHOOKS_TABLE"] = "webhooks-table"
os.environ["BUCKET"] = "test-bucket"
os.environ["BUCKET_REGION"] = "eu-west-1"
os.environ["WEBHOOKS_QUEUE_URL"] = "test-queue"
os.environ["USER_POOL_ID"] = "123"
os.environ["COGNITO_LAMBDA_NAME"] = "test-lambda-name"
os.environ["AWS_REGION"] = "eu-west-1"
os.environ["POWERTOOLS_LOG_LEVEL"] = "INFO"
os.environ["POWERTOOLS_SERVICE_NAME"] = "tams"
os.environ["POWERTOOLS_METRICS_NAMESPACE"] = "TAMS"

logger = logging.getLogger(__name__)
builder = ConditionExpressionBuilder()


def parse_dynamo_expression(expression):
    parsed = builder.build_expression(expression)
    return (
        parsed.condition_expression,
        parsed.attribute_name_placeholders,
        parsed.attribute_value_placeholders,
    )


@pytest.fixture
def time_range_one_day():
    now = datetime.now()
    start = Timestamp.from_datetime(now)
    end = Timestamp.from_datetime(now + timedelta(hours=24))
    timerange = TimeRange(start, end)
    return timerange


@pytest.fixture
def stub_source():
    return {
        "id": "2aa143ac-0ab7-4d75-bc32-5c00c13d186f",
        "format": "urn:x-nmos:format:video",
        "label": "bbb",
        "description": "Big Buck Bunny video",
        "created_by": "tams-dev",
        "updated_by": "tams-dev",
        "created": "2008-05-27T18:51:00Z",
        "updated": "2008-05-27T18:51:00Z",
        "collected_by": ["86761f3a-5998-4cfe-9a89-8459bcb8ea52"],
    }


@pytest.fixture
def stub_flow():
    return {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "source_id": "550e8400-e29b-41d4-a716-446655440001",
        "format": "urn:x-nmos:format:video",
        "label": "Main Camera Feed",
        "description": "Primary studio camera output",
        "created_by": "system_user",
        "codec": "video/h264",
        "container": "video/mp4",
        "avg_bit_rate": 8000,
        "max_bit_rate": 12000,
        "essence_parameters": {
            "frame_width": 1920,
            "frame_height": 1080,
            "frame_rate": {"numerator": 30, "denominator": 1},
            "colorspace": "BT709",
            "interlace_mode": "progressive",
            "bit_depth": 10,
        },
    }


@pytest.fixture
def stub_flowsegment():
    return {
        "flow_id": "550e8400-e29b-41d4-a716-446655440001",
        "segments": [
            {
                "object_id": "550e8400-e29b-41d4-a716-446655440002",
                "timerange": "[0:0_6:0)",
                "last_duration": "string",
                "get_urls": [
                    {
                        "label": "aws.eu-west-2:s3:Example TAMS",
                        "url": "https://BUCKET.s3.REGION.amazonaws.com/550e8400-e29b-41d4-a716-446655440002",
                    }
                ],
            }
        ],
    }


@pytest.fixture
def api_event_factory():
    """Factory fixture to create API Gateway events"""

    def _create_event(http_method, path, query_params=None):
        return {
            "httpMethod": http_method,
            "path": path,
            "queryStringParameters": query_params,
            "requestContext": {
                "httpMethod": http_method,
                "domainName": "test.com",
                "path": path,
            },
        }

    return _create_event


@pytest.fixture
def lambda_context():
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


@pytest.fixture(scope="function", autouse=True)
def aws_setup():
    with mock_aws():
        # Set up AWS credentials
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = os.environ["AWS_REGION"]

        # Create S3 bucket
        s3 = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
        s3.create_bucket(
            Bucket=os.environ["BUCKET"],
            CreateBucketConfiguration={
                "LocationConstraint": os.environ["BUCKET_REGION"]
            },
        )

        # Create DynamoDB tables with the required indexes
        dynamodb = boto3.client(
            "dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"]
        )
        dynamodb.create_table(
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

        dynamodb.create_table(
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

        yield


@pytest.fixture(autouse=True)
def ignore_warnings():
    warnings.filterwarnings(
        "ignore", category=UserWarning, module="aws_lambda_powertools.metrics"
    )


@pytest.fixture
def create_pagination_token():
    """Create a base64 encoded dynamodb pagination token"""

    def _create_token(data):
        return base64.b64encode(json.dumps(data, default=int).encode("utf-8")).decode(
            "utf-8"
        )

    return _create_token
