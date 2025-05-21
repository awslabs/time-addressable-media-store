import logging
import os
from datetime import datetime, timedelta

import pytest
from boto3.dynamodb.conditions import ConditionExpressionBuilder
from mediatimestamp.immutable import TimeRange, Timestamp

os.environ["WEBHOOKS_TABLE"] = "TEST_TABLE"
os.environ["BUCKET"] = "TEST_BUCKET"
os.environ["BUCKET_REGION"] = "eu-west-1"
os.environ["WEBHOOKS_QUEUE_URL"] = "TEST_QUEUE"
os.environ["USER_POOL_ID"] = "123"
os.environ["COGNITO_LAMBDA_NAME"] = "USERNAME_LAMBDA"
os.environ["AWS_REGION"] = "eu-west-1"

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
