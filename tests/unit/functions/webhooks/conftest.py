import logging
from unittest.mock import Mock, MagicMock, patch
from requests import Response
from aws_lambda_powertools.utilities.typing import LambdaContext

import pytest

logger = logging.getLogger(__name__)

@pytest.fixture
def mock_lambda_context():
    context = LambdaContext()
    context._function_name = "test"
    context._memory_limit_in_mb = 128
    context._invoked_function_arn = "XXXXXXXXXXXXXXXXX"
    context._aws_request_id = "XXXXXXXXXXXXXXXXX"
    return context

@pytest.fixture
def stub_source():
    return {
        "id": "5da9130f-883b-4da4-8ad2-adb54b925e9f",
        "label": "Studio Camera 1",
        "description": "Primary studio camera",
        "format": "urn:x-nmos:format:video",
        "caps": {
            "media_types": ["video/h264", "video/raw"],
            "frame_rates": [
                {
                    "numerator": 30,
                    "denominator": 1
                },
                {
                    "numerator": 60,
                    "denominator": 1
                }
            ],
            "frame_widths": [1920, 3840],
            "frame_heights": [1080, 2160],
            "interlace_modes": ["progressive"],
            "colorspaces": ["BT709"],
            "bit_depths": [8, 10],
            "sample_rates": None,
            "channels": None
        },
        "tags": {
            "location": "Studio A",
            "manufacturer": "Sony",
            "model": "HDC-3500"
        },
        "device_id": "550e8400-e29b-41d4-a716-446655440001",
        "parents": [],
        "clock_name": "clk0",
        "created": "2024-01-23T10:00:00Z",
        "updated": "2024-01-23T10:00:00Z",
        "created_by": "system_user",
        "updated_by": "system_user"
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
            "frame_rate": {
                "numerator": 30,
                "denominator": 1
            },
            "colorspace": "BT709",
            "interlace_mode": "progressive",
            "bit_depth": 10
        }
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
                        "url": "https://BUCKET.s3.REGION.amazonaws.com/550e8400-e29b-41d4-a716-446655440002"
                    }
                ]
            }
        ]
    }
    return {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "flow_id": "550e8400-e29b-41d4-a716-446655440001",
        "object_id": "550e8400-e29b-41d4-a716-446655440002",
        "timerange": "0:0",
        "duration": 300000000,
        "format": "urn:x-nmos:format:video",
        "storage": {
            "type": "s3",
            "bucket": "XXXXXXXXXXXX",
            "key": "videos/550e8400-e29b-41d4-a716-446655440000.mp4",
            "size": 15728640
        },
        "essence_parameters": {
            "frame_width": 1920,
            "frame_height": 1080,
            "frame_rate": {
                "numerator": 30,
                "denominator": 1
            },
            "bit_depth": 10,
            "colorspace": "BT709",
            "interlace_mode": "progressive"
        },
        "codec": "video/h264",
        "container": "video/mp4",
        "avg_bit_rate": 8000000,
        "max_bit_rate": 12000000,
        "created": "2024-01-23T10:00:00Z",
        "updated": "2024-01-23T10:00:00Z",
        "created_by": "system_user",
        "updated_by": "system_user",
        "tags": {
            "scene": "interview",
            "take": "1"
        }
    }
