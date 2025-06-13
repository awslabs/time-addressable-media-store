import json
import uuid
from http import HTTPStatus

import constants
import pytest
from conftest import create_pagination_token

pytestmark = [
    pytest.mark.functional,
]

############
# FIXTURES #
############


@pytest.fixture(scope="module")
def object_id():
    yield f"test-object-{str(uuid.uuid4())}"


@pytest.fixture(scope="module")
def timerange():
    yield {"str": "[0:0_6:0)", "end": 5999999999}


@pytest.fixture(scope="module")
def flow_ids():
    yield sorted([str(uuid.uuid4()) for _ in range(2)])


@pytest.fixture(scope="module", autouse=True)
# pylint: disable=redefined-outer-name
def aws_setup(s3_bucket, segments_table, storage_table, object_id, timerange, flow_ids):
    s3_bucket.put_object(Key=object_id, Body="test content")
    for flow_id in flow_ids:
        segments_table.put_item(
            Item={
                "flow_id": flow_id,
                "timerange_end": timerange["end"],
                "object_id": object_id,
                "timerange": timerange["str"],
            }
        )
    storage_table.put_item(
        Item={
            "object_id": object_id,
            "flow_id": flow_ids[0],
            "expire_at": None,
        }
    )
    yield
    s3_bucket.delete_objects(Delete={"Objects": [{"Key": object_id}]})


@pytest.fixture(scope="module")
def api_objects():
    """Import api_objects after moto is active"""
    # pylint: disable=import-outside-toplevel
    from api_objects import app

    return app


@pytest.fixture(
    params=[
        "invalid_token",  # non base64
        "non_json",  # non json
        "missing_fields",  # missing required fields
    ]
)
def invalid_page_value(request, object_id):
    """Fixture that provides different invalid page values"""
    param_type = request.param

    if param_type == "invalid_token":
        return "invalid_token"
    elif param_type == "non_json":
        return create_pagination_token("invalid token")
    elif param_type == "missing_fields":
        return create_pagination_token({"object_id": object_id})


#########
# TESTS #
#########


# pylint: disable=redefined-outer-name
def test_GET_object_returns_404_when_object_id_does_not_exist(
    lambda_context, api_event_factory, api_objects
):
    """Tests a GET call with an object_id that does not exist"""
    # Arrange
    event = api_event_factory("GET", "/objects/nonexistent-object")

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.NOT_FOUND.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("message") == "The requested media object does not exist."


# pylint: disable=redefined-outer-name
def test_GET_object_returns_200_with_complete_flow_references_when_object_exists(
    lambda_context, api_event_factory, object_id, flow_ids, api_objects
):
    """Tests a GET call with no query parameters and an object_id that exists"""
    # Arrange
    event = api_event_factory("GET", f"/objects/{object_id}")

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


# pylint: disable=redefined-outer-name
def test_GET_object_returns_limited_flow_references_with_pagination_headers_when_limit_specified(
    lambda_context, api_event_factory, object_id, flow_ids, api_objects
):
    """Tests a GET call with limit query parameter and an object_id that exists"""
    # Arrange
    event = api_event_factory("GET", f"/objects/{object_id}", {"limit": "1"})

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


# pylint: disable=redefined-outer-name
def test_GET_object_returns_next_page_of_flow_references_when_pagination_token_provided(
    lambda_context,
    api_event_factory,
    object_id,
    timerange,
    flow_ids,
    api_objects,
):
    """Tests pagination with a GET call with limit and page query parameters and an object_id that exists"""
    # Arrange
    event = api_event_factory(
        "GET",
        f"/objects/{object_id}",
        {
            "limit": "1",
            "page": create_pagination_token(
                {
                    "flow_id": flow_ids[0],
                    "timerange_end": timerange["end"],
                    "object_id": object_id,
                }
            ),
        },
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


def test_HEAD_object_returns_200_with_empty_body_when_object_exists(
    lambda_context, api_event_factory, object_id, api_objects
):
    """Tests a HEAD call with no query parameters and an object_id that exists"""
    # Arrange
    event = api_event_factory("HEAD", f"/objects/{object_id}")

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body is None


def test_GET_object_handles_maximum_limit_parameter(
    lambda_context, api_event_factory, object_id, api_objects
):
    """Tests handling of limit parameter exceeding maximum allowed value"""
    # Arrange
    event = api_event_factory("GET", f"/objects/{object_id}", {"limit": "1000"})

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]

    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_headers.get("X-Paging-Limit")[0] == str(constants.MAX_PAGE_LIMIT)


@pytest.mark.parametrize(
    "limit_value,expected_message",
    [
        (
            "invalid",
            "Input should be a valid integer, unable to parse string as an integer",
        ),
        ("-5", "Input should be greater than 0"),
    ],
)
def test_GET_object_handles_invalid_limit_parameter(
    lambda_context,
    api_event_factory,
    object_id,
    api_objects,
    limit_value,
    expected_message,
):
    """Tests handling of non-numeric limit parameter"""
    # Arrange
    event = api_event_factory("GET", f"/objects/{object_id}", {"limit": limit_value})

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("message")[0]["msg"] == expected_message


def test_GET_object_handles_invalid_page_parameter(
    lambda_context, api_event_factory, object_id, api_objects, invalid_page_value
):
    """Tests handling of invalid page values"""
    # Arrange
    event = api_event_factory(
        "GET", f"/objects/{object_id}", {"page": invalid_page_value}
    )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_body.get("message") == "Invalid page parameter value"
