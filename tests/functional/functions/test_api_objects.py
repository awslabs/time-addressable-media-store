import json
import uuid
from http import HTTPStatus

import constants
import pytest

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


#########
# TESTS #
#########


def test_GET_object_returns_404_when_object_id_does_not_exist(
    lambda_context, api_event_factory
):
    """Tests a GET call with an object_id that does not exist"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

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
    lambda_context, api_event_factory, object_id, flow_ids
):
    """Tests a GET call with no query parameters and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

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
    lambda_context, api_event_factory, object_id, flow_ids
):
    """Tests a GET call with limit query parameter and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

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
    create_pagination_token,
    object_id,
    timerange,
    flow_ids,
):
    """Tests pagination with a GET call with limit and page query parameters and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

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
    lambda_context, api_event_factory, object_id
):
    """Tests a HEAD call with no query parameters and an object_id that exists"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

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


def test_GET_object_handles_invalid_limit_parameter(
    lambda_context, api_event_factory, object_id
):
    """Tests handling of non-numeric limit parameter"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

    # Arrange
    event = api_event_factory("GET", f"/objects/{object_id}", {"limit": "invalid"})

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert (
        response_body.get("message")[0]["msg"]
        == "Input should be a valid integer, unable to parse string as an integer"
    )


def test_GET_object_handles_negative_limit_parameter(
    lambda_context, api_event_factory, object_id
):
    """Tests handling of negative limit parameter"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

    # Arrange
    event = api_event_factory("GET", f"/objects/{object_id}", {"limit": "-5"})

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("message")[0]["msg"] == "Input should be greater than 0"


def test_GET_object_handles_maximum_limit_parameter(
    lambda_context, api_event_factory, object_id
):
    """Tests handling of limit parameter exceeding maximum allowed value"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

    # Arrange
    event = api_event_factory("GET", f"/objects/{object_id}", {"limit": "1000"})

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]

    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_headers.get("X-Paging-Limit")[0] == str(constants.MAX_PAGE_LIMIT)


def test_GET_object_handles_invalid_pagination_token_non_base64(
    lambda_context, api_event_factory, object_id
):
    """Tests handling of invalid pagination token, where the value is not base64 encoded"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

    # Arrange
    event = api_event_factory("GET", f"/objects/{object_id}", {"page": "invalid_token"})

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_body.get("message") == "Invalid page parameter value"


def test_GET_object_handles_invalid_pagination_token_non_json(
    lambda_context, api_event_factory, create_pagination_token, object_id
):
    """Tests handling of invalid pagination token, where the value is not base64 encoded json"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

    # Arrange
    event = api_event_factory(
        "GET",
        f"/objects/{object_id}",
        {"page": create_pagination_token("invalid token")},
    )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_body.get("message") == "Invalid page parameter value"


def test_GET_object_handles_invalid_pagination_token_non_valid_json(
    lambda_context, api_event_factory, create_pagination_token, object_id
):
    """Tests handling of invalid pagination token, where the value is not base64 encoded valid json"""
    # pylint: disable=import-outside-toplevel
    # Import app inside the test to ensure moto is active
    from api_objects import app as api_objects

    # Arrange
    event = api_event_factory(
        "GET",
        f"/objects/{object_id}",
        {
            "page": create_pagination_token(
                {
                    "object_id": object_id,
                }
            )
        },
    )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_body.get("message") == "Invalid page parameter value"
