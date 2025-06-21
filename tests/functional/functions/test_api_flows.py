import json
import os
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
def existing_object_id():
    """
    Provides a unique object ID that already exists in the storage table.

    Returns:
        str: A unique test object identifier with UUID
    """
    yield f"test-object-{str(uuid.uuid4())}"


@pytest.fixture(scope="module")
def sample_flow_id():
    """
    Provides a unique flow ID for testing.

    Returns:
        str: A UUID string representing a flow ID
    """
    yield str(uuid.uuid4())


@pytest.fixture(scope="module")
def default_storage_id():
    """
    Provides a default unique backend storage ID for testing.

    Returns:
        str: A UUID string representing a backend storage ID
    """
    yield str(uuid.uuid4())


@pytest.fixture(scope="module")
def alternative_storage_id():
    """
    Provides an alternative unique backend storage ID for testing.

    Returns:
        str: A UUID string representing a backend storage ID
    """
    yield str(uuid.uuid4())


@pytest.fixture(scope="module", autouse=True)
# pylint: disable=redefined-outer-name
def aws_setup(
    storage_table,
    existing_object_id,
    sample_flow_id,
    default_storage_id,
    alternative_storage_id,
    service_table,
):
    """
    Sets up test data in AWS resources before tests run.

    Creates an entry in the storage table with the test object ID and flow ID.

    Args:
        storage_table: The DynamoDB storage table fixture
        existing_object_id: The test object ID fixture
        sample_flow_id: The test flow ID fixture
    """
    storage_table.put_item(
        Item={
            "object_id": existing_object_id,
            "flow_id": sample_flow_id,
            "expire_at": None,
        }
    )

    service_table.put_item(
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
    service_table.put_item(
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
    yield


@pytest.fixture(scope="module")
def api_flows():
    """
    Import api_flows Lambda handler after moto is active.

    Returns:
        module: The api_flows Lambda handler module
    """
    # pylint: disable=import-outside-toplevel
    from api_flows import app

    return app


#########
# TESTS #
#########


@pytest.mark.parametrize(
    "body_value,media_objects_length",
    [
        ({}, constants.DEFAULT_PUT_LIMIT),
        ({"limit": 5}, 5),
        ({"object_ids": [str(uuid.uuid4()) for _ in range(2)]}, 2),
    ],
)
# pylint: disable=redefined-outer-name
def test_POST_storage_returns_201_with_default_storage_objects_when_flow_exists(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    storage_table,
    sample_flow_id,
    default_storage_id,
    body_value,
    media_objects_length,
):
    """
    Verifies that a POST request to the storage endpoint returns 201 Created
    with the expected default storage objects when the flow exists and all parameters are valid.

    Tests various combinations of request body parameters including default limit,
    custom limit, and specific object IDs.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": sample_flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                    "container": "video/mp2t",
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{sample_flow_id}/storage",
        query_params=None,
        json_body=body_value,
    )

    # Act
    response = api_flows.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])
    media_objects = response_body["media_objects"]

    # Assert
    assert response["statusCode"] == HTTPStatus.CREATED.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    if "object_ids" in body_value:
        assert set(body_value["object_ids"]) == set(
            media_object["object_id"] for media_object in media_objects
        )
    else:
        assert len(media_objects) == media_objects_length
    for media_object in media_objects:
        put_url = media_object.get("put_url")
        assert put_url
        assert isinstance(put_url, dict)
        assert put_url.get("url")
        assert put_url["url"].startswith(f"https://{os.environ["BUCKET"]}.s3.")
        assert "x-amz-security-token=" in put_url["url"]
        assert put_url.get("content-type")
        assert media_object.get("object_id")
        # Check expected items are present in the storage_table
        item = storage_table.get_item(
            Key={"object_id": media_object["object_id"], "flow_id": sample_flow_id}
        )["Item"]
        assert item is not None
        assert item.get("expire_at")
        assert item["storage_ids"] == [default_storage_id]


# pylint: disable=redefined-outer-name
def test_POST_storage_returns_201_with_alternative_storage_objects_when_flow_exists(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    storage_table,
    sample_flow_id,
    alternative_storage_id,
):
    """
    Verifies that a POST request to the storage endpoint returns 201 Created
    with the expected alternative storage objects when the flow exists and all parameters are valid.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": sample_flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                    "container": "video/mp2t",
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{sample_flow_id}/storage",
        query_params=None,
        json_body={"limit": 1, "storage_id": alternative_storage_id},
    )

    # Act
    response = api_flows.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])
    media_objects = response_body["media_objects"]

    # Assert
    assert response["statusCode"] == HTTPStatus.CREATED.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert len(media_objects) == 1
    for media_object in media_objects:
        put_url = media_object.get("put_url")
        assert put_url
        assert isinstance(put_url, dict)
        assert put_url.get("url")
        assert put_url["url"].startswith("https://alternative-storage.s3.")
        assert media_object.get("object_id")
        # Check expected items are present in the storage_table
        item = storage_table.get_item(
            Key={"object_id": media_object["object_id"], "flow_id": sample_flow_id}
        )["Item"]
        assert item is not None
        assert item.get("expire_at")
        assert item["storage_ids"] == [alternative_storage_id]


# pylint: disable=redefined-outer-name
def test_POST_storage_returns_400_with_storage_id_not_exists(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    sample_flow_id,
):
    """
    Verifies that a POST request to the storage endpoint returns 400 Bad Request
    when the supplied storage_id does not exist.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": sample_flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                    "container": "video/mp2t",
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{sample_flow_id}/storage",
        query_params=None,
        json_body={"limit": 1, "storage_id": "90000000-0000-1000-8000-000000000000"},
    )

    # Act
    response = api_flows.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body["message"] == "Invalid storage backend identifier"


# pylint: disable=redefined-outer-name
def test_POST_storage_returns_400_with_invalid_storage_id(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    sample_flow_id,
):
    """
    Verifies that a POST request to the storage endpoint returns 400 Bad Request
    when the supplied storage_id is not valid.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": sample_flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                    "container": "video/mp2t",
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{sample_flow_id}/storage",
        query_params=None,
        json_body={"limit": 1, "storage_id": "invalid"},
    )

    # Act
    response = api_flows.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert (
        response_body.get("message")[0]["msg"]
        == "String should match pattern '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'"
    )


# pylint: disable=redefined-outer-name
def test_POST_storage_returns_404_when_flow_not_exists(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    sample_flow_id,
):
    """
    Verifies that a POST request to the storage endpoint returns 404 Not Found
    when attempting to access a flow that does not exist in the database.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {"results": []}
    event = api_event_factory(
        "POST",
        f"/flows/{sample_flow_id}/storage",
        query_params=None,
        json_body={},
    )

    # Act
    response = api_flows.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.NOT_FOUND.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("message") == "The requested flow does not exist."


# pylint: disable=redefined-outer-name
def test_POST_storage_returns_403_when_flow_is_readonly(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    sample_flow_id,
):
    """
    Verifies that a POST request to the storage endpoint returns 403 Forbidden
    when attempting to modify a flow that is marked as read-only.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": sample_flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                    "container": "video/mp2t",
                    "read_only": True,
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{sample_flow_id}/storage",
        query_params=None,
        json_body={},
    )

    # Act
    response = api_flows.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.FORBIDDEN.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert (
        response_body.get("message")
        == "Forbidden. You do not have permission to modify this flow. It may be marked read-only."
    )


# pylint: disable=redefined-outer-name
def test_POST_storage_returns_400_when_requested_object_id_already_exists(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    existing_object_id,
    sample_flow_id,
):
    """
    Verifies that a POST request to the storage endpoint returns 400 Bad Request
    when attempting to create storage for an object ID that already exists.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": sample_flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                    "container": "video/mp2t",
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{sample_flow_id}/storage",
        query_params=None,
        json_body={"object_ids": [existing_object_id, str(uuid.uuid4())]},
    )

    # Act
    response = api_flows.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert (
        response_body.get("message")
        == "Bad request. Invalid flow storage request JSON or the flow 'container' is not set. If object_ids supplied, some or all already exist."
    )


@pytest.mark.parametrize(
    "body_value,expected_message",
    [
        (
            "test",
            "Input should be a valid dictionary or object to extract fields from",
        ),
        (
            {"limit": "unable to case to int"},
            "Input should be a valid integer, unable to parse string as an integer",
        ),
        ({"object_ids": [1, 2]}, "Input should be a valid string"),
    ],
)
# pylint: disable=redefined-outer-name
def test_POST_storage_returns_400_with_validation_errors_for_invalid_body_parameters(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    sample_flow_id,
    body_value,
    expected_message,
):
    """
    Verifies that a POST request to the storage endpoint returns 400 Bad Request
    with appropriate validation error messages when provided with invalid body parameters.

    Tests various invalid input scenarios:
    - Non-dictionary body
    - Non-integer limit value
    - Non-string object IDs
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": sample_flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                    "container": "video/mp2t",
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{sample_flow_id}/storage",
        query_params=None,
        json_body=body_value,
    )

    # Act
    response = api_flows.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("message")[0]["msg"] == expected_message


# pylint: disable=redefined-outer-name
def test_POST_storage_returns_400_when_flow_missing_required_container_attribute(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    sample_flow_id,
):
    """
    Verifies that a POST request to the storage endpoint returns 400 Bad Request
    when the flow exists but is missing the required 'container' attribute.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": sample_flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{sample_flow_id}/storage",
        query_params=None,
        json_body={},
    )

    # Act
    response = api_flows.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert (
        response_body.get("message")
        == "Bad request. Invalid flow storage request JSON or the flow 'container' is not set. If object_ids supplied, some or all already exist."
    )
