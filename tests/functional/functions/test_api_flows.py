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
def existing_object_id():
    yield f"test-object-{str(uuid.uuid4())}"


@pytest.fixture(scope="module")
def flow_id():
    yield str(uuid.uuid4())


@pytest.fixture(scope="module", autouse=True)
# pylint: disable=redefined-outer-name
def aws_setup(storage_table, existing_object_id, flow_id):
    storage_table.put_item(
        Item={
            "object_id": existing_object_id,
            "flow_id": flow_id,
            "expire_at": None,
        }
    )
    yield


@pytest.fixture(scope="module")
def api_flows():
    """Import api_flows after moto is active"""
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
def test_POST_storage_returns_200_with_storage_objects_when_flow_exists(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    storage_table,
    flow_id,
    body_value,
    media_objects_length,
):
    """Tests a POST call to storage endpoint when flow exists"""
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                    "container": "video/mp2t",
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{flow_id}/storage",
        None,
        body_value,
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
        assert put_url["url"].startswith("https://")
        assert ".s3." in put_url["url"]
        assert "x-amz-security-token=" in put_url["url"]
        assert put_url.get("content-type")
        assert media_object.get("object_id")
        # Check expected items are present in the storage_table
        item = storage_table.get_item(
            Key={"object_id": media_object["object_id"], "flow_id": flow_id}
        )["Item"]
        assert item is not None
        assert item.get("expire_at")


# pylint: disable=redefined-outer-name
def test_POST_storage_returns_404_when_flow_not_exists(
    lambda_context,
    api_event_factory,
    api_flows,
    flow_id,
):
    """Tests a POST call to storage endpoint when flow does not exist"""
    # Arrange
    event = api_event_factory(
        "POST",
        f"/flows/{flow_id}/storage",
        None,
        {},
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
def test_POST_storage_returns_403_when_flow_readonly(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    flow_id,
):
    """Tests a POST call to storage endpoint when flow exists"""
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": flow_id,
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
        f"/flows/{flow_id}/storage",
        None,
        {},
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
def test_POST_storage_returns_400_when_object_id_exists(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    existing_object_id,
    flow_id,
):
    """Tests a POST call to storage endpoint when flow exists"""
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                    "container": "video/mp2t",
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{flow_id}/storage",
        None,
        {"object_ids": [existing_object_id, str(uuid.uuid4())]},
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
def test_POST_storage_handles_invalid_body_parameters(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    flow_id,
    body_value,
    expected_message,
):
    """Tests a POST call to storage endpoint when flow exists"""
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                    "container": "video/mp2t",
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{flow_id}/storage",
        None,
        body_value,
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
def test_POST_storage_returns_400_when_flow_container_not_set(
    lambda_context,
    api_event_factory,
    api_flows,
    mock_neptune_client,
    flow_id,
):
    """Tests a POST call to storage endpoint when flow exists"""
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "flow": {
                    "id": flow_id,
                    "source_id": str(uuid.uuid4()),
                    "format": "urn:x-nmos:format:multi",
                }
            }
        ]
    }
    event = api_event_factory(
        "POST",
        f"/flows/{flow_id}/storage",
        None,
        {},
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
