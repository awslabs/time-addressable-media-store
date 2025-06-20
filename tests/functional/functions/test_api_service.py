import json
from http import HTTPStatus

import pytest

pytestmark = [
    pytest.mark.functional,
]

############
# FIXTURES #
############


@pytest.fixture(scope="module")
def api_service():
    """
    Import api_service Lambda handler after moto is active.

    Returns:
        module: The api_objects Lambda handler module
    """
    # pylint: disable=import-outside-toplevel
    from api_service import app

    return app


#########
# TESTS #
#########


# pylint: disable=redefined-outer-name
def test_GET_storage_backends_returns_200_with_default_storage_backend(
    lambda_context, api_event_factory, api_service
):
    """
    Verifies that a GET request for storage backends returns 200 OK
    with correct storage backend metadata.
    """
    # Arrange
    event = api_event_factory("GET", "/service/storage-backends")

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert len(response_body) == 1
    for field in [
        "id",
        "default_storage",
        "label",
        "provider",
        "region",
        "store_product",
        "store_type",
    ]:
        assert response_body[0].get(field) is not None


# pylint: disable=redefined-outer-name
def test_HEAD_storage_backends_returns_200_with_no_body(
    lambda_context, api_event_factory, api_service
):
    """
    Verifies that a HEAD request for storage backends returns 200 OK
    with no body.
    """
    # Arrange
    event = api_event_factory("HEAD", "/service/storage-backends")

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body is None
