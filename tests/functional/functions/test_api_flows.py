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
def flow_id():
    yield str(uuid.uuid4())


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
        ({"object_ids": ["1", "2"]}, 2),
    ],
)
# pylint: disable=redefined-outer-name
def test_POST_storage_returns_200_with_storage_objects_when_flow_exists(
    lambda_context,
    api_event_factory,
    api_flows,
    flow_id,
    mock_neptune_client,
    body_value,
    media_objects_length,
):
    """Tests a POST call to storage endpoint when flow exists"""
    # Configure Neptune mock for this test
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

    # Create the event
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
    assert response["statusCode"] == HTTPStatus.CREATED.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert len(response_body["media_objects"]) == media_objects_length

    # Should i check the DDB table has the expected records in it?# pylint: disable=redefined-outer-name
