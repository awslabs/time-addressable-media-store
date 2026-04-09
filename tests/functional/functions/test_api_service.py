# pylint: disable=too-many-lines
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
def webhook_ids():
    return [
        "00000000-0000-1000-8000-000000000001",
        "00000000-0000-1000-8000-000000000002",
        "00000000-0000-1000-8000-000000000003",
    ]


@pytest.fixture(scope="module")
def stub_webhook_basic():
    """Basic webhook configuration for testing."""
    return {
        "url": "https://hook.example.com",
        "api_key_name": "Authorization",
        "events": ["flows/created", "flows/updated", "flows/deleted"],
    }


@pytest.fixture(scope="module")
def stub_webhook_tags():
    return {
        "url": "https://hook.example.com",
        "api_key_name": "Authorization",
        "events": ["sources/created", "sources/updated", "sources/deleted"],
        "tags": {"auth_classes": ["news", "sports"]},
    }


@pytest.fixture(scope="module")
def api_service():
    """
    Import api_service Lambda handler after moto is active.

    Returns:
        module: The api_service Lambda handler module
    """
    # pylint: disable=import-outside-toplevel
    from api_service import app

    return app


#########
# TESTS #
#########


@pytest.mark.parametrize(
    "method,check_body",
    [
        ("HEAD", False),
        ("GET", True),
    ],
)
# pylint: disable=redefined-outer-name
def test_List_Root_Endpoints(
    lambda_context, api_event_factory, api_service, method, check_body
):
    """
    Verifies that HEAD/GET requests to root return 200 OK.
    GET also validates response body contains correct endpoints.
    """
    # Arrange
    event = api_event_factory(method, "/")

    # Act
    response = api_service.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value

    if check_body:
        response_headers = response["multiValueHeaders"]
        response_body = json.loads(response["body"])
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert set(
            ["service", "flows", "sources", "objects", "flow-delete-requests"]
        ) == set(response_body)


@pytest.mark.parametrize(
    "method,check_body",
    [
        ("HEAD", False),
        ("GET", True),
    ],
)
# pylint: disable=redefined-outer-name
def test_Service_Information(
    lambda_context, api_event_factory, api_service, method, check_body
):
    """
    Verifies that HEAD/GET requests for service information return 200 OK.
    GET also validates response body contains correct service metadata.
    """
    # Arrange
    event = api_event_factory(method, "/service")

    # Act
    response = api_service.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value

    if check_body:
        response_headers = response["multiValueHeaders"]
        response_body = json.loads(response["body"])
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert "type" in response_body
        assert "api_version" in response_body
        assert "service_version" in response_body
        assert "event_stream_mechanisms" in response_body
        assert "min_object_timeout" in response_body
        assert "min_presigned_url_timeout" in response_body


# pylint: disable=redefined-outer-name
def test_Update_Service_Information_POST_200(
    lambda_context, api_event_factory, api_service
):
    """
    Verifies that a POST request to update service information returns 200 OK.
    """
    # Arrange
    event = api_event_factory(
        "POST",
        "/service",
        json_body={
            "name": "Example TAMS",
            "description": "An example Time Addressable Media Store",
        },
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body is None


# pylint: disable=redefined-outer-name
def test_Update_Service_Information_POST_400(
    lambda_context, api_event_factory, api_service
):
    """
    Verifies that a POST request with invalid data types returns 400 Bad Request.
    """
    # Arrange
    event = api_event_factory(
        "POST",
        "/service",
        json_body={
            "name": 0,
        },
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("message")[0]["msg"] == "Input should be a valid string"


# pylint: disable=redefined-outer-name
def test_Register_Webhook_URL_POST_201_create(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    stub_webhook_basic,
):
    """
    Verifies that a POST request creates a webhook and returns 201 Created.
    """
    # Arrange
    event = api_event_factory(
        "POST",
        "/service/webhooks",
        json_body={**stub_webhook_basic, "api_key_value": "Bearer dummytokenvalue"},
    )

    # Mock Neptune to return success for webhook creation
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "webhook": {
                    **stub_webhook_basic,
                    "id": event["requestContext"]["requestId"],
                    "status": "created",
                }
            }
        ]
    }

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])
    response_headers = response["multiValueHeaders"]

    # Assert
    assert "id" in response_body
    response_body.pop("id")
    assert response["statusCode"] == HTTPStatus.CREATED.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert {**stub_webhook_basic, "status": "created"} == response_body


# pylint: disable=redefined-outer-name
def test_Register_Webhook_URL_POST_201_create_tags(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    stub_webhook_tags,
):
    """
    Verifies that a POST request creates a webhook with tags and returns 201 Created.
    """
    # Arrange
    event = api_event_factory(
        "POST",
        "/service/webhooks",
        json_body={**stub_webhook_tags, "api_key_value": "Bearer dummytokenvalue"},
    )

    # Mock Neptune to return success for webhook creation
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "webhook": {
                    **stub_webhook_tags,
                    "tags": {
                        k: json.dumps(v) for k, v in stub_webhook_tags["tags"].items()
                    },
                    "id": event["requestContext"]["requestId"],
                    "status": "created",
                }
            }
        ]
    }

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])
    response_headers = response["multiValueHeaders"]

    # Assert
    assert "id" in response_body
    response_body.pop("id")
    assert response["statusCode"] == HTTPStatus.CREATED.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert {**stub_webhook_tags, "status": "created"} == response_body


# pylint: disable=redefined-outer-name
def test_Register_Webhook_URL_POST_201_create_empty_events(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    stub_webhook_basic,
):
    """
    Verifies that a POST request with empty events list creates a webhook and returns 201 Created.
    """
    # Arrange
    webhook = {**stub_webhook_basic, "events": []}
    event = api_event_factory(
        "POST",
        "/service/webhooks",
        json_body={**webhook, "api_key_value": "Bearer dummytokenvalue"},
    )

    # Mock Neptune to return success for webhook creation
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "webhook": {
                    **webhook,
                    "id": event["requestContext"]["requestId"],
                    "status": "created",
                }
            }
        ]
    }

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])
    response_headers = response["multiValueHeaders"]

    # Assert
    assert "id" in response_body
    response_body.pop("id")
    assert response["statusCode"] == HTTPStatus.CREATED.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert {**webhook, "status": "created"} == {**response_body, "events": []}


# pylint: disable=redefined-outer-name
def test_Register_Webhook_URL_POST_400_invalid_json(
    lambda_context, api_event_factory, api_service
):
    """
    Verifies that a POST request with invalid data types returns 400 Bad Request.
    """
    # Arrange
    event = api_event_factory(
        "POST",
        "/service/webhooks",
        json_body={
            "url": 0,
        },
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("message")[0]["msg"] == "Input should be a valid string"


# pylint: disable=redefined-outer-name
def test_Register_Webhook_URL_POST_400_missing_url(
    lambda_context, api_event_factory, api_service
):
    """
    Verifies that a POST request without URL field returns 400 Bad Request.
    """
    # Arrange
    event = api_event_factory(
        "POST",
        "/service/webhooks",
        json_body={
            "events": ["flows/created", "flows/updated", "flows/deleted"],
        },
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert isinstance(response_body["message"], list)
    assert len(response_body["message"]) > 0
    assert response_body["message"][0]["type"] == "missing"
    assert response_body["message"][0]["loc"] == ["body", "url"]


# pylint: disable=redefined-outer-name
def test_Register_Webhook_URL_POST_400_invalid_events(
    lambda_context, api_event_factory, api_service
):
    """
    Verifies that a POST request with invalid event types returns 400 Bad Request.
    """
    # Arrange
    event = api_event_factory(
        "POST",
        "/service/webhooks",
        json_body={
            "url": "https://hook.example.com",
            "events": ["invalid"],
        },
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert isinstance(response_body["message"], list)
    assert len(response_body["message"]) > 0
    assert response_body["message"][0]["type"] == "enum"


@pytest.mark.parametrize(
    "method,check_body",
    [
        ("HEAD", False),
        ("GET", True),
    ],
)
# pylint: disable=redefined-outer-name
def test_List_Webhook_URLs_200(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    webhook_ids,
    stub_webhook_basic,
    stub_webhook_tags,
    method,
    check_body,
):
    """
    Verifies that HEAD/GET requests return list of webhooks.
    GET also validates response body contains webhook data.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "webhook": {
                    **stub_webhook_basic,
                    "id": webhook_ids[0],
                    "status": "created",
                }
            },
            {
                "webhook": {
                    **stub_webhook_tags,
                    "tags": {
                        k: json.dumps(v) for k, v in stub_webhook_tags["tags"].items()
                    },
                    "id": webhook_ids[1],
                    "status": "created",
                }
            },
        ]
    }
    event = api_event_factory(method, "/service/webhooks")

    # Act
    response = api_service.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value

    if check_body:
        response_headers = response["multiValueHeaders"]
        response_body = json.loads(response["body"])
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert len(response_body) == 2
        assert {
            **stub_webhook_basic,
            "id": webhook_ids[0],
            "status": "created",
        } in response_body
        assert {
            **stub_webhook_tags,
            "id": webhook_ids[1],
            "status": "created",
        } in response_body


@pytest.mark.parametrize(
    "method,check_body",
    [
        ("HEAD", False),
        ("GET", True),
    ],
)
# pylint: disable=redefined-outer-name
def test_List_Webhook_URLs_200_tag_name(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    webhook_ids,
    stub_webhook_tags,
    method,
    check_body,
):
    """
    Verifies that HEAD/GET requests with tag filter return filtered webhooks.
    GET also validates response body contains filtered webhook data.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "webhook": {
                    **stub_webhook_tags,
                    "tags": {
                        k: json.dumps(v) for k, v in stub_webhook_tags["tags"].items()
                    },
                    "id": webhook_ids[1],
                    "status": "created",
                }
            }
        ]
    }
    event = api_event_factory(
        method, "/service/webhooks", query_params={"tag.auth_classes": "news"}
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value

    if check_body:
        response_headers = response["multiValueHeaders"]
        response_body = json.loads(response["body"])
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert len(response_body) == 1
        assert {
            **stub_webhook_tags,
            "id": webhook_ids[1],
            "status": "created",
        } in response_body


@pytest.mark.parametrize(
    "method,check_body",
    [
        ("HEAD", False),
        ("GET", True),
    ],
)
# pylint: disable=redefined-outer-name
def test_List_Webhook_URLs_200_tag_name_not_found(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    method,
    check_body,
):
    """
    Verifies that HEAD/GET requests with non-matching tag return no results.
    GET also validates response body is empty.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {"results": []}
    event = api_event_factory(
        method, "/service/webhooks", query_params={"tag.auth_classes": "dummy"}
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value

    if check_body:
        response_headers = response["multiValueHeaders"]
        response_body = json.loads(response["body"])
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert len(response_body) == 0


@pytest.mark.parametrize(
    "method,check_body",
    [
        ("HEAD", False),
        ("GET", True),
    ],
)
# pylint: disable=redefined-outer-name
def test_List_Webhook_URLs_200_tag_exists_name_true(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    webhook_ids,
    stub_webhook_tags,
    method,
    check_body,
):
    """
    Verifies that HEAD/GET requests with tag_exists=true return webhooks with that tag.
    GET also validates response body contains webhooks with the tag.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "webhook": {
                    **stub_webhook_tags,
                    "tags": {
                        k: json.dumps(v) for k, v in stub_webhook_tags["tags"].items()
                    },
                    "id": webhook_ids[1],
                    "status": "created",
                }
            }
        ]
    }
    event = api_event_factory(
        method, "/service/webhooks", query_params={"tag_exists.auth_classes": "true"}
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value

    if check_body:
        response_headers = response["multiValueHeaders"]
        response_body = json.loads(response["body"])
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert len(response_body) == 1
        assert {
            **stub_webhook_tags,
            "id": webhook_ids[1],
            "status": "created",
        } in response_body


@pytest.mark.parametrize(
    "method,check_body",
    [
        ("HEAD", False),
        ("GET", True),
    ],
)
# pylint: disable=redefined-outer-name
def test_List_Webhook_URLs_200_tag_exists_name_false(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    webhook_ids,
    stub_webhook_basic,
    method,
    check_body,
):
    """
    Verifies that HEAD/GET requests with tag_exists=false return webhooks without that tag.
    GET also validates response body contains webhooks without the tag.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "webhook": {
                    **stub_webhook_basic,
                    "id": webhook_ids[0],
                    "status": "created",
                }
            },
            {
                "webhook": {
                    **stub_webhook_basic,
                    "id": webhook_ids[2],
                    "status": "created",
                }
            },
        ]
    }
    event = api_event_factory(
        method, "/service/webhooks", query_params={"tag_exists.auth_classes": "false"}
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value

    if check_body:
        response_headers = response["multiValueHeaders"]
        response_body = json.loads(response["body"])
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert len(response_body) == 2
        assert {
            **stub_webhook_basic,
            "id": webhook_ids[0],
            "status": "created",
        } in response_body


@pytest.mark.parametrize(
    "method",
    ["HEAD", "GET"],
)
# pylint: disable=redefined-outer-name
def test_List_Webhook_URLs_400_tag_exists_name_bad(
    lambda_context, api_event_factory, api_service, method
):
    """
    Verifies that HEAD/GET requests with invalid tag_exists parameter return 400 Bad Request.
    """
    # Arrange
    event = api_event_factory(
        method, "/service/webhooks", query_params={"tag_exists.auth_classes": "bad"}
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert isinstance(response_body["message"], list)
    assert len(response_body["message"]) > 0
    assert response_body["message"][0]["type"] == "bool_parsing"


@pytest.mark.parametrize(
    "method,check_body",
    [
        ("HEAD", False),
        ("GET", True),
    ],
)
# pylint: disable=redefined-outer-name
def test_List_Webhook_URLs_200_limit(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    webhook_ids,
    stub_webhook_basic,
    method,
    check_body,
):
    """
    Verifies that HEAD/GET requests with limit parameter return paginated results.
    GET also validates response body and pagination headers.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "webhook": {
                    **stub_webhook_basic,
                    "id": webhook_ids[0],
                    "status": "created",
                }
            }
        ]
    }
    event = api_event_factory(method, "/service/webhooks", query_params={"limit": "1"})

    # Act
    response = api_service.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value

    if check_body:
        response_headers = response["multiValueHeaders"]
        response_body = json.loads(response["body"])
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert "Link" in response_headers
        assert "X-Paging-Limit" in response_headers
        assert "X-Paging-NextKey" in response_headers
        assert len(response_body) == 1


@pytest.mark.parametrize(
    "method",
    ["HEAD", "GET"],
)
# pylint: disable=redefined-outer-name
def test_List_Webhook_URLs_400_limit_bad(
    lambda_context, api_event_factory, api_service, method
):
    """
    Verifies that HEAD/GET requests with invalid limit parameter return 400 Bad Request.
    """
    # Arrange
    event = api_event_factory(method, "/service/webhooks", query_params={"limit": "a"})

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert isinstance(response_body["message"], list)
    assert len(response_body["message"]) > 0
    assert response_body["message"][0]["type"] == "int_parsing"


@pytest.mark.parametrize(
    "method,check_body",
    [
        ("HEAD", False),
        ("GET", True),
    ],
)
# pylint: disable=redefined-outer-name
def test_List_Webhook_URLs_200_page(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    webhook_ids,
    stub_webhook_basic,
    stub_webhook_tags,
    method,
    check_body,
):
    """
    Verifies that HEAD/GET requests with page parameter return results.
    GET also validates response body contains paginated results.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "webhook": {
                    **stub_webhook_basic,
                    "id": webhook_ids[0],
                    "status": "created",
                }
            },
            {
                "webhook": {
                    **stub_webhook_tags,
                    "tags": {
                        k: json.dumps(v) for k, v in stub_webhook_tags["tags"].items()
                    },
                    "id": webhook_ids[1],
                    "status": "created",
                }
            },
        ]
    }
    event = api_event_factory(method, "/service/webhooks", query_params={"page": "1"})

    # Act
    response = api_service.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value

    if check_body:
        response_headers = response["multiValueHeaders"]
        response_body = json.loads(response["body"])
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert len(response_body) == 2


@pytest.mark.parametrize(
    "method,check_body",
    [
        ("HEAD", False),
        ("GET", True),
    ],
)
# pylint: disable=redefined-outer-name
def test_Webhook_Details_200(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    webhook_ids,
    stub_webhook_basic,
    method,
    check_body,
):
    """
    Verifies that HEAD/GET requests for webhook details return 200 OK.
    GET also validates response body contains webhook data.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [
            {
                "webhook": {
                    **stub_webhook_basic,
                    "id": webhook_ids[0],
                    "status": "created",
                }
            }
        ]
    }
    event = api_event_factory(method, f"/service/webhooks/{webhook_ids[0]}")

    # Act
    response = api_service.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value

    if check_body:
        response_headers = response["multiValueHeaders"]
        response_body = json.loads(response["body"])
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert {
            **stub_webhook_basic,
            "id": webhook_ids[0],
            "status": "created",
        } == response_body
    else:
        response_headers = response["multiValueHeaders"]
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert response["body"] == "null"


@pytest.mark.parametrize(
    "method",
    ["HEAD", "GET"],
)
# pylint: disable=redefined-outer-name
def test_Webhook_Details_400(lambda_context, api_event_factory, api_service, method):
    """
    Verifies that HEAD/GET requests with invalid webhook ID return 400 Bad Request.
    """
    # Arrange
    event = api_event_factory(method, "/service/webhooks/bad-id")

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert isinstance(response_body["message"], list)
    assert len(response_body["message"]) > 0
    assert response_body["message"][0]["type"] == "string_pattern_mismatch"


@pytest.mark.parametrize(
    "method",
    ["HEAD", "GET"],
)
# pylint: disable=redefined-outer-name
def test_Webhook_Details_404(
    lambda_context, api_event_factory, api_service, mock_neptune_client, id_404, method
):
    """
    Verifies that HEAD/GET requests for non-existent webhook return 404 Not Found.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {"results": []}
    event = api_event_factory(method, f"/service/webhooks/{id_404}")

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.NOT_FOUND.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert (
        response_body.get("message")
        == "The requested Webhook ID in the path is invalid."
    )


# pylint: disable=redefined-outer-name
def test_Register_Webhook_URL_PUT_201_update(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    webhook_ids,
    stub_webhook_basic,
):
    """
    Verifies that a PUT request updates a webhook and returns 201 Created.
    """
    # Arrange
    webhook = {
        **stub_webhook_basic,
        "events": ["flows/created", "flows/updated"],
        "id": webhook_ids[0],
        "status": "created",
    }

    # Mock Neptune to return the updated webhook
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [{"webhook": webhook}]
    }

    event = api_event_factory(
        "PUT",
        f"/service/webhooks/{webhook_ids[0]}",
        json_body={**webhook, "api_key_value": "Bearer dummytokenvalue"},
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.CREATED.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert webhook == response_body


# pylint: disable=redefined-outer-name
def test_Register_Webhook_URL_PUT_400_update(
    lambda_context, api_event_factory, api_service, stub_webhook_basic
):
    """
    Verifies that a PUT request with invalid webhook ID returns 400 Bad Request.
    """
    # Arrange
    webhook = {**stub_webhook_basic, "id": "bad-id"}
    event = api_event_factory("PUT", "/service/webhooks/bad-id", json_body=webhook)

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert isinstance(response_body["message"], list)
    assert len(response_body["message"]) > 0
    assert response_body["message"][0]["type"] == "string_pattern_mismatch"


# pylint: disable=redefined-outer-name
def test_Register_Webhook_URL_PUT_404_update(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    id_404,
    stub_webhook_basic,
):
    """
    Verifies that a PUT request for non-existent webhook returns 404 Not Found.
    """
    # Arrange
    mock_neptune_client.execute_open_cypher_query.return_value = {"results": []}
    webhook = {**stub_webhook_basic, "id": id_404, "status": "created"}
    event = api_event_factory("PUT", f"/service/webhooks/{id_404}", json_body=webhook)

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.NOT_FOUND.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert (
        "The requested Webhook ID in the path is invalid." == response_body["message"]
    )


# pylint: disable=redefined-outer-name
def test_Register_Webhook_URL_PUT_400_update_missing_fields(
    lambda_context, api_event_factory, api_service, webhook_ids
):
    """
    Verifies that a PUT request with missing required fields returns 400 Bad Request.
    """
    # Arrange
    missing_fields = {"url", "id", "events", "status"}
    event = api_event_factory(
        "PUT", f"/service/webhooks/{webhook_ids[0]}", json_body={}
    )

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert isinstance(response_body["message"], list)
    assert len(missing_fields) == len(response_body["message"])
    assert ["missing"] * len(missing_fields) == [
        msg["type"] for msg in response_body["message"]
    ]
    assert missing_fields == set(msg["loc"][1] for msg in response_body["message"])


# pylint: disable=redefined-outer-name
def test_Register_Webhook_URL_DELETE_204(
    lambda_context,
    api_event_factory,
    api_service,
    mock_neptune_client,
    webhook_ids,
    stub_webhook_basic,
):
    """
    Verifies that a DELETE request deletes webhooks and returns 204 No Content.
    """
    # Arrange
    webhook = {
        **stub_webhook_basic,
        "id": webhook_ids[0],
        "status": "created",
    }
    mock_neptune_client.execute_open_cypher_query.return_value = {
        "results": [{"webhook": webhook}]
    }
    event = api_event_factory("DELETE", f"/service/webhooks/{webhook_ids[0]}")

    # Act
    response = api_service.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.NO_CONTENT.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body is None


@pytest.mark.parametrize(
    "method,check_body",
    [
        ("HEAD", False),
        ("GET", True),
    ],
)
# pylint: disable=redefined-outer-name
def test_Service_StorageBackends(
    lambda_context, api_event_factory, api_service, method, check_body
):
    """
    Verifies that HEAD/GET requests for storage backends return 200 OK.
    GET also validates response body contains storage backend list.
    """
    # Arrange
    event = api_event_factory(method, "/service/storage-backends")

    # Act
    response = api_service.lambda_handler(event, lambda_context)

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value

    if check_body:
        response_headers = response["multiValueHeaders"]
        response_body = json.loads(response["body"])
        assert response_headers.get("Content-Type")[0] == "application/json"
        assert isinstance(response_body, list)
        assert len(response_body) == 2
        for backend in response_body:
            assert backend.get("id") is not None
            assert backend.get("label") is not None
            assert backend.get("provider") is not None
            assert backend.get("region") is not None
            assert backend.get("store_product") is not None
            assert backend.get("store_type") is not None
