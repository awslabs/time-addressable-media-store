import json
import uuid
from http import HTTPStatus

# pylint: disable=no-name-in-module
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
def test_object_id():
    """
    Provides a unique test object identifier for testing.

    Returns:
        str: A unique test object identifier with UUID
    """
    yield f"test-object-{str(uuid.uuid4())}"


@pytest.fixture(scope="module")
def sample_timerange():
    """
    Provides a sample timerange for testing.

    Returns:
        dict: A dictionary containing string representation and end timestamp
    """
    yield {"str": "[0:0_6:0)", "end": 5999999999}


@pytest.fixture(scope="module")
def multiple_flow_ids():
    """
    Provides multiple unique flow IDs for testing.

    Returns:
        list: A list of UUID strings representing flow IDs
    """
    yield sorted([str(uuid.uuid4()) for _ in range(2)])


@pytest.fixture
def object_with_init_metadata(segments_table, storage_table):
    """Create a Media Object and init Object referenced by one Segment."""
    flow_id = str(uuid.uuid4())
    object_id = f"paged-object-{uuid.uuid4()}"
    init_object_id = f"paged-init-object-{uuid.uuid4()}"
    timerange = "[10:0_11:0)"
    timerange_end = 10_999_999_999
    media_get_url = {
        "label": "test-media",
        "url": "https://example.com/media-object",
    }
    init_get_url = {
        "label": "test-init",
        "url": "https://example.com/init-object",
    }
    segment = {
        "flow_id": flow_id,
        "timerange_end": timerange_end,
        "object_id": object_id,
        "init_object_id": init_object_id,
        "timerange": timerange,
        "object_timerange": timerange,
        "key_frame_count": 2,
        "get_urls": [media_get_url],
        "init_get_urls": [init_get_url],
    }
    segments_table.put_item(Item=segment)
    storage_table.put_item(Item={"id": object_id, "flow_id": flow_id})
    storage_table.put_item(
        Item={"id": init_object_id, "flow_id": flow_id, "is_init_object": True}
    )

    yield {
        "flow_id": flow_id,
        "object_id": object_id,
        "init_object_id": init_object_id,
        "timerange": timerange,
        "timerange_end": timerange_end,
        "media_get_url": media_get_url,
        "init_get_url": init_get_url,
    }

    segments_table.delete_item(Key={"flow_id": flow_id, "timerange_end": timerange_end})
    storage_table.delete_item(Key={"id": object_id})
    storage_table.delete_item(Key={"id": init_object_id})


@pytest.fixture(scope="module", autouse=True)
# pylint: disable=redefined-outer-name
def aws_setup(
    s3_bucket,
    segments_table,
    storage_table,
    test_object_id,
    sample_timerange,
    multiple_flow_ids,
):
    """
    Sets up test data in AWS resources before tests run.

    Creates test objects in S3 and entries in DynamoDB tables for testing.

    Args:
        s3_bucket: The S3 bucket fixture
        segments_table: The DynamoDB segments table fixture
        storage_table: The DynamoDB storage table fixture
        test_object_id: The test object ID fixture
        sample_timerange: The sample timerange fixture
        multiple_flow_ids: The multiple flow IDs fixture
    """
    s3_bucket.put_object(Key=test_object_id, Body="test content")
    for flow_id in multiple_flow_ids:
        segments_table.put_item(
            Item={
                "flow_id": flow_id,
                "timerange_end": sample_timerange["end"],
                "object_id": test_object_id,
                "timerange": sample_timerange["str"],
            }
        )
    storage_table.put_item(
        Item={
            "id": test_object_id,
            "flow_id": multiple_flow_ids[0],
            "expire_at": None,
        }
    )
    yield
    s3_bucket.delete_objects(Delete={"Objects": [{"Key": test_object_id}]})


@pytest.fixture(scope="module")
def api_objects():
    """
    Import api_objects Lambda handler after moto is active.

    Returns:
        module: The api_objects Lambda handler module
    """
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
# pylint: disable=redefined-outer-name
def invalid_page_value(request, test_object_id):
    """
    Provides different types of invalid pagination token values for testing error handling.

    Parameterized to test:
    - Non-base64 encoded tokens
    - Base64 encoded non-JSON content
    - JSON missing required fields

    Args:
        request: pytest request object containing the parameter
        test_object_id: The object ID to use in the token when applicable

    Returns:
        str: An invalid pagination token
    """
    param_type = request.param

    if param_type == "invalid_token":
        return "invalid_token"
    elif param_type == "non_json":
        return create_pagination_token("invalid token")
    elif param_type == "missing_fields":
        return create_pagination_token({"id": test_object_id})


#########
# TESTS #
#########


# pylint: disable=redefined-outer-name
def test_GET_object_returns_404_when_object_id_does_not_exist(
    lambda_context, api_event_factory, api_objects
):
    """
    Verifies that a GET request for a non-existent object returns 404 Not Found
    with an appropriate error message.
    """
    # Arrange
    event = api_event_factory("GET", "/objects/nonexistent-object")

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.NOT_FOUND.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("message") == "The requested Object does not exist."


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("page_key", ["object_id", "init_object_id"])
def test_object_returns_404_when_page_targets_nonexistent_object(
    lambda_context, api_event_factory, api_objects, method, page_key
):
    """A valid-looking cursor must not make a nonexistent Object appear."""
    object_id = f"nonexistent-object-{uuid.uuid4()}"
    page = create_pagination_token(
        {
            "flow_id": str(uuid.uuid4()),
            "timerange_end": 0,
            page_key: object_id,
        }
    )
    event = api_event_factory(
        method,
        f"/objects/{object_id}",
        query_params={"page": page},
    )

    response = api_objects.lambda_handler(event, lambda_context)

    assert response["statusCode"] == HTTPStatus.NOT_FOUND.value


@pytest.mark.parametrize(
    ("object_key", "page_key"),
    [
        ("object_id", "init_object_id"),
        ("init_object_id", "object_id"),
    ],
)
def test_GET_object_rejects_page_for_wrong_object_index(
    lambda_context,
    api_event_factory,
    object_with_init_metadata,
    api_objects,
    object_key,
    page_key,
):
    """A media cursor cannot page an init Object, or vice versa."""
    object_data = object_with_init_metadata
    object_id = object_data[object_key]
    page = create_pagination_token(
        {
            "flow_id": object_data["flow_id"],
            "timerange_end": object_data["timerange_end"],
            page_key: object_id,
        }
    )
    event = api_event_factory(
        "GET",
        f"/objects/{object_id}",
        query_params={"page": page},
    )

    response = api_objects.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_body.get("message") == "Invalid page parameter value"


# pylint: disable=redefined-outer-name
def test_GET_object_returns_200_with_complete_flow_references_when_object_exists(
    lambda_context, api_event_factory, test_object_id, multiple_flow_ids, api_objects
):
    """
    Verifies that a GET request for an existing object returns 200 OK
    with complete flow references and correct object metadata.
    """
    # Arrange
    event = api_event_factory("GET", f"/objects/{test_object_id}")

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("id") == test_object_id
    assert set(response_body.get("referenced_by_flows")) == set(multiple_flow_ids)
    assert response_body.get("first_referenced_by_flow") == multiple_flow_ids[0]


# pylint: disable=redefined-outer-name
def test_GET_object_decodes_percent_encoded_object_id(
    lambda_context,
    api_event_factory,
    segments_table,
    sample_timerange,
    multiple_flow_ids,
    api_objects,
):
    """
    Verifies that an Object ID containing "/" and a literal "%2F" is found when
    requested percent-encoded, as API Gateway passes the path undecoded.
    """
    # Arrange
    object_id = f"encoded/{uuid.uuid4()}/literal%2F.ts"
    segments_table.put_item(
        Item={
            "flow_id": multiple_flow_ids[0],
            "timerange_end": sample_timerange["end"] + 1_000_000_000,
            "object_id": object_id,
            "timerange": "[6:0_7:0)",
        }
    )
    encoded_object_id = object_id.replace("%", "%25").replace("/", "%2F")
    event = api_event_factory("GET", f"/objects/{encoded_object_id}")

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_body.get("id") == object_id
    assert response_body.get("referenced_by_flows") == [multiple_flow_ids[0]]


# pylint: disable=redefined-outer-name
def test_GET_object_returns_limited_flow_references_with_pagination_headers_when_limit_specified(
    lambda_context, api_event_factory, test_object_id, multiple_flow_ids, api_objects
):
    """
    Verifies that a GET request with a limit parameter returns:
    1. The correct number of flow references (limited to the specified value)
    2. Appropriate pagination headers (Link and X-Paging-NextKey)
    3. The expected object metadata
    """
    # Arrange
    event = api_event_factory(
        "GET", f"/objects/{test_object_id}", query_params={"limit": "1"}
    )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert len(response_headers.get("Link")) == 1
    assert len(response_headers.get("X-Paging-NextKey")) == 1
    assert response_body.get("id") == test_object_id
    assert len(response_body.get("referenced_by_flows")) == 1
    assert response_body.get("referenced_by_flows")[0] == multiple_flow_ids[0]
    assert response_body.get("first_referenced_by_flow") == multiple_flow_ids[0]


# pylint: disable=redefined-outer-name
def test_GET_object_returns_empty_flow_references_on_page_past_the_last(
    lambda_context,
    api_event_factory,
    object_with_init_metadata,
    api_objects,
):
    """
    Verifies that a page beyond the last flow reference retains the Media
    Object's global metadata while returning an empty references list.
    """
    object_data = object_with_init_metadata
    # Arrange
    event = api_event_factory(
        "GET",
        f"/objects/{object_data['object_id']}",
        query_params={
            "limit": "1",
            "page": create_pagination_token(
                {
                    "flow_id": object_data["flow_id"],
                    "timerange_end": object_data["timerange_end"],
                    "object_id": object_data["object_id"],
                }
            ),
        },
    )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_body.get("id") == object_data["object_id"]
    assert response_body.get("referenced_by_flows") == []
    assert response_body.get("first_referenced_by_flow") == object_data["flow_id"]
    assert response_body.get("timerange") == object_data["timerange"]
    assert response_body.get("key_frame_count") == 2
    assert response_body.get("get_urls") == [object_data["media_get_url"]]
    assert response_body.get("init_object") == {
        "id": object_data["init_object_id"],
        "get_urls": [object_data["init_get_url"]],
    }


def test_GET_init_object_retains_metadata_on_page_past_the_last(
    lambda_context,
    api_event_factory,
    object_with_init_metadata,
    api_objects,
):
    """An init Object's global metadata also survives an empty final page."""
    object_data = object_with_init_metadata
    event = api_event_factory(
        "GET",
        f"/objects/{object_data['init_object_id']}",
        query_params={
            "limit": "1",
            "page": create_pagination_token(
                {
                    "flow_id": object_data["flow_id"],
                    "timerange_end": object_data["timerange_end"],
                    "init_object_id": object_data["init_object_id"],
                }
            ),
        },
    )

    response = api_objects.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_body.get("id") == object_data["init_object_id"]
    assert response_body.get("referenced_by_flows") == []
    assert response_body.get("first_referenced_by_flow") == object_data["flow_id"]
    assert response_body.get("get_urls") == [object_data["init_get_url"]]
    assert "timerange" not in response_body
    assert "key_frame_count" not in response_body
    assert "init_object" not in response_body


# pylint: disable=redefined-outer-name
def test_GET_object_returns_next_page_of_flow_references_when_pagination_token_provided(
    lambda_context,
    api_event_factory,
    test_object_id,
    sample_timerange,
    multiple_flow_ids,
    api_objects,
):
    """
    Verifies that a GET request with pagination token returns the next page of results:
    1. Returns the correct flow references from the next page
    2. Does not include pagination headers when at the end of results
    3. Maintains consistent object metadata across pages
    """
    # Arrange
    event = api_event_factory(
        "GET",
        f"/objects/{test_object_id}",
        query_params={
            "limit": "1",
            "page": create_pagination_token(
                {
                    "flow_id": multiple_flow_ids[0],
                    "timerange_end": sample_timerange["end"],
                    "object_id": test_object_id,
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
    assert response_headers.get("X-Paging-Limit")[0] == "1"
    assert response_body.get("id") == test_object_id
    assert len(response_body.get("referenced_by_flows")) == 1
    assert response_body.get("referenced_by_flows")[0] == multiple_flow_ids[1]
    assert response_body.get("first_referenced_by_flow") == multiple_flow_ids[0]


def test_HEAD_object_returns_200_with_empty_body_when_object_exists(
    lambda_context, api_event_factory, test_object_id, api_objects
):
    """
    Verifies that a HEAD request for an existing object returns 200 OK
    with appropriate headers but an empty response body.
    """
    # Arrange
    event = api_event_factory("HEAD", f"/objects/{test_object_id}")

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.OK.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body is None


def test_GET_object_handles_maximum_limit_parameter(
    lambda_context, api_event_factory, test_object_id, api_objects
):
    """
    Verifies that a GET request with a limit parameter exceeding the maximum allowed value
    is handled correctly by capping the limit at the maximum allowed value.
    """
    # Arrange
    event = api_event_factory(
        "GET", f"/objects/{test_object_id}", query_params={"limit": "1000"}
    )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]

    # Assert
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
    test_object_id,
    api_objects,
    limit_value,
    expected_message,
):
    """
    Verifies that a GET request with an invalid limit parameter returns 400 Bad Request
    with appropriate validation error messages.

    Tests various invalid limit scenarios:
    - Non-numeric limit values
    - Negative limit values
    """
    # Arrange
    event = api_event_factory(
        "GET", f"/objects/{test_object_id}", query_params={"limit": limit_value}
    )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_headers = response["multiValueHeaders"]
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_headers.get("Content-Type")[0] == "application/json"
    assert response_body.get("message")[0]["msg"] == expected_message


def test_GET_object_handles_invalid_page_parameter(
    lambda_context, api_event_factory, test_object_id, api_objects, invalid_page_value
):
    """
    Verifies that a GET request with an invalid page parameter returns 400 Bad Request
    with an appropriate error message.

    Tests multiple types of invalid pagination tokens through the parameterized fixture.
    """
    # Arrange
    event = api_event_factory(
        "GET", f"/objects/{test_object_id}", query_params={"page": invalid_page_value}
    )

    # Act
    response = api_objects.lambda_handler(event, lambda_context)
    response_body = json.loads(response["body"])

    # Assert
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    assert response_body.get("message") == "Invalid page parameter value"
