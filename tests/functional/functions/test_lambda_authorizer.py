import json
from unittest.mock import patch

import jwt
import pytest
import requests

pytestmark = [
    pytest.mark.functional,
]

############
# FIXTURES #
############


@pytest.fixture(scope="module")
def lambda_authorizer():
    """
    Import lambda_authorizer Lambda handler after moto is active.

    Returns:
        module: The api_objects Lambda handler module
    """
    # pylint: disable=import-outside-toplevel
    from lambda_authorizer import app

    return app


#########
# TESTS #
#########


# pylint: disable=redefined-outer-name
def test_no_authorization_header(lambda_context, auth_event_factory, lambda_authorizer):
    """
    Test 401 is returned when no Authorization Header is provided.
    """
    # Arrange
    event = auth_event_factory("GET", "/", "/")

    # Act & Assert
    with pytest.raises(PermissionError, match="Unauthorized"):
        lambda_authorizer.lambda_handler(event, lambda_context)


def test_malformed_authorization_header(
    lambda_context, auth_event_factory, lambda_authorizer
):
    """
    Test 401 is returned when Authorization Header doesn't start with 'Bearer '.
    """
    # Arrange
    event = auth_event_factory(
        "GET", "/", "/", {"Authorization": "InvalidFormat token123"}
    )

    # Act & Assert
    with pytest.raises(PermissionError, match="Unauthorized"):
        lambda_authorizer.lambda_handler(event, lambda_context)


def test_bad_bearer_token(lambda_context, auth_event_factory, lambda_authorizer):
    """
    Test 401 is returned when Bearer token cannot be decoded.
    """
    # Arrange
    event = auth_event_factory("GET", "/", "/", {"Authorization": "Bearer bad_token"})

    # Act & Assert
    with pytest.raises(PermissionError, match="Unauthorized"):
        lambda_authorizer.lambda_handler(event, lambda_context)


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.decode")
def test_invalid_issuer(
    mock_decode, lambda_context, auth_event_factory, lambda_authorizer
):
    """
    Test 401 is returned when Issuer is not in allowed list.
    """
    # Arrange
    event = auth_event_factory("GET", "/", "/", {"Authorization": "Bearer valid_token"})
    mock_decode.return_value = {"iss": "https://wrong-issuer.com"}

    # Act & Assert
    with pytest.raises(PermissionError, match="Unauthorized"):
        lambda_authorizer.lambda_handler(event, lambda_context)


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.decode")
@patch("lambda_authorizer.app.requests.get")
def test_jwks_fetch_fails(
    mock_get, mock_decode, lambda_context, auth_event_factory, lambda_authorizer
):
    """
    Test 401 is returned when jwks fetch fails.
    """
    # Arrange
    event = auth_event_factory(
        "GET", "/", "/", {"Authorization": "Bearer valid_format_token"}
    )
    mock_decode.return_value = {"iss": "https://allowed-issuer.com"}
    mock_get.side_effect = requests.exceptions.RequestException("Connection failed")

    # Act & Assert
    with pytest.raises(PermissionError, match="Unauthorized"):
        lambda_authorizer.lambda_handler(event, lambda_context)


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.decode")
@patch("lambda_authorizer.app.jwt.get_unverified_header")
@patch("lambda_authorizer.app.get_jwks")
def test_missing_kid_in_jwks(
    mock_jwks,
    mock_header,
    mock_decode,
    lambda_context,
    auth_event_factory,
    lambda_authorizer,
):
    """
    Test 401 when kid from token not found in JWKS.
    """
    # Arrange
    event = auth_event_factory("GET", "/", "/", {"Authorization": "Bearer valid_token"})
    mock_decode.return_value = {"iss": "https://allowed-issuer.com"}
    mock_header.return_value = {"kid": "unknown_key", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "different_key"}]}

    # Act & Assert
    with pytest.raises(PermissionError, match="Unauthorized"):
        lambda_authorizer.lambda_handler(event, lambda_context)


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.decode")
@patch("lambda_authorizer.app.jwt.get_unverified_header")
@patch("lambda_authorizer.app.get_jwks")
def test_missing_algorithm_in_header(
    mock_jwks,
    mock_header,
    mock_decode,
    lambda_context,
    auth_event_factory,
    lambda_authorizer,
):
    """
    Test 401 when algorithm not specified in token header.
    """
    # Arrange
    event = auth_event_factory("GET", "/", "/", {"Authorization": "Bearer valid_token"})
    mock_decode.return_value = {"iss": "https://allowed-issuer.com"}
    mock_header.return_value = {"kid": "key123"}  # No 'alg' field
    mock_jwks.return_value = {"keys": [{"kid": "key123"}]}

    # Act & Assert
    with pytest.raises(PermissionError, match="Unauthorized"):
        lambda_authorizer.lambda_handler(event, lambda_context)


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.PyJWK")
@patch("lambda_authorizer.app.get_jwks")
@patch("lambda_authorizer.app.jwt.decode")
@patch("lambda_authorizer.app.jwt.get_unverified_header")
def test_invalid_signature(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
    lambda_authorizer,
):
    """
    Test 401 when token signature verification fails.
    """
    # Arrange
    event = auth_event_factory("GET", "/", "/", {"Authorization": "Bearer valid_token"})
    mock_decode.side_effect = [
        {"iss": "https://allowed-issuer.com"},
        jwt.InvalidSignatureError("Signature verification failed"),
    ]
    mock_header.return_value = {"kid": "key123", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key123"}]}
    mock_pyjwk.return_value.key = "mock_key"

    # Act & Assert
    with pytest.raises(PermissionError, match="Unauthorized"):
        lambda_authorizer.lambda_handler(event, lambda_context)


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.PyJWK")
@patch("lambda_authorizer.app.get_jwks")
@patch("lambda_authorizer.app.jwt.decode")
@patch("lambda_authorizer.app.jwt.get_unverified_header")
def test_expired_token(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
    lambda_authorizer,
):
    """
    Test 401 when token has expired.
    """
    # Arrange
    event = auth_event_factory(
        "GET", "/", "/", {"Authorization": "Bearer expired_token"}
    )
    mock_decode.side_effect = [
        {"iss": "https://allowed-issuer.com"},
        jwt.ExpiredSignatureError("Token has expired"),
    ]
    mock_header.return_value = {"kid": "key123", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key123"}]}
    mock_pyjwk.return_value.key = "mock_key"

    # Act & Assert
    with pytest.raises(PermissionError, match="Unauthorized"):
        lambda_authorizer.lambda_handler(event, lambda_context)


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.decode")
def test_token_missing_issuer(
    mock_decode, lambda_context, auth_event_factory, lambda_authorizer
):
    """
    Test 401 when token doesn't contain issuer claim.
    """
    # Arrange
    event = auth_event_factory("GET", "/", "/", {"Authorization": "Bearer valid_token"})
    mock_decode.return_value = {"sub": "user123"}  # No 'iss' field

    # Act & Assert
    with pytest.raises(PermissionError, match="Unauthorized"):
        lambda_authorizer.lambda_handler(event, lambda_context)


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.PyJWK")
@patch("lambda_authorizer.app.get_jwks")
@patch("lambda_authorizer.app.jwt.decode")
@patch("lambda_authorizer.app.jwt.get_unverified_header")
def test_valid_token_success(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
    lambda_authorizer,
):
    # Arrange
    event = auth_event_factory("GET", "/", "/", {"Authorization": "Bearer valid_token"})
    mock_decode.side_effect = [
        {
            "iss": "https://allowed-issuer.com",
            "sub": "user123",
        },  # First call (unverified)
        {
            "iss": "https://allowed-issuer.com",
            "sub": "user123",
            "scope": "tams-api/read",
        },  # Second call (verified)
    ]
    mock_header.return_value = {"kid": "key123", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key123"}]}
    mock_pyjwk.return_value.key = "mock_public_key"  # Mock the RSA key

    # Act
    response = lambda_authorizer.lambda_handler(event, lambda_context)

    # Assert
    assert response["principalId"] == "user123"
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Allow"


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.PyJWK")
@patch("lambda_authorizer.app.get_jwks")
@patch("lambda_authorizer.app.jwt.decode")
@patch("lambda_authorizer.app.jwt.get_unverified_header")
def test_deny_when_no_scopes_supplied(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
    lambda_authorizer,
):
    """
    Test 403 when token has no scopes
    """
    # Arrange
    event = auth_event_factory("GET", "/", "/", {"Authorization": "Bearer valid_token"})
    mock_decode.side_effect = [
        {"iss": "https://allowed-issuer.com", "sub": "user123"},
        {"iss": "https://allowed-issuer.com", "sub": "user123"},
    ]
    mock_header.return_value = {"kid": "key123", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key123"}]}
    mock_pyjwk.return_value.key = "mock_public_key"

    # Act
    response = lambda_authorizer.lambda_handler(event, lambda_context)

    # Assert
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Deny"


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.PyJWK")
@patch("lambda_authorizer.app.get_jwks")
@patch("lambda_authorizer.app.jwt.decode")
@patch("lambda_authorizer.app.jwt.get_unverified_header")
def test_deny_when_scopes_dont_match_required(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
    lambda_authorizer,
):
    """
    Test 403 when supplied scopes don't match required scopes
    """
    # Arrange
    event = auth_event_factory("GET", "/", "/", {"Authorization": "Bearer valid_token"})
    mock_decode.side_effect = [
        {"iss": "https://allowed-issuer.com", "sub": "user123"},
        {"iss": "https://allowed-issuer.com", "sub": "user123", "scope": "wrong-scope"},
    ]
    mock_header.return_value = {"kid": "key123", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key123"}]}
    mock_pyjwk.return_value.key = "mock_public_key"

    # Act
    response = lambda_authorizer.lambda_handler(event, lambda_context)

    # Assert
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Deny"


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.PyJWK")
@patch("lambda_authorizer.app.get_jwks")
@patch("lambda_authorizer.app.jwt.decode")
@patch("lambda_authorizer.app.jwt.get_unverified_header")
def test_context_includes_username_from_sub(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
    lambda_authorizer,
):
    """
    Test username falls back to sub for non-Cognito issuer
    """
    # Arrange
    event = auth_event_factory("GET", "/", "/", {"Authorization": "Bearer valid_token"})
    mock_decode.side_effect = [
        {"iss": "https://allowed-issuer.com", "sub": "user123"},
        {
            "iss": "https://allowed-issuer.com",
            "sub": "user123",
            "scope": "tams-api/read",
        },
    ]
    mock_header.return_value = {"kid": "key123", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key123"}]}
    mock_pyjwk.return_value.key = "mock_public_key"

    # Act
    response = lambda_authorizer.lambda_handler(event, lambda_context)

    # Assert
    assert response["context"]["username"] == "user123"


@patch.dict(
    "os.environ",
    {"ALLOWED_ISSUERS": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_ABC123"},
)
@patch("lambda_authorizer.app.jwt.PyJWK")
@patch("lambda_authorizer.app.get_jwks")
@patch("lambda_authorizer.app.jwt.decode")
@patch("lambda_authorizer.app.jwt.get_unverified_header")
@patch("lambda_authorizer.app.get_user_pool")
@patch("lambda_authorizer.app.idp.admin_get_user")
def test_context_username_from_cognito_email(
    mock_admin_get,
    mock_pool,
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
    lambda_authorizer,
):
    """
    Test username returns email for Cognito user
    """
    # Arrange
    event = auth_event_factory(
        "POST", "/service", "/service", {"Authorization": "Bearer valid_token"}
    )
    mock_decode.side_effect = [
        {
            "iss": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_ABC123",
            "sub": "user123",
        },
        {
            "iss": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_ABC123",
            "sub": "user123",
            "username": "testuser",
            "scope": "tams-api/admin",
        },
    ]
    mock_header.return_value = {"kid": "key123", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key123"}]}
    mock_pyjwk.return_value.key = "mock_public_key"
    mock_pool.return_value = {"UsernameAttributes": ["email"]}
    mock_admin_get.return_value = {
        "UserAttributes": [{"Name": "email", "Value": "user@example.com"}]
    }

    # Act
    response = lambda_authorizer.lambda_handler(event, lambda_context)

    # Assert
    assert response["context"]["username"] == "user@example.com"


@patch.dict("os.environ", {"ALLOWED_ISSUERS": "https://allowed-issuer.com"})
@patch("lambda_authorizer.app.jwt.PyJWK")
@patch("lambda_authorizer.app.get_jwks")
@patch("lambda_authorizer.app.jwt.decode")
@patch("lambda_authorizer.app.jwt.get_unverified_header")
def test_allow_route_with_wildcard_resource(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
    lambda_authorizer,
):
    """
    Test resource path with parameters converts to ARN format
    """
    # Arrange
    event = auth_event_factory(
        "GET", "/flows/{flowId}", "/flows/123", {"Authorization": "Bearer valid_token"}
    )
    mock_decode.side_effect = [
        {"iss": "https://allowed-issuer.com", "sub": "user123"},
        {
            "iss": "https://allowed-issuer.com",
            "sub": "user123",
            "scope": "tams-api/read",
        },
    ]
    mock_header.return_value = {"kid": "key123", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key123"}]}
    mock_pyjwk.return_value.key = "mock_public_key"

    # Act
    response = lambda_authorizer.lambda_handler(event, lambda_context)
    print("here", response)

    # Assert
    assert response["policyDocument"]["Statement"][0]["Resource"][0].endswith(
        "/flows/*"
    )
