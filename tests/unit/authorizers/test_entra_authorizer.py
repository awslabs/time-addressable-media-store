import json
from unittest.mock import MagicMock, patch

import jwt
import pytest
import requests
from entra_id import app as entra_authorizer


@pytest.fixture
def lambda_context():
    class Context:
        function_name = "test-entra-authorizer"
        memory_limit_in_mb = 1024
        invoked_function_arn = (
            "arn:aws:lambda:eu-west-1:123456789012:function:test-authorizer"
        )
        aws_request_id = "request-id-123"

        def get_remaining_time_in_millis(self):
            return 10000

    return Context()


@pytest.fixture
def auth_event_factory():
    def _factory(http_method: str, resource: str, path: str, headers: dict = None):
        event = {
            "type": "REQUEST",
            "methodArn": f"arn:aws:execute-api:eu-west-1:123456789012:abcde12345/Prod/{http_method}{path}",
            "resource": resource,
            "path": path,
            "httpMethod": http_method,
            "headers": headers or {},
        }
        return event

    return _factory


# -----------------------------------------------------------------------------
# Tests: Missing or Invalid Headers
# -----------------------------------------------------------------------------


def test_no_authorization_header(lambda_context, auth_event_factory):
    event = auth_event_factory("GET", "/flows", "/flows")
    with pytest.raises(PermissionError, match="Unauthorized"):
        entra_authorizer.lambda_handler(event, lambda_context)


def test_malformed_token(lambda_context, auth_event_factory):
    event = auth_event_factory(
        "GET", "/flows", "/flows", {"Authorization": "Bearer invalid.token.payload"}
    )
    with pytest.raises(PermissionError, match="Unauthorized"):
        entra_authorizer.lambda_handler(event, lambda_context)


# -----------------------------------------------------------------------------
# Tests: Delegated Tokens (User + App)
# -----------------------------------------------------------------------------


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.jwt.PyJWK")
@patch("entra_id.app.get_jwks")
@patch("entra_id.app.jwt.decode")
@patch("entra_id.app.jwt.get_unverified_header")
def test_delegated_token_success(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
):
    event = auth_event_factory(
        "GET", "/flows", "/flows", {"Authorization": "Bearer valid_delegated_token"}
    )
    mock_header.return_value = {"kid": "key-1", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key-1"}]}
    mock_pyjwk.return_value.key = "mock_rsa_key"

    unverified = {"iss": "https://login.microsoftonline.com/tenant-123/v2.0"}
    verified_claims = {
        "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
        "sub": "user-sub-abc",
        "oid": "user-oid-456",
        "preferred_username": "jane.doe@example.com",
        "scp": "tams-api.read",
        "tid": "tenant-123",
        "azp": "client-app-789",
        "groups": ["group-admin", "group-media"],
    }
    mock_decode.side_effect = [unverified, verified_claims]

    response = entra_authorizer.lambda_handler(event, lambda_context)

    assert response["principalId"] == "user-oid-456"
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Allow"
    assert response["context"]["username"] == "jane.doe@example.com"
    assert response["context"]["token_type"] == "delegated"
    assert "tams-api.read" in json.loads(response["context"]["scopes"])
    assert "group-admin" in json.loads(response["context"]["auth_classes"])


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.jwt.PyJWK")
@patch("entra_id.app.get_jwks")
@patch("entra_id.app.jwt.decode")
@patch("entra_id.app.jwt.get_unverified_header")
def test_delegated_token_upn_fallback(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
):
    event = auth_event_factory(
        "GET", "/flows", "/flows", {"Authorization": "Bearer token"}
    )
    mock_header.return_value = {"kid": "key-1", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key-1"}]}
    mock_pyjwk.return_value.key = "mock_key"

    mock_decode.side_effect = [
        {"iss": "https://login.microsoftonline.com/tenant-123/v2.0"},
        {
            "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
            "upn": "john.doe@company.onmicrosoft.com",
            "scp": "tams-api/read",
            "sub": "user-sub-123",
        },
    ]

    response = entra_authorizer.lambda_handler(event, lambda_context)
    assert response["context"]["username"] == "john.doe@company.onmicrosoft.com"
    assert response["context"]["token_type"] == "delegated"
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Allow"


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.jwt.PyJWK")
@patch("entra_id.app.get_jwks")
@patch("entra_id.app.jwt.decode")
@patch("entra_id.app.jwt.get_unverified_header")
def test_delegated_token_email_and_name_fallbacks(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
):
    event = auth_event_factory(
        "GET", "/flows", "/flows", {"Authorization": "Bearer token"}
    )
    mock_header.return_value = {"kid": "key-1", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key-1"}]}
    mock_pyjwk.return_value.key = "mock_key"

    mock_decode.side_effect = [
        {"iss": "https://login.microsoftonline.com/tenant-123/v2.0"},
        {
            "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
            "email": "user@example.com",
            "scope": "tams-api.read",
            "sub": "user-sub-email",
        },
    ]

    response = entra_authorizer.lambda_handler(event, lambda_context)
    assert response["context"]["username"] == "user@example.com"
    assert response["context"]["token_type"] == "delegated"


# -----------------------------------------------------------------------------
# Tests: App-Only Tokens (Machine-to-Machine / Client Credentials)
# -----------------------------------------------------------------------------


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.jwt.PyJWK")
@patch("entra_id.app.get_jwks")
@patch("entra_id.app.jwt.decode")
@patch("entra_id.app.jwt.get_unverified_header")
def test_app_only_token_with_roles_success(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
):
    # App-only token accessing POST /flows/{flowId}/storage (requires tams-api.write or admin)
    event = auth_event_factory(
        "POST",
        "/flows/{flowId}/storage",
        "/flows/flow-123/storage",
        {"Authorization": "Bearer valid_app_token"},
    )
    mock_header.return_value = {"kid": "key-1", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key-1"}]}
    mock_pyjwk.return_value.key = "mock_rsa_key"

    unverified = {"iss": "https://login.microsoftonline.com/tenant-123/v2.0"}
    verified_claims = {
        "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
        "idtyp": "app",
        "oid": "service-principal-oid-999",
        "sub": "service-principal-oid-999",
        "appid": "00000000-1111-2222-3333-444444444444",
        "azp": "00000000-1111-2222-3333-444444444444",
        "app_displayname": "TAMS-Ingest-Worker",
        "roles": ["tams-api.write"],
        "tid": "tenant-123",
    }
    mock_decode.side_effect = [unverified, verified_claims]

    response = entra_authorizer.lambda_handler(event, lambda_context)

    assert response["principalId"] == "service-principal-oid-999"
    assert response["context"]["username"] == "TAMS-Ingest-Worker"
    assert response["context"]["token_type"] == "app_only"
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Allow"
    assert "tams-api.write" in json.loads(response["context"]["scopes"])


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.jwt.PyJWK")
@patch("entra_id.app.get_jwks")
@patch("entra_id.app.jwt.decode")
@patch("entra_id.app.jwt.get_unverified_header")
def test_app_only_token_fallback_to_appid(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
):
    event = auth_event_factory(
        "GET", "/flows", "/flows", {"Authorization": "Bearer app_token"}
    )
    mock_header.return_value = {"kid": "key-1", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key-1"}]}
    mock_pyjwk.return_value.key = "mock_key"

    mock_decode.side_effect = [
        {"iss": "https://login.microsoftonline.com/tenant-123/v2.0"},
        {
            "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
            "appid": "app-guid-123",
            "roles": ["tams-api.read"],
            "sub": "sp-sub-123",
            "oid": "sp-oid-123",
        },
    ]

    response = entra_authorizer.lambda_handler(event, lambda_context)
    assert response["context"]["username"] == "app-guid-123"
    assert response["context"]["token_type"] == "app_only"
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Allow"


# -----------------------------------------------------------------------------
# Tests: Scope Normalization & Route Matching
# -----------------------------------------------------------------------------


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.jwt.PyJWK")
@patch("entra_id.app.get_jwks")
@patch("entra_id.app.jwt.decode")
@patch("entra_id.app.jwt.get_unverified_header")
def test_slash_formatted_scopes_match_dot_routes(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
):
    # Route /service requires "tams-api.admin" or "tams-api.read"
    event = auth_event_factory(
        "GET", "/service", "/service", {"Authorization": "Bearer token"}
    )
    mock_header.return_value = {"kid": "key-1", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key-1"}]}
    mock_pyjwk.return_value.key = "mock_key"

    mock_decode.side_effect = [
        {"iss": "https://login.microsoftonline.com/tenant-123/v2.0"},
        {
            "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
            "scp": "tams-api/read",
            "sub": "user-123",
        },
    ]

    response = entra_authorizer.lambda_handler(event, lambda_context)
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Allow"
    scopes = json.loads(response["context"]["scopes"])
    assert "tams-api.read" in scopes
    assert "tams-api/read" in scopes


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.jwt.PyJWK")
@patch("entra_id.app.get_jwks")
@patch("entra_id.app.jwt.decode")
@patch("entra_id.app.jwt.get_unverified_header")
def test_admin_scope_authorizes_all(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
):
    # Admin scope should allow DELETE /flows/{flowId}
    event = auth_event_factory(
        "DELETE",
        "/flows/{flowId}",
        "/flows/flow-999",
        {"Authorization": "Bearer admin_token"},
    )
    mock_header.return_value = {"kid": "key-1", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key-1"}]}
    mock_pyjwk.return_value.key = "mock_key"

    mock_decode.side_effect = [
        {"iss": "https://login.microsoftonline.com/tenant-123/v2.0"},
        {
            "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
            "roles": ["tams-api.admin"],
            "sub": "admin-sp",
        },
    ]

    response = entra_authorizer.lambda_handler(event, lambda_context)
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Allow"


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.jwt.PyJWK")
@patch("entra_id.app.get_jwks")
@patch("entra_id.app.jwt.decode")
@patch("entra_id.app.jwt.get_unverified_header")
def test_insufficient_scope_denies(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
):
    # Caller only has read scope, but POST /service requires tams-api.admin
    event = auth_event_factory(
        "POST", "/service", "/service", {"Authorization": "Bearer read_only_token"}
    )
    mock_header.return_value = {"kid": "key-1", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key-1"}]}
    mock_pyjwk.return_value.key = "mock_key"

    mock_decode.side_effect = [
        {"iss": "https://login.microsoftonline.com/tenant-123/v2.0"},
        {
            "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
            "scp": "tams-api.read",
            "sub": "user-123",
        },
    ]

    response = entra_authorizer.lambda_handler(event, lambda_context)
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Deny"


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.jwt.PyJWK")
@patch("entra_id.app.get_jwks")
@patch("entra_id.app.jwt.decode")
@patch("entra_id.app.jwt.get_unverified_header")
def test_unmapped_route_passthrough(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
):
    # An unmapped path in oauth_scopes.json should pass through to Lambda for 404
    event = auth_event_factory(
        "GET",
        "/unknown/resource",
        "/unknown/resource",
        {"Authorization": "Bearer token"},
    )
    mock_header.return_value = {"kid": "key-1", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key-1"}]}
    mock_pyjwk.return_value.key = "mock_key"

    mock_decode.side_effect = [
        {"iss": "https://login.microsoftonline.com/tenant-123/v2.0"},
        {
            "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
            "scp": "tams-api.read",
            "sub": "user-123",
        },
    ]

    response = entra_authorizer.lambda_handler(event, lambda_context)
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Allow"


# -----------------------------------------------------------------------------
# Tests: Audience & Issuer Validation
# -----------------------------------------------------------------------------


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.jwt.decode")
def test_invalid_issuer_rejected(
    mock_decode,
    lambda_context,
    auth_event_factory,
):
    event = auth_event_factory(
        "GET", "/flows", "/flows", {"Authorization": "Bearer token"}
    )
    mock_decode.return_value = {"iss": "https://evil-idp.com"}

    with pytest.raises(PermissionError, match="Unauthorized"):
        entra_authorizer.lambda_handler(event, lambda_context)


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.allowed_audiences", ["api://tams-api"])
@patch("entra_id.app.jwt.PyJWK")
@patch("entra_id.app.get_jwks")
@patch("entra_id.app.jwt.decode")
@patch("entra_id.app.jwt.get_unverified_header")
def test_audience_mismatch_rejected(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
):
    event = auth_event_factory(
        "GET", "/flows", "/flows", {"Authorization": "Bearer token"}
    )
    mock_header.return_value = {"kid": "key-1", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key-1"}]}
    mock_pyjwk.return_value.key = "mock_key"

    mock_decode.side_effect = [
        {
            "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
            "aud": "wrong-audience",
        },
        jwt.InvalidAudienceError("Audience doesn't match"),
    ]

    with pytest.raises(PermissionError, match="Unauthorized"):
        entra_authorizer.lambda_handler(event, lambda_context)


# -----------------------------------------------------------------------------
# Tests: Discovery & JWKS Caching
# -----------------------------------------------------------------------------


@patch("entra_id.app.requests.get")
def test_oidc_discovery_and_jwks_caching(mock_get):
    issuer = "https://login.microsoftonline.com/tenant-test/v2.0"
    entra_authorizer._oidc_config_cache.clear()
    entra_authorizer._jwks_cache.clear()

    # 1. Discovery response
    discovery_response = MagicMock()
    discovery_response.json.return_value = {"jwks_uri": f"{issuer}/discovery/v2.0/keys"}
    discovery_response.raise_for_status = MagicMock()

    # 2. JWKS response
    jwks_response = MagicMock()
    jwks_response.json.return_value = {"keys": [{"kid": "k1", "kty": "RSA"}]}
    jwks_response.raise_for_status = MagicMock()

    mock_get.side_effect = [discovery_response, jwks_response]

    # First call: fetches from network
    jwks1 = entra_authorizer.get_jwks(issuer)
    assert jwks1["keys"][0]["kid"] == "k1"
    assert mock_get.call_count == 2

    # Second call: uses in-memory cache
    jwks2 = entra_authorizer.get_jwks(issuer)
    assert jwks2 == jwks1
    assert mock_get.call_count == 2  # No new network requests


def test_normalize_scopes_with_app_and_all_suffixes():
    scopes = {
        "tams-api.read.app",
        "tams-api.write.app",
        "tams-api.admin.app",
        "tams-api.delete.app",
        "tams-api.read.all",
    }
    normalized = entra_authorizer.normalize_scopes(scopes)

    # Base forms are extracted and expanded
    assert "tams-api.read" in normalized
    assert "tams-api/read" in normalized
    assert "tams-api.write" in normalized
    assert "tams-api/write" in normalized
    assert "tams-api.admin" in normalized
    assert "tams-api/admin" in normalized
    assert "tams-api.delete" in normalized
    assert "tams-api/delete" in normalized
    # Original forms are preserved
    assert "tams-api.read.app" in normalized


@patch(
    "entra_id.app.allowed_issuers",
    ["https://login.microsoftonline.com/tenant-123/v2.0"],
)
@patch("entra_id.app.jwt.PyJWK")
@patch("entra_id.app.get_jwks")
@patch("entra_id.app.jwt.decode")
@patch("entra_id.app.jwt.get_unverified_header")
def test_app_role_app_suffix_authorizes_route(
    mock_header,
    mock_decode,
    mock_jwks,
    mock_pyjwk,
    lambda_context,
    auth_event_factory,
):
    # App token with 'tams-api.read.app' accessing GET /service
    event = auth_event_factory(
        "GET", "/service", "/service", {"Authorization": "Bearer app_token"}
    )
    mock_header.return_value = {"kid": "key-1", "alg": "RS256"}
    mock_jwks.return_value = {"keys": [{"kid": "key-1"}]}
    mock_pyjwk.return_value.key = "mock_key"

    mock_decode.side_effect = [
        {"iss": "https://login.microsoftonline.com/tenant-123/v2.0"},
        {
            "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
            "idtyp": "app",
            "roles": ["tams-api.read.app"],
            "sub": "service-principal-123",
            "app_displayname": "TAMS-Reader-Service",
        },
    ]

    response = entra_authorizer.lambda_handler(event, lambda_context)
    assert response["policyDocument"]["Statement"][0]["Effect"] == "Allow"
    assert response["context"]["username"] == "TAMS-Reader-Service"
    assert response["context"]["token_type"] == "app_only"
    scopes = json.loads(response["context"]["scopes"])
    assert "tams-api.read" in scopes
    assert "tams-api.read.app" in scopes
