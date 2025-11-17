import json
import os
import re
import time
from functools import cache
from urllib.parse import urlparse

import boto3
import jwt
import requests
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.utilities.data_classes import event_source
from aws_lambda_powertools.utilities.data_classes.api_gateway_authorizer_event import (
    APIGatewayAuthorizerRequestEvent,
    APIGatewayAuthorizerResponse,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.exceptions import ClientError

logger = Logger()
metrics = Metrics()

idp = boto3.client("cognito-idp")


# Cache for JWKS - keyed by issuer
_jwks_cache: dict[str, dict] = {}
_jwks_cache_time: dict[str, float] = {}
jwks_cache_ttl = int(os.environ.get("JWKS_CACHE_TTL", 86400))
allowed_issuers = os.environ.get("ALLOWED_ISSUERS", "").split(",")

# Load at module initialization instead of lazy loading
file_path = os.path.join(os.path.dirname(__file__), "oauth_scopes.json")
with open(file_path, "r", encoding="utf-8") as f:
    _oauth_scopes = json.load(f)


def get_required_scopes(method: str, resource: str) -> list[str]:
    """Get required OAuth scopes for a method and path"""
    if resource in _oauth_scopes and method in _oauth_scopes[resource]:
        return _oauth_scopes[resource][method]

    return []


def resource_to_arn_path(resource: str) -> str:
    """Convert API Gateway resource path to ARN format"""
    return re.sub(r"\{[^}]+\}", "*", resource)


def get_jwks(issuer: str) -> dict:
    """Fetch and cache JWKS per issuer"""
    current_time = time.time()

    if (
        issuer in _jwks_cache
        and (current_time - _jwks_cache_time.get(issuer, 0)) < jwks_cache_ttl
    ):
        return _jwks_cache[issuer]

    # Validate the issuer URL scheme
    parsed_issuer = urlparse(issuer)
    if parsed_issuer.scheme not in ("https", "http"):
        raise ValueError(f"Invalid issuer URL scheme: {parsed_issuer.scheme}")

    jwks_url = f"{issuer}/.well-known/jwks.json"

    response = requests.get(jwks_url, timeout=5)
    response.raise_for_status()

    _jwks_cache[issuer] = response.json()
    _jwks_cache_time[issuer] = current_time
    return _jwks_cache[issuer]


def verify_jwt(token: str) -> dict:
    """Verify and decode JWT token"""
    unverified = jwt.decode(token, options={"verify_signature": False})
    issuer = unverified.get("iss")
    if issuer not in allowed_issuers:
        logger.error("Invalid Issuer.")
        raise PermissionError("Unauthorized")

    jwks = get_jwks(issuer)
    headers = jwt.get_unverified_header(token)
    key = next((k for k in jwks["keys"] if k["kid"] == headers["kid"]), None)

    if not key:
        logger.error("Public key not found.")
        raise PermissionError("Unauthorized")

    algorithm = headers.get("alg")
    if not algorithm:
        logger.error("Algorithm not specified in token.")
        raise PermissionError("Unauthorized")

    # Let PyJWT determine the correct algorithm class from the JWK
    public_key = jwt.PyJWK(key).key

    return jwt.decode(
        token,
        public_key,
        algorithms=[algorithm],
        issuer=issuer,
        options={"verify_aud": False},
    )


# Pre-warm JWKS cache for SnapStart
for allowed_issuer in allowed_issuers:
    if allowed_issuer.strip():
        try:
            get_jwks(allowed_issuer.strip())
            logger.info(f"Pre-warmed JWKS cache for {allowed_issuer}")
        except (ValueError, requests.exceptions.RequestException) as e:
            logger.warning(f"Failed to pre-warm JWKS for {allowed_issuer}: {e}")


@cache
def get_user_pool(user_pool_id: str) -> dict:
    """
    Get user pool details. Raises ClientError on failure.
    Only successful results are cached (exceptions are not cached by @cache).
    """
    return idp.describe_user_pool(UserPoolId=user_pool_id)["UserPool"]


@cache
def get_user_pool_client(user_pool_id: str, client_id: str) -> dict:
    """
    Get user pool client details. Raises ClientError on failure.
    Only successful results are cached (exceptions are not cached by @cache).
    """
    return idp.describe_user_pool_client(UserPoolId=user_pool_id, ClientId=client_id)[
        "UserPoolClient"
    ]


@cache
def get_user_attributes(user_pool_id: str, username: str) -> dict[str, str]:
    """
    Get user attributes. Raises ClientError on failure.
    Only successful results are cached (exceptions are not cached by @cache).
    """
    user_attributes = idp.admin_get_user(UserPoolId=user_pool_id, Username=username)[
        "UserAttributes"
    ]
    return {a["Name"]: a["Value"] for a in user_attributes}


def get_user_pool_id_from_issuer(issuer: str) -> str:
    """Extract user pool ID from Cognito issuer URL"""
    return issuer.split("/")[-1]


def get_username(claims: dict, issuer: str) -> str:
    """
    Derive username from claims using Cognito API if issuer is Cognito.
    Falls back to claim values if Cognito API calls fail.
    """
    username = claims.get("username")

    # For non-Cognito issuers, use claims directly
    if "cognito" not in issuer.lower():
        return username or claims.get("sub", "")

    user_pool_id = get_user_pool_id_from_issuer(issuer)

    # Try to get email from Cognito if username is present
    if username:
        try:
            user_pool = get_user_pool(user_pool_id)
            if user_pool.get("UsernameAttributes"):
                user_attributes = get_user_attributes(user_pool_id, username)
                if (
                    "email" in user_pool["UsernameAttributes"]
                    and "email" in user_attributes
                ):
                    return user_attributes["email"]
        except ClientError as e:
            logger.warning(
                f"Failed to get user pool or attributes for {username}: {str(e)}. "
                "Using username from claims."
            )
        return username

    # Try to get client name if client_id is present
    if claims.get("client_id"):
        try:
            user_pool_client = get_user_pool_client(user_pool_id, claims["client_id"])
            return user_pool_client["ClientName"]
        except ClientError as e:
            logger.warning(
                f"Failed to get user pool client {claims['client_id']}: {str(e)}. "
                "Using sub from claims."
            )

    # Final fallback to sub or empty string
    return claims.get("sub", "")


@logger.inject_lambda_context(log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
# pylint: disable=no-value-for-parameter
@event_source(data_class=APIGatewayAuthorizerRequestEvent)
# pylint: disable=unused-argument
def lambda_handler(event: APIGatewayAuthorizerRequestEvent, context: LambdaContext):
    auth_header = event.headers.get("Authorization")
    if not auth_header:
        logger.error("Authorization Header not found.")
        raise PermissionError("Unauthorized")

    token = auth_header.removeprefix("Bearer ").removeprefix("bearer ")

    try:
        claims = verify_jwt(token)
        supplied_scopes = claims.get("scope", "").split() if "scope" in claims else []
        arn = event.parsed_arn
        policy = APIGatewayAuthorizerResponse(
            principal_id=claims.get("sub", claims.get("client_id", "user")),
            context={
                "scopes": json.dumps(supplied_scopes),
                "username": (
                    get_username(claims, claims.get("iss", ""))
                    if event.http_method in ("PUT", "DELETE", "POST")
                    else claims.get("sub", "")
                ),
            },
            region=arn.region,
            aws_account_id=arn.aws_account_id,
            api_id=arn.api_id,
            stage=arn.stage,
        )
        required_scopes = get_required_scopes(event.http_method, event.resource)
        # Check if supplied scopes contain one of the required scopes
        if set(supplied_scopes) & set(required_scopes):
            policy.allow_route(event.http_method, resource_to_arn_path(event.resource))
        else:
            policy.deny_all_routes()
        return policy.asdict()

    except (PermissionError, jwt.PyJWTError, ValueError) as e:
        # Catches authorization, JWT validation, and URL validation errors
        logger.error(f"Authorization failed: {str(e)}")
        raise PermissionError("Unauthorized") from e
    except requests.exceptions.RequestException as e:
        # Catches all requests-related errors (Timeout, HTTPError, ConnectionError, etc.)
        logger.error(f"Request failed while fetching JWKS: {str(e)}", exc_info=True)
        raise PermissionError("Unauthorized") from e
    except Exception as e:
        # Catch-all for any unexpected errors
        logger.error(f"Unexpected authorization error: {str(e)}", exc_info=True)
        raise PermissionError("Unauthorized") from e
