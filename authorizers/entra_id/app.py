import json
import os
import re
import time
from functools import cache
from urllib.parse import urlparse

import jwt
import requests
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.utilities.data_classes import event_source
from aws_lambda_powertools.utilities.data_classes.api_gateway_authorizer_event import (
    APIGatewayAuthorizerRequestEvent,
    APIGatewayAuthorizerResponse,
)
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger(
    service=os.environ.get("POWERTOOLS_SERVICE_NAME", "tams-entra-authorizer")
)
metrics = Metrics(namespace=os.environ.get("POWERTOOLS_METRICS_NAMESPACE", "TAMS"))

REQUESTS_GET_TIMEOUT = 5

# Cache for JWKS - keyed by issuer
_jwks_cache: dict[str, dict] = {}
_jwks_cache_time: dict[str, float] = {}

# Cache for OIDC config - keyed by issuer
_oidc_config_cache: dict[str, dict] = {}
_oidc_config_cache_time: dict[str, float] = {}

jwks_cache_ttl = int(os.environ.get("JWKS_CACHE_TTL", 86400))
tenant_id = os.environ.get("TENANT_ID", "").strip()

# Build allowed issuers list from environment
_env_allowed_issuers = [
    iss.strip()
    for iss in os.environ.get("ALLOWED_ISSUERS", "").split(",")
    if iss.strip()
]
if not _env_allowed_issuers and tenant_id:
    # Default to standard Microsoft Entra ID v2.0 and v1.0 issuer formats for the tenant
    allowed_issuers = [
        f"https://login.microsoftonline.com/{tenant_id}/v2.0",
        f"https://sts.windows.net/{tenant_id}/",
        f"https://login.microsoftonline.com/{tenant_id}",
    ]
else:
    allowed_issuers = _env_allowed_issuers

# Allowed audiences (optional)
_env_allowed_audiences = os.environ.get(
    "ALLOWED_AUDIENCES",
    os.environ.get("AUDIENCE", os.environ.get("CLIENT_ID", "")),
).split(",")
allowed_audiences = [aud.strip() for aud in _env_allowed_audiences if aud.strip()]

# Load route-to-scopes mapping
file_path = os.path.join(os.path.dirname(__file__), "oauth_scopes.json")
with open(file_path, "r", encoding="utf-8") as f:
    _oauth_scopes = json.load(f)


def get_required_scopes(method: str, resource: str) -> list[str]:
    """Get required OAuth scopes for a method and path."""
    if resource in _oauth_scopes and method in _oauth_scopes[resource]:
        return _oauth_scopes[resource][method]
    return []


def resource_to_arn_path(resource: str) -> str:
    """Convert API Gateway resource path to ARN format (e.g. /flows/{flowId} -> /flows/*)."""
    return re.sub(r"\{[^}]+\}", "*", resource)


def get_oidc_config(issuer: str) -> dict:
    """Fetch and cache OIDC configuration per issuer."""
    current_time = time.time()

    if (
        issuer in _oidc_config_cache
        and (current_time - _oidc_config_cache_time.get(issuer, 0)) < jwks_cache_ttl
    ):
        return _oidc_config_cache[issuer]

    parsed_issuer = urlparse(issuer)
    if parsed_issuer.scheme not in ("https", "http"):
        raise ValueError(f"Invalid issuer URL scheme: {parsed_issuer.scheme}")

    # For Entra ID v1/v2 or standard OIDC discovery URL
    discovery_url = f"{issuer.rstrip('/')}/.well-known/openid-configuration"
    try:
        response = requests.get(discovery_url, timeout=REQUESTS_GET_TIMEOUT)
        response.raise_for_status()
        config = response.json()

        _oidc_config_cache[issuer] = config
        _oidc_config_cache_time[issuer] = current_time

        logger.info(
            "Fetched Entra ID OIDC config.",
            extra={"issuer": issuer, "jwks_uri": config.get("jwks_uri")},
        )
        return config
    except requests.exceptions.RequestException as e:
        logger.warning(
            "Failed to fetch OIDC config.",
            extra={"issuer": issuer, "exception": str(e)},
        )
        raise


def get_jwks(issuer: str) -> dict:
    """Fetch and cache JWKS per issuer using OIDC discovery or fallback."""
    current_time = time.time()

    if (
        issuer in _jwks_cache
        and (current_time - _jwks_cache_time.get(issuer, 0)) < jwks_cache_ttl
    ):
        return _jwks_cache[issuer]

    parsed_issuer = urlparse(issuer)
    if parsed_issuer.scheme not in ("https", "http"):
        raise ValueError(f"Invalid issuer URL scheme: {parsed_issuer.scheme}")

    jwks_url = None
    try:
        oidc_config = get_oidc_config(issuer)
        jwks_url = oidc_config.get("jwks_uri")
    except Exception as e:
        logger.warning(
            "OIDC discovery failed, falling back to standard discovery path.",
            extra={"issuer": issuer, "exception": str(e)},
        )

    if not jwks_url:
        # Fallback to standard Entra ID keys endpoint
        if tenant_id:
            jwks_url = (
                f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
            )
        else:
            jwks_url = f"{issuer.rstrip('/')}/discovery/v2.0/keys"

    logger.info("Fetching JWKS.", extra={"jwks_uri": jwks_url})
    response = requests.get(jwks_url, timeout=REQUESTS_GET_TIMEOUT)
    response.raise_for_status()

    _jwks_cache[issuer] = response.json()
    _jwks_cache_time[issuer] = current_time
    return _jwks_cache[issuer]


def verify_jwt(token: str) -> dict:
    """
    Verify and decode Microsoft Entra ID JWT token (Delegated or App-Only).
    """
    unverified_payload = jwt.decode(token, options={"verify_signature": False})
    issuer = unverified_payload.get("iss")

    if not issuer:
        logger.error("Token missing issuer claim.")
        raise PermissionError("Unauthorized")

    if allowed_issuers and issuer not in allowed_issuers:
        # Check normalized trailing slash match
        norm_issuer = issuer.rstrip("/")
        norm_allowed = [i.rstrip("/") for i in allowed_issuers]
        if norm_issuer not in norm_allowed:
            logger.error(
                "Invalid Issuer.",
                extra={"issuer": issuer, "allowed_issuers": allowed_issuers},
            )
            raise PermissionError("Unauthorized")

    headers = jwt.get_unverified_header(token)
    kid = headers.get("kid")
    algorithm = headers.get("alg")

    if not kid:
        logger.error("Missing kid in token header.")
        raise PermissionError("Unauthorized")

    if not algorithm:
        logger.error("Algorithm not specified in token header.")
        raise PermissionError("Unauthorized")

    jwks = get_jwks(issuer)
    key = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)
    if not key:
        logger.error("Public key not found in JWKS for kid.", extra={"kid": kid})
        raise PermissionError("Unauthorized")

    public_key = jwt.PyJWK(key).key

    # Decode and verify signature, expiration, not-before
    decode_options = {
        "verify_signature": True,
        "verify_exp": True,
        "verify_nbf": True,
        "verify_iat": True,
        "verify_aud": bool(allowed_audiences),
    }

    claims = jwt.decode(
        token,
        public_key,
        algorithms=[algorithm],
        issuer=issuer,
        audience=allowed_audiences if allowed_audiences else None,
        options=decode_options,
    )

    return claims


def normalize_scopes(scopes: set[str] | list[str]) -> list[str]:
    """
    Normalize scopes and roles to include:
    - Stripping common M2M/app role suffixes (.app, .all, /app, /all)
    - Both dot and slash variants (e.g., 'tams-api.read' and 'tams-api/read')
    for backward compatibility with backend decorators, constants, and oauth_scopes.json.
    """
    normalized = set()
    for s in scopes:
        s = s.strip()
        if not s:
            continue
        normalized.add(s)

        # Strip common M2M / application role suffixes (.app, .all)
        base = re.sub(r"(\.all|\.app|/all|/app)$", "", s, flags=re.IGNORECASE)
        normalized.add(base)

        for item in (s, base):
            # Add dot variant if contains slash (tams-api/read -> tams-api.read)
            if "/" in item:
                normalized.add(item.replace("/", "."))
            # Add slash variant if contains dot (tams-api.read -> tams-api/read)
            if "." in item:
                normalized.add(item.replace(".", "/"))
    return sorted(normalized)


def extract_token_info(claims: dict) -> dict:
    """
    Extract caller identity, scopes, and auth classes for both
    Delegated (user+app) and App-Only (machine-to-machine) tokens.
    """
    is_app_only = claims.get("idtyp") == "app" or (
        "roles" in claims
        and "scp" not in claims
        and "scope" not in claims
        and not claims.get("preferred_username")
        and not claims.get("upn")
    )

    # 1. Extract Scopes / Roles
    raw_scopes: set[str] = set()

    # Delegated scopes (scp or scope claim)
    if "scp" in claims and isinstance(claims["scp"], str):
        raw_scopes.update(claims["scp"].split())
    elif "scope" in claims and isinstance(claims["scope"], str):
        raw_scopes.update(claims["scope"].split())

    # App roles / Application permissions (roles claim)
    if "roles" in claims:
        roles_val = claims["roles"]
        if isinstance(roles_val, list):
            raw_scopes.update(roles_val)
        elif isinstance(roles_val, str):
            raw_scopes.add(roles_val)

    normalized_scopes = normalize_scopes(raw_scopes)

    # 2. Extract Username for TAMS audit (created_by / updated_by)
    if is_app_only:
        # App-only identity
        username = (
            claims.get("app_displayname")
            or claims.get("azp")
            or claims.get("appid")
            or claims.get("sub")
            or claims.get("oid")
            or "app-principal"
        )
    else:
        # User / Delegated identity
        username = (
            claims.get("preferred_username")
            or claims.get("upn")
            or claims.get("email")
            or claims.get("unique_name")
            or claims.get("name")
            or claims.get("sub")
            or claims.get("oid")
            or "user"
        )

    # 3. Extract Principal ID (unique subject / object identifier)
    principal_id = (
        claims.get("oid")
        or claims.get("sub")
        or claims.get("azp")
        or claims.get("appid")
        or username
    )

    # 4. Extract Auth Classes / Groups
    auth_classes: list[str] = []
    if "groups" in claims:
        groups_val = claims["groups"]
        if isinstance(groups_val, list):
            auth_classes.extend(groups_val)
        elif isinstance(groups_val, str):
            auth_classes.append(groups_val)

    if "auth_classes" in claims:
        ac_val = claims["auth_classes"]
        if isinstance(ac_val, list):
            auth_classes.extend(ac_val)
        elif isinstance(ac_val, str):
            auth_classes.append(ac_val)

    return {
        "token_type": "app_only" if is_app_only else "delegated",
        "principal_id": principal_id,
        "username": username,
        "scopes": normalized_scopes,
        "auth_classes": auth_classes,
        "tenant_id": claims.get("tid", ""),
        "client_id": claims.get("azp", claims.get("appid", "")),
    }


def allow_route_with_and_without_trailing_slash(
    policy: APIGatewayAuthorizerResponse, http_method: str, arn_path: str
) -> None:
    """Allow route with and without trailing slash for caching and gateway compatibility."""
    policy.allow_route(http_method, arn_path)
    if not arn_path.endswith("/"):
        policy.allow_route(http_method, arn_path + "/")


# Pre-warm JWKS cache during Lambda initialization (SnapStart)
for allowed_issuer in allowed_issuers:
    if allowed_issuer.strip():
        try:
            get_jwks(allowed_issuer.strip())
            logger.info(
                "Pre-warmed Entra ID JWKS cache.", extra={"issuer": allowed_issuer}
            )
        except (ValueError, requests.exceptions.RequestException) as err:
            logger.warning(
                "Failed to pre-warm JWKS.",
                extra={"issuer": allowed_issuer, "exception": str(err)},
            )


@logger.inject_lambda_context(log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
@event_source(data_class=APIGatewayAuthorizerRequestEvent)
def lambda_handler(
    event: APIGatewayAuthorizerRequestEvent, context: LambdaContext
) -> dict:
    """
    Lambda Authorizer for Microsoft Entra ID supporting both Delegated
    and Application (App-only) tokens for the TAMS API.
    """
    auth_header = event.headers.get("Authorization") or event.headers.get(
        "authorization"
    )
    if not auth_header:
        logger.error("Authorization Header not found.")
        raise PermissionError("Unauthorized")

    token = auth_header.removeprefix("Bearer ").removeprefix("bearer ")

    try:
        claims = verify_jwt(token)
        token_info = extract_token_info(claims)

        arn = event.parsed_arn
        policy = APIGatewayAuthorizerResponse(
            principal_id=token_info["principal_id"],
            context={
                "scopes": json.dumps(token_info["scopes"]),
                "username": token_info["username"],
                "auth_classes": json.dumps(token_info["auth_classes"]),
                "token_type": token_info["token_type"],
                "tenant_id": token_info["tenant_id"],
                "client_id": token_info["client_id"],
            },
            region=arn.region,
            aws_account_id=arn.aws_account_id,
            api_id=arn.api_id,
            stage=arn.stage,
        )

        required_scopes = get_required_scopes(event.http_method, event.resource)
        arn_path = resource_to_arn_path(event.resource)

        # Evaluate scopes against required scopes
        # Since normalize_scopes expands both tams-api.read and tams-api/read, set intersection works reliably
        if set(token_info["scopes"]) & set(required_scopes):
            allow_route_with_and_without_trailing_slash(
                policy, event.http_method, arn_path
            )
        else:
            # If no scopes are defined in oauth_scopes.json for this method/path,
            # allow through to backend Lambda for appropriate 404 handler
            http_method = "*" if event.http_method == "ANY" else event.http_method
            if not required_scopes:
                allow_route_with_and_without_trailing_slash(
                    policy, http_method, arn_path
                )
            else:
                # Scopes are required but caller lacks matching scope/role -> deny
                logger.warning(
                    "Forbidden: Insufficient scopes/roles.",
                    extra={
                        "required": required_scopes,
                        "supplied": token_info["scopes"],
                        "resource": event.resource,
                        "method": event.http_method,
                    },
                )
                policy.deny_all_routes()

        return policy.asdict()

    except (PermissionError, jwt.PyJWTError, ValueError) as e:
        logger.error("Authorization failed.", extra={"exception": str(e)})
        raise PermissionError("Unauthorized") from e
    except requests.exceptions.RequestException as e:
        logger.error(
            "Request failed while fetching JWKS / OIDC config.",
            extra={"exception": str(e)},
            exc_info=True,
        )
        raise PermissionError("Unauthorized") from e
    except Exception as e:
        logger.error(
            "Unexpected authorizer error.",
            extra={"exception": str(e)},
            exc_info=True,
        )
        raise PermissionError("Unauthorized") from e
