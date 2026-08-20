import os
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Optional

import boto3
import constants
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.event_handler import (
    APIGatewayRestResolver,
    CORSConfig,
    Response,
    content_types,
)
from aws_lambda_powertools.event_handler.exceptions import (
    BadRequestError,
    NotFoundError,
)
from aws_lambda_powertools.event_handler.openapi.exceptions import (
    RequestValidationError,
)
from aws_lambda_powertools.event_handler.openapi.params import Body, Path, Query
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from dynamodb import list_storage_backends
from mediatimestamp.immutable import Timestamp
from neptune import (
    check_node_exists,
    delete_webhook,
    merge_profile,
    merge_webhook,
    query_node,
    query_profiles,
    query_webhooks,
)
from schema import (
    Contentformat,
    Eventstreamcommon,
    Mimetype,
    Profile,
    Service,
    Servicepost,
    Storagebackendslist,
    StoragebackendslistItem,
    Uuid,
    Webhookget,
    Webhookpost,
    Webhookput,
)
from typing_extensions import Annotated
from utils import (
    generate_link_url,
    get_username,
    model_dump,
    parse_tag_parameters,
    tags_match,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(
    enable_validation=True, cors=CORSConfig(expose_headers=["*"])
)
metrics = Metrics()

record_type = "webhook"
dynamodb = boto3.resource("dynamodb")
service_table = dynamodb.Table(os.environ["SERVICE_TABLE"])

UUID_PATTERN = Uuid.model_fields["root"].metadata[0].pattern
MIMETYPE_PATTERN = Mimetype.model_fields["root"].metadata[0].pattern


@app.head("/")
@app.get("/")
@tracer.capture_method(capture_response=False)
def get_root():
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return [
        "service",
        "flows",
        "sources",
        "objects",
        "flow-delete-requests",
    ], HTTPStatus.OK.value  # 200


@app.head("/service")
@app.get("/service")
@tracer.capture_method(capture_response=False)
def get_service():
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    get_item = service_table.get_item(
        Key={"record_type": "service", "id": constants.SERVICE_INFO_ID}
    )
    stage_variables = app.current_event.stage_variables
    service = Service(
        type="urn:x-tams:service.example",
        api_version=stage_variables["api_version"],
        service_version=stage_variables["service_version"],
        min_object_timeout=str(Timestamp.from_float(constants.MIN_OBJECT_TIMEOUT_SECS)),
        min_presigned_url_timeout=str(
            Timestamp.from_float(constants.MIN_PRESIGNED_URL_TIMEOUT_SECS)
        ),
        **get_item.get("Item", {}),
    )
    service.event_stream_mechanisms = [Eventstreamcommon(name="webhooks")]
    return model_dump(service), HTTPStatus.OK.value  # 200


@app.post("/service")
@tracer.capture_method(capture_response=False)
def post_service(service_post: Annotated[Servicepost, Body()]):
    get_item = service_table.get_item(
        Key={"record_type": "service", "id": constants.SERVICE_INFO_ID}
    )
    service_record = get_item.get(
        "Item", {"record_type": "service", "id": constants.SERVICE_INFO_ID}
    )
    if service_post.name == "":
        del service_record["name"]
    if service_post.description == "":
        del service_record["description"]
    if service_post.name:
        service_record["name"] = service_post.name
    if service_post.description:
        service_record["description"] = service_post.description
    service_table.put_item(Item=service_record)
    return None, HTTPStatus.OK.value  # 200


@app.head("/service/webhooks")
@app.get("/service/webhooks")
@tracer.capture_method(capture_response=False)
def get_webhooks(
    param_reverse_order: Annotated[Optional[bool], Query(alias="reverse_order")] = None,
    param_page: Annotated[Optional[str], Query(alias="page")] = None,
    param_limit: Annotated[Optional[int], Query(alias="limit", gt=0)] = None,
):
    param_tag_values, param_tag_exists = parse_tag_parameters(
        app.current_event.query_string_parameters
    )
    reverse_order = bool(param_reverse_order)
    custom_headers = {}
    items, next_page, limit_used = query_webhooks(
        {
            "tag_values": param_tag_values,
            "tag_exists": param_tag_exists,
            "reverse_order": reverse_order,
            "page": param_page,
            "limit": param_limit,
        }
    )
    if next_page:
        custom_headers["X-Paging-NextKey"] = str(next_page)
        custom_headers["Link"] = generate_link_url(app.current_event, str(next_page))
    if next_page or limit_used != param_limit:
        custom_headers["X-Paging-Limit"] = str(limit_used)
    custom_headers["X-Paging-Count"] = str(len(items))
    custom_headers["X-Paging-Reverse-Order"] = str(reverse_order)
    if app.current_event.request_context.http_method == "HEAD":
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            body=None,
            headers=custom_headers,
        )
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=model_dump(
            [Webhookget(**item) for item in items],
            preserve_empty_list_fields={"accept_get_urls", "events"},
        ),
        headers=custom_headers,
    )


@app.post("/service/webhooks")
@tracer.capture_method(capture_response=False)
def post_webhooks(webhook: Annotated[Webhookpost, Body()]):
    webhook_dict = webhook.model_dump(mode="json")
    # Set default status if None
    if not webhook_dict.get("status"):
        webhook_dict["status"] = "created"
    webhook_put = Webhookput(
        **webhook_dict,
        id=app.current_event.request_context.request_id,
    )
    item_dict = model_dump(
        Webhookget(**merge_webhook(webhook_put.model_dump(mode="json"), None)),
        preserve_empty_list_fields={"accept_get_urls", "events"},
    )
    return item_dict, HTTPStatus.CREATED.value  # 201


@app.head("/service/webhooks/<webhookId>")
@app.get("/service/webhooks/<webhookId>")
@tracer.capture_method(capture_response=False)
def get_webhook_by_id(
    webhook_id: Annotated[str, Path(alias="webhookId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, webhook_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested Webhook ID in the path is invalid."
        ) from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return (
        model_dump(
            Webhookget(**item), preserve_empty_list_fields={"accept_get_urls", "events"}
        ),
        HTTPStatus.OK.value,
    )  # 200


@app.put("/service/webhooks/<webhookId>")
@tracer.capture_method(capture_response=False)
def put_webhook_by_id(
    webhook: Annotated[Webhookput, Body()],
    webhook_id: Annotated[str, Path(alias="webhookId", pattern=UUID_PATTERN)],
):
    if webhook.id.root != webhook_id:
        raise NotFoundError("The requested Webhook ID in the path is invalid.")  # 404
    try:
        existing_item = query_node(record_type, webhook_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested Webhook ID in the path is invalid."
        ) from e  # 404
    if (
        webhook.status
        and webhook.status.value == "disabled"
        and existing_item["status"] == "error"
    ):
        raise BadRequestError(
            "Bad request. The Webhook is currently in an error status and therefore cannot be updated to disabled."
        )  # 400
    updated_webhook = merge_webhook(model_dump(webhook), existing_item)
    return (
        model_dump(
            Webhookget(**updated_webhook),
            preserve_empty_list_fields={"accept_get_urls", "events"},
        ),
        HTTPStatus.CREATED.value,
    )  # 201


@app.delete("/service/webhooks/<webhookId>")
@tracer.capture_method(capture_response=False)
def delete_webhook_by_id(
    webhook_id: Annotated[str, Path(alias="webhookId", pattern=UUID_PATTERN)],
):
    if not check_node_exists(record_type, webhook_id):
        raise NotFoundError("The requested Webhook ID in the path is invalid.")  # 404
    delete_webhook(webhook_id)
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/service/storage-backends")
@app.get("/service/storage-backends")
@tracer.capture_method(capture_response=False)
def get_storage_backends(
    param_reverse_order: Annotated[Optional[bool], Query(alias="reverse_order")] = None,
    param_page: Annotated[Optional[str], Query(alias="page")] = None,
    param_limit: Annotated[Optional[int], Query(alias="limit", gt=0)] = None,
):
    reverse_order = bool(param_reverse_order)
    param_tag_values, param_tag_exists = parse_tag_parameters(
        app.current_event.query_string_parameters
    )
    # Storage Backend tags live on the DynamoDB record (set out of band, not via
    # the API); filter on them here as list_storage_backends holds no filter.
    backends = [
        backend
        for backend in list_storage_backends()
        if tags_match(backend.get("tags"), param_tag_values, param_tag_exists)
    ]
    # list_storage_backends is lru_cached, so sort a copy rather than in place.
    # Storage Backends sort alphabetically by label by default; id is a unique
    # secondary key so pagination is deterministic when labels collide. Backends
    # with an unset label sort after those with one by default (and before when
    # reverse_order is set); the leading `label is None` flag separates the
    # groups so None is never compared against a str.
    storage_backends = sorted(
        backends,
        key=lambda backend: (
            backend.get("label") is None,
            backend.get("label"),
            backend["id"],
        ),
        reverse=reverse_order,
    )
    page = int(param_page) if param_page else 0
    limit_used = min(
        param_limit if param_limit else constants.DEFAULT_PAGE_LIMIT,
        constants.MAX_PAGE_LIMIT,
    )
    items = storage_backends[page : page + limit_used]
    next_page = page + limit_used if page + limit_used < len(storage_backends) else None
    custom_headers = {}
    if next_page:
        custom_headers["X-Paging-NextKey"] = str(next_page)
        custom_headers["Link"] = generate_link_url(app.current_event, str(next_page))
    if next_page or limit_used != param_limit:
        custom_headers["X-Paging-Limit"] = str(limit_used)
    custom_headers["X-Paging-Count"] = str(len(items))
    custom_headers["X-Paging-Reverse-Order"] = str(reverse_order)
    if app.current_event.request_context.http_method == "HEAD":
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            content_type=content_types.APPLICATION_JSON,
            body=None,
            headers=custom_headers,
        )
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=model_dump(
            Storagebackendslist([StoragebackendslistItem(**item) for item in items])
        ),
        headers=custom_headers,
    )


@app.head("/service/profiles")
@app.get("/service/profiles")
@tracer.capture_method(capture_response=False)
def get_profiles(
    param_format: Annotated[Optional[Contentformat], Query(alias="format")] = None,
    param_codec: Annotated[
        Optional[str], Query(alias="codec", pattern=MIMETYPE_PATTERN)
    ] = None,
    param_label: Annotated[Optional[str], Query(alias="label")] = None,
    param_page: Annotated[Optional[str], Query(alias="page")] = None,
    param_limit: Annotated[Optional[int], Query(alias="limit", gt=0)] = None,
):
    custom_headers = {}
    items, next_page, limit_used = query_profiles(
        {
            "format": param_format.value if param_format else None,
            "codec": param_codec,
            "label": param_label,
            "page": param_page,
            "limit": param_limit,
        }
    )
    if next_page:
        custom_headers["X-Paging-NextKey"] = str(next_page)
        custom_headers["Link"] = generate_link_url(app.current_event, str(next_page))
    if next_page or limit_used != param_limit:
        custom_headers["X-Paging-Limit"] = str(limit_used)
    custom_headers["X-Paging-Count"] = str(len(items))
    if app.current_event.request_context.http_method == "HEAD":
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            body=None,
            headers=custom_headers,
        )
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=model_dump([Profile(**item) for item in items]),
        headers=custom_headers,
    )


@app.head("/service/profiles/<profileId>")
@app.get("/service/profiles/<profileId>")
@tracer.capture_method(capture_response=False)
def get_profile_by_id(
    profile_id: Annotated[str, Path(alias="profileId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node("profile", profile_id)
    except ValueError as e:
        raise NotFoundError("The requested profile does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump(Profile(**item)), HTTPStatus.OK.value  # 200


@app.post("/service/profiles/<profileId>")
@tracer.capture_method(capture_response=False)
def post_profile_by_id(
    profile: Annotated[Profile, Body()],
    profile_id: Annotated[str, Path(alias="profileId", pattern=UUID_PATTERN)],
):
    if profile.id.root != profile_id:
        raise NotFoundError("The requested Profile ID in the path is invalid.")  # 404
    # Profiles are immutable: recreating one would change the Flows already
    # created from it, so an existing Profile must be rejected, not updated.
    if check_node_exists("profile", profile_id):
        raise BadRequestError(
            "Bad request. The requested Profile already exists and Profiles are immutable."
        )  # 400
    # Profile.created is an AwareDatetime, so set a timezone-aware value; it
    # round-trips through Neptune and is re-validated when the Profile is rebuilt.
    profile.created = datetime.now(timezone.utc)
    if not profile.created_by:
        profile.created_by = get_username(app.current_event.request_context)
    # Use the project model_dump (exclude_unset / exclude_none) rather than the raw
    # pydantic dump, so unset defaults (e.g. essence_parameters.vfr=False) are not
    # persisted or echoed back -- mirrors how put_flow_by_id dumps a Flow.
    item = merge_profile(model_dump(profile))
    return model_dump(Profile(**item)), HTTPStatus.CREATED.value  # 201


@logger.inject_lambda_context(
    log_event=True, correlation_id_path=correlation_paths.API_GATEWAY_REST
)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)


@app.exception_handler(RequestValidationError)
def handle_validation_error(ex: RequestValidationError):
    raise BadRequestError(ex.errors())  # 400
