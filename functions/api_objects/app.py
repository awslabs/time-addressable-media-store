import base64
import json
import os
from http import HTTPStatus
from typing import Optional

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
from dynamodb import (
    append_to_segment_list,
    list_storage_backends,
    query_segments_by_object_id,
    remove_get_url_by_label_from_segment,
    remove_storage_id_from_segment,
    storage_table,
)
from neptune import query_object_flows
from schema import Object, Objectsinstancespost
from segment_get_urls import populate_get_urls
from typing_extensions import Annotated
from utils import (
    generate_link_url,
    get_unique_get_urls,
    model_dump,
    parse_tag_parameters,
    put_message,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(
    enable_validation=True, cors=CORSConfig(expose_headers=["*"])
)
metrics = Metrics()

duplication_queue = os.environ["DUPLICATION_QUEUE_URL"]
s3_queue = os.environ["S3_QUEUE_URL"]


@app.head("/objects/<objectId>")
@app.get("/objects/<objectId>")
@tracer.capture_method(capture_response=False)
def get_objects_by_id(
    object_id: Annotated[str, Path(alias="objectId")],
    param_verbose_storage: Annotated[
        Optional[bool], Query(alias="verbose_storage")
    ] = None,
    param_accept_get_urls: Annotated[
        Optional[str], Query(alias="accept_get_urls", pattern=r"^([^,]+(,[^,]+)*)?$")
    ] = None,
    param_accept_storage_ids: Annotated[
        Optional[str],
        Query(
            alias="accept_storage_ids",
            pattern=r"^([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})(,[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})*$",
        ),
    ] = None,
    param_presigned: Annotated[Optional[bool], Query(alias="presigned")] = None,
    param_page: Annotated[Optional[str], Query(alias="page")] = None,
    param_limit: Annotated[Optional[int], Query(alias="limit", gt=0)] = None,
):
    param_tag_values, param_tag_exists = parse_tag_parameters(
        app.current_event.query_string_parameters
    )
    items, last_evaluated_key, limit_used = query_segments_by_object_id(
        object_id,
        limit=param_limit,
        page=param_page,
    )
    if len(items) == 0 and param_page is None:
        raise NotFoundError("The requested media object does not exist.")  # 404
    custom_headers = {}
    if last_evaluated_key:
        next_key = base64.b64encode(
            json.dumps(last_evaluated_key, default=int).encode("utf-8")
        ).decode("utf-8")
        custom_headers["X-Paging-NextKey"] = next_key
        custom_headers["Link"] = generate_link_url(app.current_event, next_key)
    # Set Paging Limit header if paging limit being used is not the one specified
    if last_evaluated_key or param_limit != limit_used:
        custom_headers["X-Paging-Limit"] = str(limit_used)
    if app.current_event.request_context.http_method == "HEAD":
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            content_type=content_types.APPLICATION_JSON,
            body=None,
            headers=custom_headers,
        )
    get_item = storage_table.get_item(
        Key={"id": object_id}, ProjectionExpression="flow_id"
    )
    combined_item = {
        "object_id": object_id,
        "get_urls": get_unique_get_urls(items),
        "storage_ids": list(
            {storage_id for item in items for storage_id in item.get("storage_ids", [])}
        ),
    }
    populate_get_urls(
        [combined_item],
        param_accept_get_urls,
        param_verbose_storage,
        param_accept_storage_ids,
        param_presigned,
    )
    schema_item = Object(
        **{
            "id": object_id,
            "referenced_by_flows": set([item["flow_id"] for item in items]),
            "first_referenced_by_flow": get_item.get("Item", {}).get("flow_id"),
            "get_urls": combined_item.get("get_urls"),
            "key_frame_count": next(
                (
                    item.get("key_frame_count")
                    for item in items
                    if "key_frame_count" in item
                ),
                None,
            ),
        }
    )
    # Filter referenced_by_flows by tag parameters if provided
    if param_tag_values or param_tag_exists:
        tagged_flows = query_object_flows(
            schema_item.referenced_by_flows,
            {
                "tag_values": param_tag_values,
                "tag_exists": param_tag_exists,
            },
        )
        schema_item.referenced_by_flows = list(
            set(schema_item.referenced_by_flows) & set(tagged_flows)
        )
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=model_dump(schema_item),
        headers=custom_headers,
    )


@app.post("/objects/<objectId>/instances")
@tracer.capture_method(capture_response=False)
def post_objects_by_id(
    object_instance: Annotated[Objectsinstancespost, Body()],
    object_id: Annotated[str, Path(alias="objectId")],
):
    items, _, _ = query_segments_by_object_id(
        object_id,
        fetch_all=True,
    )
    if len(items) == 0:
        raise NotFoundError("The Media Object does not exist.")  # 404
    existing_storage_ids = set(
        storage_id for item in items for storage_id in item.get("storage_ids", [])
    )
    if hasattr(object_instance.root, "storage_id"):
        if object_instance.root.storage_id.root in existing_storage_ids:
            raise BadRequestError(
                "The Media Object specified is already available on this Storage Backend."
            )  # 400
        if existing_storage_ids:
            put_message(
                duplication_queue,
                {
                    "object_id": object_id,
                    "destination_storage_id": object_instance.root.storage_id.root,
                },
            )
        else:
            raise BadRequestError(
                "The Media Object specified does not currently exist on controlled storage."
            )  # 400
    elif hasattr(object_instance.root, "url"):
        storage_backends = list_storage_backends()
        storage_labels = set(item["label"] for item in storage_backends)
        presigned_labels = set(
            item["label"].replace(":s3:", ":s3.presigned:")
            for item in storage_backends
            if ":s3:" in item["label"]
        )
        if object_instance.root.label and object_instance.root.label in (
            storage_labels | presigned_labels
        ):
            raise BadRequestError(
                "The specified label is already in use by a storage backend."
            )  # 400
        existing_labels = set(
            get_url.get("label")
            for item in items
            for get_url in item.get("get_urls", [])
        )
        if object_instance.root.label in existing_labels:
            raise BadRequestError(
                "The Media Object specified already exists with this label."
            )  # 400
        for item in items:
            append_to_segment_list(item, "get_urls", model_dump(object_instance.root))
    else:
        raise BadRequestError("Unexpected request body content.")  # 400
    return None, HTTPStatus.CREATED.value  # 201


@app.delete("/objects/<objectId>/instances")
@tracer.capture_method(capture_response=False)
def delete_objects_by_id(
    object_id: Annotated[str, Path(alias="objectId")],
    param_label: Annotated[Optional[str], Query(alias="label")] = None,
    param_storage_id: Annotated[Optional[str], Query(alias="storage_id")] = None,
):
    if not param_label and not param_storage_id:
        raise BadRequestError(
            "One of 'label' or 'storage_id' query parameters must be specified."
        )  # 400
    items, _, _ = query_segments_by_object_id(
        object_id,
        fetch_all=True,
    )
    if len(items) == 0:
        raise NotFoundError("The requested Object ID in the path is invalid.")  # 404

    # Filter items based on storage_id and/or label
    if param_storage_id:
        items = [
            item for item in items if param_storage_id in item.get("storage_ids", [])
        ]
    if param_label:
        items = [
            item
            for item in items
            if any(
                get_url.get("label") == param_label
                for get_url in item.get("get_urls", [])
            )
        ]

    # Check if relevant items exist
    if len(items) == 0:
        raise BadRequestError("Bad request. Invalid query options.")  # 400

    storage_ids = list(
        {storage_id for item in items for storage_id in item.get("storage_ids", [])}
    )
    get_urls = get_unique_get_urls(items)

    # Check if deleting last storage_id instance
    if param_storage_id and storage_ids == [param_storage_id] and not get_urls:
        raise BadRequestError(
            "All instances would be deleted. Use flow segment deletion instead."
        )  # 400

    # Check if deleting last get_url instance
    if param_label and (
        not storage_ids
        and len(get_urls) == 1
        and get_urls[0].get("label") == param_label
    ):
        raise BadRequestError(
            "All instances would be deleted. Use flow segment deletion instead."
        )  # 400

    # Update flow segments
    for item in items:
        if param_storage_id:
            remove_storage_id_from_segment(item, param_storage_id)
        if param_label:
            remove_get_url_by_label_from_segment(item, param_label)

    if param_storage_id:
        # Send message to S3 SQS to delete item if no longer in use
        put_message(
            s3_queue,
            [
                [
                    object_id,
                    [param_storage_id],
                ]
            ],
        )


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
