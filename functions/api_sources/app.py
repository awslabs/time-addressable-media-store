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
from neptune import (
    check_node_exists,
    enhance_resources,
    query_node,
    query_node_property,
    query_node_tags,
    query_sources,
    set_node_property,
)
from schema import Contentformat, Source, Tags, Uuid
from typing_extensions import Annotated
from utils import (
    generate_link_url,
    get_username,
    model_dump,
    opencypher_property_name,
    parse_claims,
    parse_tag_parameters,
    publish_event,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(
    enable_validation=True, cors=CORSConfig(expose_headers=["*"])
)
metrics = Metrics()

record_type = "source"
event_bus = os.environ["EVENT_BUS"]

UUID_PATTERN = Uuid.model_fields["root"].metadata[0].pattern


@app.head("/sources")
@app.get("/sources")
@tracer.capture_method(capture_response=False)
def list_sources(
    param_label: Annotated[Optional[str], Query(alias="label")] = None,
    param_format: Annotated[Optional[Contentformat], Query(alias="format")] = None,
    param_page: Annotated[Optional[str], Query(alias="page")] = None,
    param_limit: Annotated[Optional[int], Query(alias="limit", gt=0)] = None,
):
    param_tag_values, param_tag_exists = parse_tag_parameters(
        app.current_event.query_string_parameters
    )
    custom_headers = {}
    if param_limit:
        custom_headers["X-Paging-Limit"] = str(param_limit)
    items, next_page = query_sources(
        {
            "label": param_label,
            "tag_values": param_tag_values,
            "tag_exists": param_tag_exists,
            "format": param_format.value if param_format else None,
            "page": param_page,
            "limit": param_limit,
        }
    )
    if next_page:
        custom_headers["X-Paging-NextKey"] = str(next_page)
        custom_headers["Link"] = generate_link_url(app.current_event, str(next_page))
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
        body=model_dump([Source(**item) for item in items]),
        headers=custom_headers,
    )


@app.head("/sources/<sourceId>")
@app.get("/sources/<sourceId>")
@tracer.capture_method(capture_response=False)
def get_source_details(
    source_id: Annotated[str, Path(alias="sourceId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, source_id)
    except ValueError as e:
        raise NotFoundError("The requested Source does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump(Source(**item)), HTTPStatus.OK.value  # 200


@app.head("/sources/<sourceId>/tags")
@app.get("/sources/<sourceId>/tags")
@tracer.capture_method(capture_response=False)
def get_source_tags(
    source_id: Annotated[str, Path(alias="sourceId", pattern=UUID_PATTERN)],
):
    try:
        tags = query_node_tags(record_type, source_id)
    except ValueError as e:
        raise NotFoundError("The requested Source does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump(Tags(**tags)), HTTPStatus.OK.value  # 200


@app.head("/sources/<sourceId>/tags/<name>")
@app.get("/sources/<sourceId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def get_source_tag_value(
    source_id: Annotated[str, Path(alias="sourceId", pattern=UUID_PATTERN)],
    tag_name: Annotated[str, Path(alias="name")],
):
    try:
        tags = query_node_tags(record_type, source_id)
    except ValueError as e:
        raise NotFoundError("The requested Source or tag does not exist.") from e  # 404
    if tag_name not in tags:
        raise NotFoundError("The requested Source or tag does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return tags[tag_name], HTTPStatus.OK.value  # 200


@app.put("/sources/<sourceId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def put_source_tag_value(
    tag_value: Annotated[str, Body()],
    source_id: Annotated[str, Path(alias="sourceId", pattern=UUID_PATTERN)],
    tag_name: Annotated[str, Path(alias="name")],
):
    if not check_node_exists(record_type, source_id):
        raise NotFoundError(
            "The requested Source does not exist, or the tag name in the path is invalid."
        )  # 404
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type,
        source_id,
        username,
        {f"t.{opencypher_property_name(tag_name)}": tag_value},
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/sources/<sourceId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def delete_source_tag(
    source_id: Annotated[str, Path(alias="sourceId", pattern=UUID_PATTERN)],
    tag_name: Annotated[str, Path(alias="name")],
):
    try:
        item = query_node(record_type, source_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested Source ID or tag in the path is invalid."
        ) from e  # 404
    if tag_name not in item["tags"]:
        raise NotFoundError(
            "The requested Source ID or tag in the path is invalid."
        )  # 404
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type,
        source_id,
        username,
        {f"t.{opencypher_property_name(tag_name)}": None},
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/sources/<sourceId>/description")
@app.get("/sources/<sourceId>/description")
@tracer.capture_method(capture_response=False)
def get_source_description(
    source_id: Annotated[str, Path(alias="sourceId", pattern=UUID_PATTERN)],
):
    try:
        description = query_node_property(record_type, source_id, "description")
    except ValueError as e:
        raise NotFoundError("The requested Source does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return description, HTTPStatus.OK.value  # 200


@app.put("/sources/<sourceId>/description")
@tracer.capture_method(capture_response=False)
def put_source_description(
    description: Annotated[str, Body()],
    source_id: Annotated[str, Path(alias="sourceId", pattern=UUID_PATTERN)],
):
    if not check_node_exists(record_type, source_id):
        raise NotFoundError("The requested Source does not exist.")  # 404
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, source_id, username, {"source.description": description}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/sources/<sourceId>/description")
@tracer.capture_method(capture_response=False)
def delete_source_description(
    source_id: Annotated[str, Path(alias="sourceId", pattern=UUID_PATTERN)],
):
    if not check_node_exists(record_type, source_id):
        raise NotFoundError("The Source ID in the path is invalid.")  # 404
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, source_id, username, {"source.description": None}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/sources/<sourceId>/label")
@app.get("/sources/<sourceId>/label")
@tracer.capture_method(capture_response=False)
def get_source_label(
    source_id: Annotated[str, Path(alias="sourceId", pattern=UUID_PATTERN)],
):
    try:
        label = query_node_property(record_type, source_id, "label")
    except ValueError as e:
        raise NotFoundError(
            "The requested Source does not exist, or does not have a label set."
        ) from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return label, HTTPStatus.OK.value  # 200


@app.put("/sources/<sourceId>/label")
@tracer.capture_method(capture_response=False)
def put_source_label(
    label: Annotated[str, Body()],
    source_id: Annotated[str, Path(alias="sourceId", pattern=UUID_PATTERN)],
):
    if not check_node_exists(record_type, source_id):
        raise NotFoundError("The requested Source does not exist.")  # 404
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, source_id, username, {"source.label": label}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/sources/<sourceId>/label")
@tracer.capture_method(capture_response=False)
def delete_source_label(
    source_id: Annotated[str, Path(alias="sourceId", pattern=UUID_PATTERN)],
):
    if not check_node_exists(record_type, source_id):
        raise NotFoundError("The requested Source ID in the path is invalid.")  # 404
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, source_id, username, {"source.label": None}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


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


@tracer.capture_method(capture_response=False)
def get_event_resources(obj: dict) -> list:
    """Generate a list of event resources for the given source object."""
    return enhance_resources(
        [
            f'tams:source:{obj["id"]}',
            *set(
                f"tams:source-collected-by:{c_id}"
                for c_id in obj.get("collected_by", [])
            ),
        ]
    )
