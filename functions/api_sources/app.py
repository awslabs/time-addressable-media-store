import json
import os
from http import HTTPStatus

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
from aws_lambda_powertools.event_handler.openapi.params import Path
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
from schema import Source, Tags
from typing_extensions import Annotated
from utils import (
    generate_link_url,
    get_username,
    model_dump,
    publish_event,
    validate_query_string,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(enable_validation=True, cors=CORSConfig())
metrics = Metrics(namespace="Powertools")

record_type = "source"
event_bus = os.environ["EVENT_BUS"]

SOURCE_ID_PATTERN = Source.model_fields["id"].metadata[0].pattern


@app.head("/sources")
@app.get("/sources")
@tracer.capture_method(capture_response=False)
def list_sources():
    parameters = app.current_event.query_string_parameters
    if not validate_query_string(parameters, app.current_event.request_context):
        raise BadRequestError("Bad request. Invalid query options.")  # 400
    custom_headers = {}
    if parameters and "limit" in parameters:
        custom_headers["X-Paging-Limit"] = parameters["limit"]
    items, next_page = query_sources(parameters)
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
def get_source_details(sourceId: Annotated[str, Path(pattern=SOURCE_ID_PATTERN)]):
    try:
        item = query_node(record_type, sourceId)
    except ValueError as e:
        raise NotFoundError("The requested Source does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump(Source(**item)), HTTPStatus.OK.value  # 200


@app.head("/sources/<sourceId>/tags")
@app.get("/sources/<sourceId>/tags")
@tracer.capture_method(capture_response=False)
def get_source_tags(sourceId: Annotated[str, Path(pattern=SOURCE_ID_PATTERN)]):
    try:
        tags = query_node_tags(record_type, sourceId)
    except ValueError as e:
        raise NotFoundError("The requested Source does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump(Tags(**tags)), HTTPStatus.OK.value  # 200


@app.head("/sources/<sourceId>/tags/<name>")
@app.get("/sources/<sourceId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def get_source_tag_value(
    sourceId: Annotated[str, Path(pattern=SOURCE_ID_PATTERN)], name: str
):
    try:
        tags = query_node_tags(record_type, sourceId)
    except ValueError as e:
        raise NotFoundError("The requested Source or tag does not exist.") from e  # 404
    if name not in tags:
        raise NotFoundError("The requested Source or tag does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return tags[name], HTTPStatus.OK.value  # 200


@app.put("/sources/<sourceId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def put_source_tag_value(
    sourceId: Annotated[str, Path(pattern=SOURCE_ID_PATTERN)], name: str
):
    if not check_node_exists(record_type, sourceId):
        raise NotFoundError(
            "The requested Source does not exist, or the tag name in the path is invalid."
        )  # 404
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid Source tag value.")  # 400
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(record_type, sourceId, username, {f"t.{name}": body})
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/sources/<sourceId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def delete_source_tag(
    sourceId: Annotated[str, Path(pattern=SOURCE_ID_PATTERN)], name: str
):
    try:
        item = query_node(record_type, sourceId)
    except ValueError as e:
        raise NotFoundError(
            "The requested Source ID or tag in the path is invalid."
        ) from e  # 404
    if name not in item["tags"]:
        raise NotFoundError(
            "The requested Source ID or tag in the path is invalid."
        )  # 404
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(record_type, sourceId, username, {f"t.{name}": None})
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/sources/<sourceId>/description")
@app.get("/sources/<sourceId>/description")
@tracer.capture_method(capture_response=False)
def get_source_description(sourceId: Annotated[str, Path(pattern=SOURCE_ID_PATTERN)]):
    try:
        description = query_node_property(record_type, sourceId, "description")
    except ValueError as e:
        raise NotFoundError("The requested Source does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return description, HTTPStatus.OK.value  # 200


@app.put("/sources/<sourceId>/description")
@tracer.capture_method(capture_response=False)
def put_source_description(sourceId: Annotated[str, Path(pattern=SOURCE_ID_PATTERN)]):
    if not check_node_exists(record_type, sourceId):
        raise NotFoundError("The requested Source does not exist.")  # 404
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid Source description.")  # 400
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(
        record_type, sourceId, username, {"source.description": body}
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
    sourceId: Annotated[str, Path(pattern=SOURCE_ID_PATTERN)]
):
    if not check_node_exists(record_type, sourceId):
        raise NotFoundError("The Source ID in the path is invalid.")  # 404
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(
        record_type, sourceId, username, {"source.description": None}
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
def get_source_label(sourceId: Annotated[str, Path(pattern=SOURCE_ID_PATTERN)]):
    try:
        label = query_node_property(record_type, sourceId, "label")
    except ValueError as e:
        raise NotFoundError(
            "The requested Source does not exist, or does not have a label set."
        ) from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return label, HTTPStatus.OK.value  # 200


@app.put("/sources/<sourceId>/label")
@tracer.capture_method(capture_response=False)
def put_source_label(sourceId: Annotated[str, Path(pattern=SOURCE_ID_PATTERN)]):
    if not check_node_exists(record_type, sourceId):
        raise NotFoundError("The requested Source does not exist.")  # 404
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid Source label.")  # 400
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(
        record_type, sourceId, username, {"source.label": body}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/sources/<sourceId>/label")
@tracer.capture_method(capture_response=False)
def delete_source_label(sourceId: Annotated[str, Path(pattern=SOURCE_ID_PATTERN)]):
    if not check_node_exists(record_type, sourceId):
        raise NotFoundError("The requested Source ID in the path is invalid.")  # 404
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(
        record_type, sourceId, username, {"source.label": None}
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
