import os
import uuid
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Optional

# pylint: disable=no-member
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
    ServiceError,
)
from aws_lambda_powertools.event_handler.openapi.exceptions import (
    RequestValidationError,
)
from aws_lambda_powertools.event_handler.openapi.params import Body, Path, Query
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from dynamodb import get_flow_timerange, storage_table
from mediatimestamp.immutable import TimeRange
from neptune import (
    check_delete_source,
    check_node_exists,
    delete_flow,
    enhance_resources,
    merge_delete_request,
    merge_source_flow,
    query_flow_collection,
    query_flows,
    query_node,
    query_node_property,
    query_node_tags,
    set_flow_collection,
    set_node_property,
    validate_flow_collection,
)
from schema import (
    Collectionitem,
    Contentformat,
    Deletionrequest,
    Flow,
    Flowcollection,
    Flowstorage,
    Flowstoragepost,
    Httprequest,
    MediaObject,
    Mimetype,
    Tags,
    Timerange,
    Uuid,
)
from typing_extensions import Annotated
from utils import (
    base_delete_request_dict,
    generate_link_url,
    generate_presigned_url,
    get_username,
    model_dump,
    parse_claims,
    parse_tag_parameters,
    publish_event,
    put_message,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(
    enable_validation=True, cors=CORSConfig(expose_headers=["*"])
)
metrics = Metrics()

record_type = "flow"
bucket = os.environ["BUCKET"]
del_queue = os.environ["DELETE_QUEUE_URL"]

UUID_PATTERN = Uuid.model_fields["root"].metadata[0].pattern
TIMERANGE_PATTERN = Timerange.model_fields["root"].metadata[0].pattern
MIMETYPE_PATTERN = Mimetype.model_fields["root"].metadata[0].pattern


@app.head("/flows")
@app.get("/flows")
@tracer.capture_method(capture_response=False)
def get_flows(
    param_source_id: Annotated[
        Optional[str], Query(alias="source_id", pattern=UUID_PATTERN)
    ] = None,
    param_timerange: Annotated[
        Optional[str], Query(alias="timerange", pattern=TIMERANGE_PATTERN)
    ] = None,
    param_format: Annotated[Optional[Contentformat], Query(alias="format")] = None,
    param_codec: Annotated[
        Optional[str], Query(alias="codec", pattern=MIMETYPE_PATTERN)
    ] = None,
    param_label: Annotated[Optional[str], Query(alias="label")] = None,
    param_frame_width: Annotated[Optional[int], Query(alias="frame_width")] = None,
    param_frame_height: Annotated[Optional[int], Query(alias="frame_height")] = None,
    param_page: Annotated[Optional[str], Query(alias="page")] = None,
    param_limit: Annotated[Optional[int], Query(alias="limit")] = None,
):
    param_tag_values, param_tag_exists = parse_tag_parameters(
        app.current_event.query_string_parameters
    )
    custom_headers = {}
    if param_limit:
        custom_headers["X-Paging-Limit"] = str(param_limit)
    items, next_page = query_flows(
        {
            "source_id": param_source_id,
            "timerange": param_timerange,
            "format": param_format.value if param_format else None,
            "codec": param_codec,
            "label": param_label,
            "tag_values": param_tag_values,
            "tag_exists": param_tag_exists,
            "frame_width": param_frame_width,
            "frame_height": param_frame_height,
            "page": param_page,
            "limit": param_limit,
        }
    )
    if param_timerange:
        timerange_filter = TimeRange.from_str(param_timerange)
        if timerange_filter.is_empty():
            items = [
                item
                for item in items
                if TimeRange.from_str(get_flow_timerange(item["id"])).is_empty()
            ]
        else:
            items = [
                item
                for item in items
                if not TimeRange.from_str(get_flow_timerange(item["id"]))
                .intersect_with(timerange_filter)
                .is_empty()
            ]
    if next_page:
        custom_headers["X-Paging-NextKey"] = str(next_page)
        custom_headers["Link"] = generate_link_url(app.current_event, str(next_page))
    if app.current_event.request_context.http_method == "HEAD":
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            body=None,
            headers=custom_headers,
        )
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=model_dump([Flow(**item) for item in items]),
        headers=custom_headers,
    )


@app.head("/flows/<flowId>")
@app.get("/flows/<flowId>")
@tracer.capture_method(capture_response=False)
def get_flow_by_id(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
    param_include_timerange: Annotated[
        Optional[bool], Query(alias="include_timerange")
    ] = None,
    param_timerange: Annotated[
        Optional[str], Query(alias="timerange", pattern=TIMERANGE_PATTERN)
    ] = None,
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if param_include_timerange:
        item["timerange"] = get_flow_timerange(flow_id)
    # Update timerange if timerange parameter supplied
    if param_timerange and param_include_timerange:
        timerange_filter = TimeRange.from_str(param_timerange)
        item["timerange"] = str(
            timerange_filter.intersect_with(TimeRange.from_str(item["timerange"]))
        )
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump(Flow(**item)), HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>")
@tracer.capture_method(capture_response=False)
def put_flow_by_id(
    flow: Annotated[Flow, Body()],
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    if flow.root.id != flow_id:
        raise NotFoundError("The requested Flow ID in the path is invalid.")  # 404
    try:
        existing_item = query_node(record_type, flow_id)
        if existing_item.get("read_only"):
            raise ServiceError(
                403,
                "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
            )  # 403
    except ValueError:
        existing_item = {}
    if not validate_flow_collection(flow_id, flow.root.flow_collection):
        raise BadRequestError("Bad request. Invalid flow collection.")  # 400
    # API spec states these fields should be ignored if given in a PUT request.
    for field in constants.FLOW_PUT_IGNORE_FIELDS:
        setattr(flow.root, field, None)
        existing_item.pop(field, None)
    now = datetime.now()
    if existing_item:
        flow.root.metadata_updated = now
    else:
        flow.root.created = now
    # Set these if not supplied
    username = get_username(parse_claims(app.current_event.request_context))
    if not flow.root.created_by and not existing_item:
        flow.root.created_by = username
    if not flow.root.updated_by and existing_item:
        flow.root.updated_by = username
    item_dict = model_dump(Flow(**merge_source_flow(model_dump(flow), existing_item)))
    publish_event(
        (f"{record_type}s/updated" if existing_item else f"{record_type}s/created"),
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    if existing_item:
        return None, HTTPStatus.NO_CONTENT.value  # 204
    return item_dict, HTTPStatus.CREATED.value  # 201


@app.delete("/flows/<flowId>")
@tracer.capture_method(capture_response=False)
def delete_flow_by_id(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested Flow ID in the path is invalid."
        ) from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    # Get flow timerange, if timerange is empty delete flow sync, otherwise return a delete request
    flow_timerange = TimeRange.from_str(get_flow_timerange(flow_id))
    if flow_timerange.is_empty():
        source_id = delete_flow(flow_id)
        if source_id:
            publish_event(
                f"{record_type}s/deleted",
                {f"{record_type}_id": flow_id},
                get_event_resources(item),
            )
        # Delete source if no longer referenced by any other flows
        if check_delete_source(source_id):
            publish_event(
                "sources/deleted",
                {"source_id": source_id},
                enhance_resources([f"tams:source:{source_id}"]),
            )
        return None, HTTPStatus.NO_CONTENT.value  # 204
    # Create flow delete-request
    item_dict = {
        **base_delete_request_dict(flow_id, app.current_event.request_context),
        "delete_flow": True,
        "timerange_to_delete": str(flow_timerange),
        "timerange_remaining": str(flow_timerange),
    }
    put_message(del_queue, item_dict)
    merge_delete_request(item_dict)
    return Response(
        status_code=HTTPStatus.ACCEPTED.value,  # 202
        content_type=content_types.APPLICATION_JSON,
        body=model_dump(Deletionrequest(**item_dict)),
        headers={
            "Location": f'https://{app.current_event.request_context.domain_name}{app.current_event.request_context.path.split("/flows/")[0]}/flow-delete-requests/{item_dict["id"]}'
        },
    )


@app.head("/flows/<flowId>/tags")
@app.get("/flows/<flowId>/tags")
@tracer.capture_method(capture_response=False)
def get_flow_tags(flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)]):
    try:
        tags = query_node_tags(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump(Tags(**tags)), HTTPStatus.OK.value  # 200


@app.head("/flows/<flowId>/tags/<name>")
@app.get("/flows/<flowId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def get_flow_tag_value(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
    tag_name: Annotated[str, Path(alias="name")],
):
    try:
        tags = query_node_tags(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError("The requested flow or tag does not exist.") from e  # 404
    if tag_name not in tags:
        raise NotFoundError("The requested flow or tag does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return tags[tag_name], HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def put_flow_tag_value(
    tag_value: Annotated[str, Body()],
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
    tag_name: Annotated[str, Path(alias="name")],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flow_id, username, {f"t.{tag_name}": tag_value}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def delete_flow_tag_value(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
    tag_name: Annotated[str, Path(alias="name")],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    if tag_name not in item["tags"]:
        raise NotFoundError("The requested flow ID in the path is invalid.")  # 404
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flow_id, username, {f"t.{tag_name}": None}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/flows/<flowId>/description")
@app.get("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def get_flow_description(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        description = query_node_property(record_type, flow_id, "description")
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return description, HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def put_flow_description(
    description: Annotated[str, Body()],
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flow_id, username, {"flow.description": description}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def delete_flow_description(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flow_id, username, {"flow.description": None}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/flows/<flowId>/label")
@app.get("/flows/<flowId>/label")
@tracer.capture_method(capture_response=False)
def get_flow_label(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        label = query_node_property(record_type, flowId, "label")
    except ValueError as e:
        raise NotFoundError(
            "The requested Flow does not exist, or does not have a label set."
        ) from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return label, HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/label")
@tracer.capture_method(capture_response=False)
def put_flow_label(
    label: Annotated[str, Body()],
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(record_type, flow_id, username, {"flow.label": label})
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/label")
@tracer.capture_method(capture_response=False)
def delete_flow_label(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(record_type, flow_id, username, {"flow.label": None})
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/flows/<flowId>/flow_collection")
@app.get("/flows/<flowId>/flow_collection")
@tracer.capture_method(capture_response=False)
def get_flow_flow_collection(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        flow_collection = query_flow_collection(flow_id)
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return (
        model_dump(
            Flowcollection([Collectionitem(**item) for item in flow_collection])
        ),
        HTTPStatus.OK.value,
    )  # 200


@app.put("/flows/<flowId>/flow_collection")
@tracer.capture_method(capture_response=False)
def put_flow_flow_collection(
    flow_collection: Flowcollection,
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    if not validate_flow_collection(flow_id, flow_collection):
        raise BadRequestError("Bad request. Invalid flow collection.")  # 400
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_flow_collection(flow_id, username, model_dump(flow_collection))
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/flow_collection")
@tracer.capture_method(capture_response=False)
def delete_flow_flow_collection(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_flow_collection(flow_id, username, [])
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/flows/<flowId>/max_bit_rate")
@app.get("/flows/<flowId>/max_bit_rate")
@tracer.capture_method(capture_response=False)
def get_flow_max_bit_rate(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        max_bit_rate = query_node_property(record_type, flow_id, "max_bit_rate")
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return max_bit_rate, HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/max_bit_rate")
@tracer.capture_method(capture_response=False)
def put_flow_max_bit_rate(
    max_bit_rate: Annotated[int, Body()],
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flow_id, username, {"flow.max_bit_rate": max_bit_rate}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/max_bit_rate")
@tracer.capture_method(capture_response=False)
def delete_flow_max_bit_rate(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flow_id, username, {"flow.max_bit_rate": None}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/flows/<flowId>/avg_bit_rate")
@app.get("/flows/<flowId>/avg_bit_rate")
@tracer.capture_method(capture_response=False)
def get_flow_avg_bit_rate(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        avg_bit_rate = query_node_property(record_type, flow_id, "avg_bit_rate")
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return avg_bit_rate, HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/avg_bit_rate")
@tracer.capture_method(capture_response=False)
def put_flow_avg_bit_rate(
    avg_bit_rate: Annotated[int, Body()],
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flow_id, username, {"flow.avg_bit_rate": avg_bit_rate}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/avg_bit_rate")
@tracer.capture_method(capture_response=False)
def delete_flow_avg_bit_rate(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flow_id, username, {"flow.avg_bit_rate": None}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/flows/<flowId>/read_only")
@app.get("/flows/<flowId>/read_only")
@tracer.capture_method(capture_response=False)
def get_flow_read_only(
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    try:
        read_only = query_node_property(record_type, flow_id, "read_only")
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return bool(read_only), HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/read_only")
@tracer.capture_method(capture_response=False)
def put_flow_read_only(
    read_only: Annotated[bool, Body()],
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    if not check_node_exists(record_type, flow_id):
        raise NotFoundError("The requested flow does not exist.")  # 404
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flow_id, username, {"flow.read_only": read_only}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.post("/flows/<flowId>/storage")
@tracer.capture_method(capture_response=False)
def post_flow_storage_by_id(
    flow_storage_post: Flowstoragepost,
    flow_id: Annotated[str, Path(alias="flowId", pattern=UUID_PATTERN)],
):
    if flow_storage_post.limit is None:
        flow_storage_post.limit = constants.DEFAULT_PUT_LIMIT
    try:
        item = query_node(record_type, flow_id)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    flow: Flow = Flow(**item)
    if flow.root.container is None:
        raise BadRequestError(
            "Bad request. Invalid flow storage request JSON or the flow 'container' is not set."
        )  # 400
    flow_storage: Flowstorage = Flowstorage(
        media_objects=[
            get_presigned_put(flow.root.container)
            for _ in range(flow_storage_post.limit)
        ]
    )
    expire_at = int(
        (
            datetime.now() + timedelta(seconds=constants.PRESIGNED_URL_EXPIRES_IN)
        ).timestamp()
    )
    for media_object in flow_storage.media_objects:
        storage_table.put_item(
            Item={
                "object_id": media_object.object_id,
                "flow_id": flow_id,
                "expire_at": expire_at,
            }
        )
    return model_dump(flow_storage), HTTPStatus.CREATED.value  # 201


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
def get_presigned_put(content_type):
    object_id = str(uuid.uuid4())
    url = generate_presigned_url(
        "put_object",
        bucket,
        object_id,
        ContentType=content_type,
    )
    return MediaObject(
        object_id=object_id,
        put_url=Httprequest.model_validate({"url": url, "content-type": content_type}),
    )


@tracer.capture_method(capture_response=False)
def get_event_resources(obj: dict) -> list:
    """Generate a list of event resources for the given flow object."""
    return enhance_resources(
        [
            f'tams:flow:{obj["id"]}',
            f'tams:source:{obj["source_id"]}',
            *set(
                f"tams:flow-collected-by:{c_id}" for c_id in obj.get("collected_by", [])
            ),
        ]
    )
