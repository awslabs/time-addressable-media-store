import json
import os
import uuid
from datetime import datetime
from http import HTTPStatus

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
from aws_lambda_powertools.event_handler.openapi.params import Path
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from dynamodb import get_flow_timerange
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
    Deletionrequest,
    Flow,
    Flowcollection,
    Flowstorage,
    Flowstoragepost,
    Httprequest,
    MediaObject,
    Tags,
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
    publish_event,
    put_message,
    validate_query_string,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(enable_validation=True, cors=CORSConfig())
metrics = Metrics(namespace="Powertools")

record_type = "flow"
bucket = os.environ["BUCKET"]
del_queue = os.environ["DELETE_QUEUE_URL"]

UUID_PATTERN = Uuid.model_fields["root"].metadata[0].pattern


@app.head("/flows")
@app.get("/flows")
@tracer.capture_method(capture_response=False)
def get_flows():
    parameters = app.current_event.query_string_parameters
    if not validate_query_string(parameters, app.current_event.request_context):
        raise BadRequestError("Bad request. Invalid query options.")  # 400
    custom_headers = {}
    if parameters and "limit" in parameters:
        custom_headers["X-Paging-Limit"] = parameters["limit"]
    items, next_page = query_flows(parameters)
    if parameters and "timerange" in parameters:
        timerange_filter = TimeRange.from_str(parameters["timerange"])
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
def get_flow_by_id(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    parameters = app.current_event.query_string_parameters
    if not validate_query_string(parameters, app.current_event.request_context):
        raise BadRequestError("Bad request. Invalid query options.")  # 400
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if parameters and "include_timerange" in parameters:
        item["timerange"] = get_flow_timerange(flowId)
    # Update timerange if timerange parameter supplied
    if parameters and "timerange" in parameters and "include_timerange" in parameters:
        timerange_filter = TimeRange.from_str(parameters["timerange"])
        item["timerange"] = str(
            timerange_filter.intersect_with(TimeRange.from_str(item["timerange"]))
        )
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump(Flow(**item)), HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>")
@tracer.capture_method(capture_response=False)
def put_flow_by_id(flow: Flow, flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    if flow.root.id != flowId:
        raise NotFoundError("The requested Flow ID in the path is invalid.")  # 404
    try:
        existing_item = query_node(record_type, flowId)
        if existing_item.get("read_only"):
            raise ServiceError(
                403,
                "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
            )  # 403
    except ValueError:
        existing_item = {}
    if not validate_flow_collection(flowId, flow.root.flow_collection):
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
def delete_flow_by_id(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        item = query_node(record_type, flowId)
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
    flow_timerange = TimeRange.from_str(get_flow_timerange(flowId))
    if flow_timerange.is_empty():
        source_id = delete_flow(flowId)
        if source_id:
            publish_event(
                f"{record_type}s/deleted",
                {f"{record_type}_id": flowId},
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
        **base_delete_request_dict(flowId, app.current_event.request_context),
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
def get_flow_tags(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        tags = query_node_tags(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump(Tags(**tags)), HTTPStatus.OK.value  # 200


@app.head("/flows/<flowId>/tags/<name>")
@app.get("/flows/<flowId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def get_flow_tag_value(flowId: Annotated[str, Path(pattern=UUID_PATTERN)], name: str):
    try:
        tags = query_node_tags(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested flow or tag does not exist.") from e  # 404
    if name not in tags:
        raise NotFoundError("The requested flow or tag does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return tags[name], HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def put_flow_tag_value(flowId: Annotated[str, Path(pattern=UUID_PATTERN)], name: str):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid flow tag value.")  # 400
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(record_type, flowId, username, {f"t.{name}": body})
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def delete_flow_tag_value(
    flowId: Annotated[str, Path(pattern=UUID_PATTERN)], name: str
):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    if name not in item["tags"]:
        raise NotFoundError("The requested flow ID in the path is invalid.")  # 404
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(record_type, flowId, username, {f"t.{name}": None})
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/flows/<flowId>/description")
@app.get("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def get_flow_description(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        description = query_node_property(record_type, flowId, "description")
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return description, HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def put_flow_description(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid flow description.")  # 400
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flowId, username, {"flow.description": body}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def delete_flow_description(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        item = query_node(record_type, flowId)
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
        record_type, flowId, username, {"flow.description": None}
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
def put_flow_label(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, str):
        raise BadRequestError("Bad request. Invalid flow label.")  # 400
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(record_type, flowId, username, {"flow.label": body})
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/label")
@tracer.capture_method(capture_response=False)
def delete_flow_label(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        item = query_node(record_type, flowId)
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
    item_dict = set_node_property(record_type, flowId, username, {"flow.label": None})
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/flows/<flowId>/flow_collection")
@app.get("/flows/<flowId>/flow_collection")
@tracer.capture_method(capture_response=False)
def get_flow_flow_collection(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        flow_collection = query_flow_collection(flowId)
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
    flowId: Annotated[str, Path(pattern=UUID_PATTERN)],
):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    if not validate_flow_collection(flowId, flow_collection):
        raise BadRequestError("Bad request. Invalid flow collection.")  # 400
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_flow_collection(flowId, username, model_dump(flow_collection))
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/flow_collection")
@tracer.capture_method(capture_response=False)
def delete_flow_flow_collection(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        item = query_node(record_type, flowId)
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
    item_dict = set_flow_collection(flowId, username, [])
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.head("/flows/<flowId>/max_bit_rate")
@app.get("/flows/<flowId>/max_bit_rate")
@tracer.capture_method(capture_response=False)
def get_flow_max_bit_rate(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        max_bit_rate = query_node_property(record_type, flowId, "max_bit_rate")
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return max_bit_rate, HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/max_bit_rate")
@tracer.capture_method(capture_response=False)
def put_flow_max_bit_rate(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, int):
        raise BadRequestError("Bad request. Invalid flow max bit rate.")  # 400
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flowId, username, {"flow.max_bit_rate": body}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/max_bit_rate")
@tracer.capture_method(capture_response=False)
def delete_flow_max_bit_rate(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        item = query_node(record_type, flowId)
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
        record_type, flowId, username, {"flow.max_bit_rate": None}
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
def get_flow_avg_bit_rate(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        avg_bit_rate = query_node_property(record_type, flowId, "avg_bit_rate")
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return avg_bit_rate, HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/avg_bit_rate")
@tracer.capture_method(capture_response=False)
def put_flow_avg_bit_rate(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if item.get("read_only"):
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, int):
        raise BadRequestError("Bad request. Invalid flow avg bit rate.")  # 400
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flowId, username, {"flow.avg_bit_rate": body}
    )
    publish_event(
        f"{record_type}s/updated",
        {record_type: item_dict},
        get_event_resources(item_dict),
    )
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/avg_bit_rate")
@tracer.capture_method(capture_response=False)
def delete_flow_avg_bit_rate(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        item = query_node(record_type, flowId)
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
        record_type, flowId, username, {"flow.avg_bit_rate": None}
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
def get_flow_read_only(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    try:
        read_only = query_node_property(record_type, flowId, "read_only")
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return bool(read_only), HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/read_only")
@tracer.capture_method(capture_response=False)
def put_flow_read_only(flowId: Annotated[str, Path(pattern=UUID_PATTERN)]):
    if not check_node_exists(record_type, flowId):
        raise NotFoundError("The requested flow does not exist.")  # 404
    try:
        body = json.loads(app.current_event.body)
    except json.decoder.JSONDecodeError:
        body = None
    if not isinstance(body, bool):
        raise BadRequestError(
            "Bad request. Invalid flow read_only value. Value must be boolean."
        )  # 400
    username = get_username(parse_claims(app.current_event.request_context))
    item_dict = set_node_property(
        record_type, flowId, username, {"flow.read_only": body}
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
    flowId: Annotated[str, Path(pattern=UUID_PATTERN)],
):
    if flow_storage_post.limit is None:
        flow_storage_post.limit = constants.DEFAULT_PUT_LIMIT
    try:
        item = query_node(record_type, flowId)
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
    return model_dump(flow_storage), HTTPStatus.CREATED.value  # 201


@logger.inject_lambda_context(
    log_event=True, correlation_id_path=correlation_paths.API_GATEWAY_REST
)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)


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
