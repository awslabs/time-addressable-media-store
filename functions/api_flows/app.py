import json
import os
import uuid
from datetime import datetime
from http import HTTPStatus

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
    ServiceError,
)
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from mediatimestamp.immutable import TimeRange
from schema import (
    Deletionrequest,
    Flow,
    Flowstorage,
    Flowstoragepost,
    Httprequest,
    MediaObject,
    Source,
    Tags,
)
from utils import (
    base_delete_request_dict,
    check_delete_source,
    check_node_exists,
    delete_flow,
    generate_link_url,
    generate_presigned_url,
    get_flow_timerange,
    get_username,
    merge_flow,
    merge_source,
    model_dump,
    model_dump_json,
    publish_event,
    put_deletion_request,
    query_flows,
    query_node,
    query_node_property,
    query_node_tags,
    set_node_property,
    validate_query_string,
)

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(enable_validation=True, cors=CORSConfig())
metrics = Metrics(namespace="Powertools")

dynamodb = boto3.resource("dynamodb")
segments_table = dynamodb.Table(os.environ["SEGMENTS_TABLE"])
record_type = "flow"
bucket = os.environ["BUCKET"]
del_queue = os.environ["DELETE_QUEUE_URL"]


@app.route("/flows", method=["HEAD"])
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
                if TimeRange.from_str(
                    get_flow_timerange(segments_table, item["id"])
                ).is_empty()
            ]
        else:
            items = [
                item
                for item in items
                if not TimeRange.from_str(
                    get_flow_timerange(segments_table, item["id"])
                )
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
        body=model_dump_json([Flow(**item) for item in items]),
        headers=custom_headers,
    )


@app.route("/flows/<flowId>", method=["HEAD"])
@app.get("/flows/<flowId>")
@tracer.capture_method(capture_response=False)
def get_flow_by_id(flowId: str):
    parameters = app.current_event.query_string_parameters
    if not validate_query_string(parameters, app.current_event.request_context):
        raise BadRequestError("Bad request. Invalid query options.")  # 400
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if parameters and "include_timerange" in parameters:
        item["timerange"] = get_flow_timerange(segments_table, flowId)
    # Update timerange if timerange parameter supplied
    if parameters and "timerange" in parameters and "include_timerange" in parameters:
        timerange_filter = TimeRange.from_str(parameters["timerange"])
        item["timerange"] = str(
            timerange_filter.intersect_with(TimeRange.from_str(item["timerange"]))
        )
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return model_dump_json(Flow(**item)), HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>")
@tracer.capture_method(capture_response=False)
def put_flow_by_id(flow: Flow, flowId: str):
    if flow.root.id != flowId:
        raise NotFoundError("The requested Flow ID in the path is invalid.")  # 404
    try:
        existing_item = query_node(record_type, flowId)
        if "read_only" in existing_item and existing_item["read_only"]:
            raise ServiceError(
                403,
                "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
            )  # 403
    except ValueError:
        existing_item = None
    if flow.root.flow_collection:
        for collection in flow.root.flow_collection:
            if not check_node_exists(record_type, collection.id):
                raise BadRequestError(
                    "The supplied value for flow_collection references flowId(s) that do not exist"
                )  # 400
    request_item = model_dump(flow)
    # API spec states these fields should be ignored if given in a PUT request.
    for field in ["created", "metadata_updated", "collected_by"]:
        request_item.pop(field, None)
    merged_item = (
        Flow(**{**existing_item, **request_item})
        if existing_item
        else Flow(**request_item)
    )
    now = datetime.now()
    if merged_item.root.created:
        merged_item.root.metadata_updated = now
    else:
        merged_item.root.created = now
    # Set these if not supplied
    username = get_username(app.current_event.request_context)
    if not merged_item.root.created_by:
        merged_item.root.created_by = username
    if not merged_item.root.updated_by and existing_item:
        merged_item.root.updated_by = username
    if not check_node_exists("source", merged_item.root.source_id):
        source: Source = Source(**model_dump(flow))
        source.id = flow.root.source_id
        merge_source(model_dump(source))
    item_dict = model_dump(merged_item)
    merge_flow(item_dict)
    publish_event(
        (f"{record_type}s/updated" if existing_item else f"{record_type}s/created"),
        {record_type: item_dict},
        [flowId],
    )
    if existing_item:
        return None, HTTPStatus.NO_CONTENT.value  # 204
    return model_dump_json(merged_item), HTTPStatus.CREATED.value  # 201


@app.delete("/flows/<flowId>")
@tracer.capture_method(capture_response=False)
def delete_flow_by_id(flowId: str):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError(
            "The requested Flow ID in the path is invalid."
        ) from e  # 404
    if "read_only" in item and item["read_only"]:
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    # Get flow timerange, if timerange is empty delete flow sync, otherwise return a delete request
    flow_timerange = TimeRange.from_str(get_flow_timerange(segments_table, flowId))
    if flow_timerange.is_empty():
        source_id = delete_flow(flowId)
        if source_id:
            publish_event(
                f"{record_type}s/deleted", {f"{record_type}_id": flowId}, [flowId]
            )
        # Delete source if no longer referenced by any other flows
        check_delete_source(source_id)
        return None, HTTPStatus.NO_CONTENT.value  # 204
    # Create flow delete-request
    item_dict = {
        **base_delete_request_dict(flowId, app.current_event.request_context),
        "delete_flow": True,
        "timerange_to_delete": str(flow_timerange),
        "timerange_remaining": str(flow_timerange),
    }
    put_deletion_request(del_queue, item_dict)
    return Response(
        status_code=HTTPStatus.ACCEPTED.value,  # 202
        content_type=content_types.APPLICATION_JSON,
        body=model_dump_json(Deletionrequest(**item_dict)),
        headers={
            "Location": f'https://{app.current_event.request_context.domain_name}{app.current_event.request_context.path.split("/flows/")[0]}/flow-delete-requests/{item_dict["id"]}'
        },
    )


@app.route("/flows/<flowId>/tags", method=["HEAD"])
@app.get("/flows/<flowId>/tags")
@tracer.capture_method(capture_response=False)
def get_flow_tags(flowId: str):
    try:
        tags = query_node_tags(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return (
        model_dump_json(Tags(**tags)),
        HTTPStatus.OK.value,
    )  # 200


@app.route("/flows/<flowId>/tags/<name>", method=["HEAD"])
@app.get("/flows/<flowId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def get_flow_tag_value(flowId: str, name: str):
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
def put_flow_tag_value(flowId: str, name: str):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if "read_only" in item and item["read_only"]:
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
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(record_type, flowId, username, {f"t.{name}": body})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/tags/<name>")
@tracer.capture_method(capture_response=False)
def delete_flow_tag_value(flowId: str, name: str):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if "read_only" in item and item["read_only"]:
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    if name not in item["tags"]:
        raise NotFoundError("The requested flow ID in the path is invalid.")  # 404
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(record_type, flowId, username, {f"t.{name}": None})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.route("/flows/<flowId>/description", method=["HEAD"])
@app.get("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def get_flow_description(flowId: str):
    try:
        description = query_node_property(record_type, flowId, "description")
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    if description is None:
        return None, HTTPStatus.OK.value  # 200
    return description, HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def put_flow_description(flowId: str):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if "read_only" in item and item["read_only"]:
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
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(
        record_type, flowId, username, {"flow.description": body}
    )
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/description")
@tracer.capture_method(capture_response=False)
def delete_flow_description(flowId: str):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if "read_only" in item and item["read_only"]:
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(
        record_type, flowId, username, {"flow.description": None}
    )
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.route("/flows/<flowId>/label", method=["HEAD"])
@app.get("/flows/<flowId>/label")
@tracer.capture_method(capture_response=False)
def get_flow_label(flowId: str):
    try:
        label = query_node_property(record_type, flowId, "label")
    except ValueError as e:
        raise NotFoundError(
            "The requested Flow does not exist, or does not have a label set."
        ) from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    if label is None:
        return None, HTTPStatus.OK.value  # 200
    return label, HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/label")
@tracer.capture_method(capture_response=False)
def put_flow_label(flowId: str):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested Flow does not exist.") from e  # 404
    if "read_only" in item and item["read_only"]:
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
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(record_type, flowId, username, {"flow.label": body})
    publish_event("sources/updated", {"source": item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.delete("/flows/<flowId>/label")
@tracer.capture_method(capture_response=False)
def delete_flow_label(flowId: str):
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow ID in the path is invalid."
        ) from e  # 404
    if "read_only" in item and item["read_only"]:
        raise ServiceError(
            403,
            "Forbidden. You do not have permission to modify this flow. It may be marked read-only.",
        )  # 403
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(record_type, flowId, username, {"flow.label": None})
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.route("/flows/<flowId>/read_only", method=["HEAD"])
@app.get("/flows/<flowId>/read_only")
@tracer.capture_method(capture_response=False)
def get_flow_read_only(flowId: str):
    try:
        read_only = query_node_property(record_type, flowId, "read_only")
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    return bool(read_only), HTTPStatus.OK.value  # 200


@app.put("/flows/<flowId>/read_only")
@tracer.capture_method(capture_response=False)
def put_flow_read_only(flowId: str):
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
    username = get_username(app.current_event.request_context)
    item_dict = set_node_property(
        record_type, flowId, username, {"flow.read_only": body}
    )
    publish_event(f"{record_type}s/updated", {record_type: item_dict}, [flowId])
    return None, HTTPStatus.NO_CONTENT.value  # 204


@app.post("/flows/<flowId>/storage")
@tracer.capture_method(capture_response=False)
def post_flow_storage_by_id(flow_storage_post: Flowstoragepost, flowId: str):
    if flow_storage_post.limit is None:
        flow_storage_post.limit = constants.DEFAULT_PUT_LIMIT
    try:
        item = query_node(record_type, flowId)
    except ValueError as e:
        raise NotFoundError("The requested flow does not exist.") from e  # 404
    if "read_only" in item and item["read_only"]:
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
    return (
        flow_storage.model_dump_json(by_alias=True, exclude_unset=True),
        HTTPStatus.CREATED.value,
    )  # 201


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
        {"ContentType": content_type},
    )
    return MediaObject(
        object_id=object_id,
        put_url=Httprequest.model_validate({"url": url, "content-type": content_type}),
    )
