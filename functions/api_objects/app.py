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
from aws_lambda_powertools.event_handler.openapi.params import Path, Query
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from dynamodb import get_object_id_query_kwargs, segments_table, storage_table
from schema import Object
from typing_extensions import Annotated
from utils import generate_link_url, model_dump

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(
    enable_validation=True, cors=CORSConfig(expose_headers=["*"])
)
metrics = Metrics()


@app.head("/objects/<objectId>")
@app.get("/objects/<objectId>")
@tracer.capture_method(capture_response=False)
def get_objects_by_id(
    object_id: Annotated[str, Path(alias="objectId")],
    param_page: Annotated[Optional[str], Query(alias="page")] = None,
    param_limit: Annotated[Optional[int], Query(alias="limit", gt=0)] = None,
):
    args = get_object_id_query_kwargs(
        object_id,
        {
            "limit": param_limit,
            "page": param_page,
        },
    )
    query = segments_table.query(**args)
    items = query["Items"]
    if len(items) == 0 and param_page is None:
        raise NotFoundError("The requested media object does not exist.")  # 404
    custom_headers = {}
    while "LastEvaluatedKey" in query and len(items) < args["Limit"]:
        args["ExclusiveStartKey"] = query["LastEvaluatedKey"]
        query = segments_table.query(**args)
        items.extend(query["Items"])
    if "LastEvaluatedKey" in query:
        next_key = base64.b64encode(
            json.dumps(query["LastEvaluatedKey"], default=int).encode("utf-8")
        ).decode("utf-8")
        custom_headers["X-Paging-NextKey"] = next_key
        custom_headers["Link"] = generate_link_url(app.current_event, next_key)
    # Set Paging Limit header if paging limit being used is not the one specified
    if "LastEvaluatedKey" in query or param_limit != args["Limit"]:
        custom_headers["X-Paging-Limit"] = str(args["Limit"])
    if app.current_event.request_context.http_method == "HEAD":
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            content_type=content_types.APPLICATION_JSON,
            body=None,
            headers=custom_headers,
        )
    object_query = storage_table.query(KeyConditionExpression=Key("id").eq(object_id))
    valid_object_items = [
        item for item in object_query["Items"] if item.get("expire_at") is None
    ]
    first_referenced_by_flow = (
        None if len(valid_object_items) == 0 else valid_object_items[0]["flow_id"]
    )
    schema_item = Object(
        **{
            "id": object_id,
            "referenced_by_flows": set([item["flow_id"] for item in items]),
            "first_referenced_by_flow": first_referenced_by_flow,
        }
    )
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=model_dump(schema_item),
        headers=custom_headers,
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
