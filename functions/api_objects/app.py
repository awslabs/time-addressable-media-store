import base64
import json
import os
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
)
from aws_lambda_powertools.event_handler.openapi.exceptions import (
    RequestValidationError,
)
from aws_lambda_powertools.event_handler.openapi.params import Path, Query
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from dynamodb import segments_table
from schema import Object
from typing_extensions import Annotated
from utils import generate_link_url, get_object_tags, model_dump

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(
    enable_validation=True, cors=CORSConfig(expose_headers=["*"])
)
metrics = Metrics()
bucket = os.environ["BUCKET"]


@app.head("/objects/<objectId>")
@app.get("/objects/<objectId>")
@tracer.capture_method(capture_response=False)
def get_objects_by_id(
    object_id: Annotated[str, Path(alias="objectId")],
    param_page: Annotated[Optional[str], Query(alias="page")] = None,
    param_limit: Annotated[Optional[int], Query(alias="limit")] = None,
):
    object_s3_tags = get_object_tags(bucket, object_id)
    if object_s3_tags is None:
        raise NotFoundError("The requested media object does not exist.")  # 404
    args = get_query_kwargs(
        object_id,
        {
            "limit": param_limit,
            "page": param_page,
        },
    )
    query = segments_table.query(**args)
    items = query["Items"]
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
    if param_limit != args["Limit"]:
        custom_headers["X-Paging-Limit"] = str(args["Limit"])
    if app.current_event.request_context.http_method == "HEAD":
        return Response(
            status_code=HTTPStatus.OK.value,  # 200
            content_type=content_types.APPLICATION_JSON,
            body=None,
            headers=custom_headers,
        )
    schema_item = Object(
        **{
            "object_id": object_id,
            "referenced_by_flows": set([item["flow_id"] for item in items]),
            "first_referenced_by_flow": object_s3_tags.get(
                "first_referenced_by_flow", None
            ),
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


@tracer.capture_method(capture_response=False)
def get_query_kwargs(object_id: str, parameters: dict) -> dict:
    """Generate key expression and args for a dynamodb query operation"""
    kwargs = {
        "IndexName": "object-id-index",
        "KeyConditionExpression": Key("object_id").eq(object_id),
        "Limit": constants.DEFAULT_PAGE_LIMIT,
    }
    # Pagination query string parameters
    if parameters.get("limit"):
        kwargs["Limit"] = min(parameters["limit"], constants.MAX_PAGE_LIMIT)
    if parameters.get("page"):
        kwargs["ExclusiveStartKey"] = json.loads(base64.b64decode(parameters["page"]))
    return kwargs
