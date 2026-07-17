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
from neptune import query_delete_requests, query_node
from schema import Deletionrequest
from schema_extra import DeleteRequestsSortBy
from typing_extensions import Annotated
from utils import generate_link_url, model_dump

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(
    enable_validation=True, cors=CORSConfig(expose_headers=["*"])
)
metrics = Metrics()
record_type = "delete_request"


@app.head("/flow-delete-requests")
@app.get("/flow-delete-requests")
@tracer.capture_method(capture_response=False)
def get_flow_delete_requests(
    param_reverse_order: Annotated[Optional[bool], Query(alias="reverse_order")] = None,
    param_sort_by: Annotated[
        Optional[DeleteRequestsSortBy], Query(alias="sort_by")
    ] = None,
    param_page: Annotated[Optional[str], Query(alias="page")] = None,
    param_limit: Annotated[Optional[int], Query(alias="limit", gt=0)] = None,
):
    reverse_order = bool(param_reverse_order)
    custom_headers = {}
    items, next_page, limit_used = query_delete_requests(
        {
            "reverse_order": reverse_order,
            "sort_by": param_sort_by.value if param_sort_by else None,
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
            content_type=content_types.APPLICATION_JSON,
            body=None,
            headers=custom_headers,
        )
    return Response(
        status_code=HTTPStatus.OK.value,  # 200
        content_type=content_types.APPLICATION_JSON,
        body=model_dump([Deletionrequest(**item) for item in items]),
        headers=custom_headers,
    )


@app.head("/flow-delete-requests/<requestId>")
@app.get("/flow-delete-requests/<requestId>")
@tracer.capture_method(capture_response=False)
def get_flow_delete_requests_by_id(request_id: Annotated[str, Path(alias="requestId")]):
    try:
        item = query_node(record_type, request_id)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow delete request does not exist."
        ) from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return None, HTTPStatus.OK.value  # 200
    deletion_request: Deletionrequest = Deletionrequest(**item)
    return model_dump(deletion_request), HTTPStatus.OK.value  # 200


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
