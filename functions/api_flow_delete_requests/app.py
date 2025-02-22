from http import HTTPStatus

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, CORSConfig
from aws_lambda_powertools.event_handler.exceptions import NotFoundError
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from neptune import query_delete_requests, query_node
from schema import Deletionrequest
from utils import model_dump

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(enable_validation=True, cors=CORSConfig())
metrics = Metrics(namespace="Powertools")
record_type = "delete_request"


@app.get("/flow-delete-requests")
@tracer.capture_method(capture_response=False)
def get_flow_delete_requests():
    items = query_delete_requests()
    return (
        model_dump([Deletionrequest(**item) for item in items]),
        HTTPStatus.OK.value,
    )  # 200


@app.head("/flow-delete-requests/<requestId>")
@app.get("/flow-delete-requests/<requestId>")
@tracer.capture_method(capture_response=False)
def get_flow_delete_requests_by_id(requestId: str):
    try:
        item = query_node(record_type, requestId)
    except ValueError as e:
        raise NotFoundError(
            "The requested flow delete request does not exist."
        ) from e  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return (
            None,
            HTTPStatus.OK.value,
        )  # 200
    deletion_request: Deletionrequest = Deletionrequest(**item)
    return model_dump(deletion_request), HTTPStatus.OK.value  # 200


@logger.inject_lambda_context(
    log_event=True, correlation_id_path=correlation_paths.API_GATEWAY_REST
)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)
