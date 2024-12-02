import os
from http import HTTPStatus

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, CORSConfig
from aws_lambda_powertools.event_handler.exceptions import NotFoundError
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key
from schema import Deletionrequest
from utils import model_dump_json

tracer = Tracer()
logger = Logger()
app = APIGatewayRestResolver(enable_validation=True, cors=CORSConfig())
metrics = Metrics(namespace="Powertools")


dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE"])
record_type = "delete-request"


@app.get("/flow-delete-requests")
@tracer.capture_method(capture_response=False)
def get_flow_delete_requests():
    query = table.query(KeyConditionExpression=Key("record_type").eq(record_type))
    return (
        model_dump_json([Deletionrequest(**item) for item in query["Items"]]),
        HTTPStatus.OK.value,
    )  # 200


@app.route("/flow-delete-requests/<requestId>", method=["HEAD"])
@app.get("/flow-delete-requests/<requestId>")
@tracer.capture_method(capture_response=False)
def get_flow_delete_requests_by_id(requestId: str):
    item = table.get_item(
        Key={"record_type": record_type, "id": requestId},
    )
    if "Item" not in item:
        raise NotFoundError("The requested flow delete request does not exist.")  # 404
    if app.current_event.request_context.http_method == "HEAD":
        return (
            None,
            HTTPStatus.OK.value,
        )  # 200
    deletion_request: Deletionrequest = Deletionrequest(**item["Item"])
    return model_dump_json(deletion_request), HTTPStatus.OK.value  # 200


@logger.inject_lambda_context(
    log_event=True, correlation_id_path=correlation_paths.API_GATEWAY_REST
)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)
