import os
from collections import defaultdict
from functools import reduce

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer, single_metric
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.data_classes.event_bridge_event import (
    EventBridgeEvent,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import And, Attr, Key, Or
from requests import Session
from requests.adapters import HTTPAdapter, Retry
from schema import Flow, Flowsegment, Source, Webhook
from utils import generate_presigned_url, model_dump

tracer = Tracer()
logger = Logger()
metrics = Metrics(namespace="Powertools")

dynamodb = boto3.resource("dynamodb")
webhooks_table = dynamodb.Table(os.environ["WEBHOOKS_TABLE"])
api_id = os.environ["API_ID"]
stage_name = os.environ["STAGE_NAME"]
bucket = os.environ["BUCKET"]
bucket_region = os.environ["BUCKET_REGION"]

attribute_mappings = {
    "flow": "flow_ids",
    "source": "source_ids",
    "flow-collected-by": "flow_collected_by_ids",
    "source-collected-by": "source_collected_by_ids",
}

store_name = (
    boto3.client("apigateway")
    .get_stage(restApiId=api_id, stageName=stage_name)["variables"]
    .get("name", "example-store-name")
)


@tracer.capture_method(capture_response=False)
def get_matching_webhooks(event):
    expressions = defaultdict(list)
    for resource in event.resources:
        _, resource_type, resource_id = resource.split(":")
        expressions[attribute_mappings[resource_type]].append(resource_id)
    filter_expressions = []
    for attr, id_list in expressions.items():
        id_expressions = [Attr(attr).contains(id_list[0])]
        for r_id in id_list[1:]:
            id_expressions.append(Attr(attr).contains(r_id))
        id_expressions.append(Attr(attr).not_exists())
        filter_expressions.append(reduce(Or, id_expressions))
    args = {"KeyConditionExpression": Key("event").eq(event.detail_type)}
    if len(filter_expressions) > 0:
        args["FilterExpression"] = reduce(And, filter_expressions)
    query = webhooks_table.query(**args)
    items = query["Items"]
    while "LastEvaluatedKey" in query:
        query = webhooks_table.query(
            **args,
            ExclusiveStartKey=query["LastEvaluatedKey"],
        )
        items.extend(query["Items"])
    return [Webhook(**item, events=[item["event"]]) for item in items]


@tracer.capture_method(capture_response=False)
def post_event(event, item, get_urls):
    retries = Retry(
        total=5,
        backoff_factor=0.1,
        status_forcelist=[500],
        allowed_methods=["POST"],
    )
    s = Session()
    headers = {"Content-Type": "application/json"}
    if item.api_key_name and item.api_key_value:
        headers[item.api_key_name] = item.api_key_value
    s.mount(item.url, HTTPAdapter(max_retries=retries))
    # Use associated model to clean the response data
    match event.detail_type:
        case "flows/created" | "flows/updated":
            event.detail["flow"] = model_dump(Flow(**event.detail["flow"]))
        case "source/created" | "source/updated":
            event.detail["source"] = model_dump(Source(**event.detail["source"]))
        case "flows/segments_added":
            event.detail["segments"][0]["get_urls"] = get_urls
            event.detail["segments"][0] = model_dump(
                Flowsegment(**event.detail["segments"][0])
            )
    response = s.post(
        item.url,
        headers=headers,
        json={
            "event_timestamp": event.time,
            "event_type": event.detail_type,
            "event": event.detail,
        },
        timeout=30,
    )
    with single_metric(
        namespace="Powertools",
        name=f"StatusCode-{response.status_code}",
        unit=MetricUnit.Count,
        value=1,
    ) as metric:
        metric.add_dimension(name="url", value=item.url)
        metric.add_dimension(name="event_type", value=event.detail_type)
    logger.info(f"Status Code: {response.status_code}")
    logger.info(response.text)


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
# pylint: disable=unused-argument
def lambda_handler(event: dict, context: LambdaContext):
    event: EventBridgeEvent = EventBridgeEvent(event)
    query = webhooks_table.query(
        KeyConditionExpression=Key("event").eq(event.detail_type),
    )
    items = query["Items"]
    while "LastEvaluatedKey" in query:
        query = webhooks_table.query(
            KeyConditionExpression=Key("event").eq(event.detail_type),
            ExclusiveStartKey=query["LastEvaluatedKey"],
        )
        items.extend(query["Items"])
    schema_items = get_matching_webhooks(event)
    if event.detail_type == "flows/segments_added":
        need_presigned_urls = any(
            item.accept_get_urls is None
            or any(":s3.presigned:" in label for label in (item.accept_get_urls or []))
            for item in schema_items
        )
        get_urls = event.detail["segments"][0].get("get_urls", [])
        if need_presigned_urls:
            get_urls.append(
                {
                    "label": f"aws.{bucket_region}:s3.presigned:{store_name}",
                    "url": generate_presigned_url(
                        "get_object", bucket, event.detail["segments"][0]["object_id"]
                    ),
                }
            )
    for item in schema_items:
        if event.detail_type != "flows/segments_added":
            post_event(event, item, None)
        elif (
            item.accept_get_urls is None
        ):  # Explictly check for None since empty list is falsey
            post_event(event, item, get_urls)
        else:
            # Only post the get_urls with matching label(s)
            accepted_labels = set(item.accept_get_urls)
            post_event(
                event,
                item,
                list(
                    filter(
                        lambda u, accepted=accepted_labels: u["label"] in accepted,
                        get_urls,
                    )
                ),
            )
