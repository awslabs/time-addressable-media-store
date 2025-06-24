import os
from collections import defaultdict
from functools import reduce

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.data_classes.event_bridge_event import (
    EventBridgeEvent,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import And, Attr, Key, Or
from dynamodb import get_store_name, webhooks_table
from schema import Webhook
from utils import generate_presigned_url, model_dump, put_message

tracer = Tracer()
logger = Logger()
metrics = Metrics()

bucket = os.environ["BUCKET"]
bucket_region = os.environ["BUCKET_REGION"]
webhooks_queue = os.environ["WEBHOOKS_QUEUE_URL"]
store_name = get_store_name()


attribute_mappings = {
    "flow": "flow_ids",
    "source": "source_ids",
    "flow-collected-by": "flow_collected_by_ids",
    "source-collected-by": "source_collected_by_ids",
}


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
        args["ExclusiveStartKey"] = query["LastEvaluatedKey"]
        query = webhooks_table.query(**args)
        items.extend(query["Items"])
    return [Webhook(**item, events=[item["event"]]) for item in items]


@tracer.capture_method(capture_response=False)
def post_event(event, item, get_urls=None):
    put_message(
        webhooks_queue,
        {"event": event.raw_event, "item": model_dump(item), "get_urls": get_urls},
    )


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
# pylint: disable=unused-argument
def lambda_handler(event: EventBridgeEvent, context: LambdaContext):
    event = EventBridgeEvent(event)
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
            post_event(event, item)
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
