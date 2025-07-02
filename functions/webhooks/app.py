import os
from collections import defaultdict
from functools import reduce

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.data_classes.event_bridge_event import (
    EventBridgeEvent,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import And, Attr, Key, Or
from dynamodb import (
    get_default_storage_backend,
    get_storage_backend,
    get_store_name,
    webhooks_table,
)
from schema import Webhook
from segment_get_urls import populate_get_urls
from utils import model_dump, put_message

tracer = Tracer()
logger = Logger()
metrics = Metrics()

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
        default_storage_backend = get_default_storage_backend()
        # Add default storage_id if not present when get_urls also not present
        for segment in event.detail["segments"]:
            if not segment.get("storage_ids") and not segment.get("get_urls"):
                segment["storage_ids"] = [default_storage_backend["id"]]
        # Get storage metadata mapping
        storage_mapping = {
            default_storage_backend["id"]: default_storage_backend,
            **{
                storage_id: get_storage_backend(storage_id)
                for segment in event.detail["segments"]
                for storage_id in segment.get("storage_ids", [])
                if storage_id != default_storage_backend["id"]
            },
        }
        # Add ALL get_urls to event
        populate_get_urls(event.detail["segments"], include_storage_id=True)
    for item in schema_items:
        # Not a segments_added event so no further action required just send it.
        if event.detail_type != "flows/segments_added":
            post_event(event, item)
            continue
        get_urls = [*event.detail["segments"][0].get("get_urls", [])]
        # No filtering of the get_urls has been requested so send event with all get_urls
        if (
            item.accept_get_urls is None
            and item.accept_storage_ids is None
            and item.presigned is None
            and item.verbose_storage is None
        ):
            post_event(event, item, get_urls)
            continue
        # No get_urls are requested so send event with no get_urls
        if item.accept_get_urls is not None and len(item.accept_get_urls) == 0:
            post_event(event, item, [])
            continue
        # Filter by label
        if item.accept_get_urls:
            get_urls = [
                get_url
                for get_url in get_urls
                if get_url["label"] in item.accept_get_urls
            ]
        # Filter by storage_id
        if item.accept_storage_ids:
            get_urls = [
                get_url
                for get_url in get_urls
                if get_url.get("storage_id") in item.accept_storage_ids
            ]
        # Filter by presigned
        if item.presigned is not None:
            get_urls = [
                get_url
                for get_url in get_urls
                if get_url.get("presigned", False) == item.presigned
            ]
        # Add verbose storage
        if item.verbose_storage:
            get_urls = [
                {
                    **get_url,
                    **storage_mapping.get(get_url.get("storage_id"), {}),
                    **({"controlled": True} if get_url.get("storage_id") else {}),
                }
                for get_url in get_urls
            ]
        post_event(event, item, get_urls)
