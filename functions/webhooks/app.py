import os

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.data_classes.event_bridge_event import (
    EventBridgeEvent,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from dynamodb import get_default_storage_backend, get_storage_backend, get_store_name
from neptune import get_matching_webhooks, set_node_property_base
from segment_get_urls import populate_get_urls
from utils import filter_dict, model_dump, put_message

tracer = Tracer()
logger = Logger()
metrics = Metrics()

webhooks_queue = os.environ["WEBHOOKS_QUEUE_URL"]
store_name = get_store_name()


@tracer.capture_method(capture_response=False)
def post_event(
    event, item, get_urls=None, init_get_urls=None, include_object_timerange=False
):
    put_message(
        webhooks_queue,
        {
            "event": event.raw_event,
            "item": model_dump(item),
            "get_urls": get_urls,
            "init_get_urls": init_get_urls,
            "include_object_timerange": include_object_timerange,
        },
    )


@tracer.capture_method(capture_response=False)
def filter_webhook_get_urls(get_urls, item, storage_mapping):
    """Apply a webhook's get_urls filters (accept_get_urls, accept_storage_ids,
    presigned, verbose_storage) to a list of get_urls, mirroring the
    /flows/{flowId}/segments query parameter behaviour. Used for both the Flow
    Segment `get_urls` and the `init_object.get_urls`."""
    # Filter by label
    if item.accept_get_urls:
        get_urls = [
            get_url for get_url in get_urls if get_url["label"] in item.accept_get_urls
        ]
    # Filter by storage_id
    if item.accept_storage_ids:
        accept_storage_ids = [storage_id.root for storage_id in item.accept_storage_ids]
        get_urls = [
            get_url
            for get_url in get_urls
            if get_url.get("storage_id") in accept_storage_ids
        ]
    # Filter by presigned
    if item.presigned is not None:
        get_urls = [
            get_url
            for get_url in get_urls
            if get_url.get("presigned", False) == item.presigned
        ]
    # Add verbose storage, or remove storage_id if verbose_storage not requested
    if item.verbose_storage:
        return [
            {
                **get_url,
                **storage_mapping.get(get_url.get("storage_id"), {}),
                **({"controlled": True} if get_url.get("storage_id") else {}),
            }
            for get_url in get_urls
        ]
    return [filter_dict(get_url, {"storage_id"}) for get_url in get_urls]


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
# pylint: disable=unused-argument
def lambda_handler(event: EventBridgeEvent, context: LambdaContext):
    event = EventBridgeEvent(event)
    schema_items = get_matching_webhooks(event)
    storage_mapping = {}
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
        # object_timerange is only stored when it differs from the segment
        # timerange; fall back to the segment timerange so it can be included
        # when a webhook requests it. Inclusion is decided per-webhook below.
        for segment in event.detail["segments"]:
            segment.setdefault("object_timerange", segment["timerange"])
    for item in schema_items:
        # Update status to started if created
        if item.status.value == "created":
            set_node_property_base(
                "webhook", item.id.root, {"webhook.status": "started"}
            )
        # Not a segments_added event so no further action required just send it.
        if event.detail_type != "flows/segments_added":
            post_event(event, item)
            continue
        segment = event.detail["segments"][0]
        get_urls = [*segment.get("get_urls", [])]
        # init_object.get_urls (if present) are filtered identically to the
        # Segment's own get_urls; None when the Segment has no init Object.
        init_object = segment.get("init_object")
        init_get_urls = [*init_object.get("get_urls", [])] if init_object else None
        # No filtering of the get_urls has been requested so send event with all get_urls
        if (
            item.accept_get_urls is None
            and item.accept_storage_ids is None
            and item.presigned is None
            and item.verbose_storage is None
        ):
            # Remove storage_id since no verbose_storage requested
            post_event(
                event,
                item,
                [filter_dict(get_url, {"storage_id"}) for get_url in get_urls],
                (
                    [filter_dict(get_url, {"storage_id"}) for get_url in init_get_urls]
                    if init_get_urls is not None
                    else None
                ),
                bool(item.include_object_timerange),
            )
            continue
        # No get_urls are requested so send event with no get_urls
        if item.accept_get_urls is not None and len(item.accept_get_urls) == 0:
            post_event(
                event,
                item,
                [],
                [] if init_get_urls is not None else None,
                bool(item.include_object_timerange),
            )
            continue
        post_event(
            event,
            item,
            filter_webhook_get_urls(get_urls, item, storage_mapping),
            (
                filter_webhook_get_urls(init_get_urls, item, storage_mapping)
                if init_get_urls is not None
                else None
            ),
            bool(item.include_object_timerange),
        )
