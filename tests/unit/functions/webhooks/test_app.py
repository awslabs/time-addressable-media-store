import os
import pytest
from unittest.mock import MagicMock, Mock, patch
from schema import Webhookpost, Flow, Webhook
from requests import Session, Response
from aws_lambda_powertools.utilities.data_classes.event_bridge_event import (
    EventBridgeEvent,
)
from undecorated import undecorated
import json
from boto3.dynamodb.conditions import ConditionExpressionBuilder
from conftest import parse_dynamo_expression

builder = ConditionExpressionBuilder()

with patch('boto3.client'):
    with patch('boto3.resource'):
        with patch('utils.get_store_name', lambda: 'tams'):
            from webhooks import app


class TestWebhooks():

    @pytest.mark.parametrize("resources", [
        (["arn:flow:123"]),  # One type, one id
        (["arn:flow:123", "arn:flow:456"]),  # One type, multiple ids
        (["arn:flow:123", "arn:source:789"]),  # Multiple types
        # Multiple types, multiple ids
        (["arn:flow:123", "arn:flow:456", "arn:source:789"]),
        (["arn:flow:123", "arn:source:123"]),  # Multiple types, same id
    ])
    @patch('webhooks.app.webhooks_table')
    def test_get_webhooks_filter_expressions(self, webhooks_table, resources):
        ids = set()
        types = []
        for resource in resources:
            _, resource_type, resource_id = resource.split(":")
            ids.add(f'{resource_type}#{resource_id}')
            types.append(resource_type)

        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": "flows/created",
                "resources": resources
            }
        )

        app.get_matching_webhooks(event)

        kw_args = webhooks_table.query.call_args[1]

        expression_string, expression_attribute_names, expression_attribute_values = parse_dynamo_expression(
            kw_args['FilterExpression'])

        # Ensure the expression uses contains once per id
        contains_count = expression_string.count('contains')
        assert contains_count == len(ids)

        # Ensure the expression uses attribute_not_exists once per distinct resource type
        attribute_not_exists_count = expression_string.count(
            'attribute_not_exists')
        assert attribute_not_exists_count == len(set(types))

        # Ensure a value per distinct id (used in contains)
        assert len(expression_attribute_values) == len(ids)

        # Each resource type should be named for each occurence, and once per type for the OR attribute_not_exists
        assert len(expression_attribute_names) == len(types) + len(set(types))

        assert webhooks_table.query.call_count == 1

    @patch('webhooks.app.webhooks_table')
    def test_get_webhooks_key_condition(self, webhooks_table):
        detail_type = "flows/created"
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": detail_type,
                "resources": ["arn:flow:123"]
            }
        )

        app.get_matching_webhooks(event)

        kw_args = webhooks_table.query.call_args[1]
        key_condition_expression = kw_args['KeyConditionExpression'].get_expression(
        )

        assert key_condition_expression['values'][1] == detail_type

    @patch('webhooks.app.webhooks_table')
    def test_get_matching_webhooks_pagination(self, webhooks_table):
        # Setup
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": "flows/created",
                "resources": ["arn:flow:123"]
            }
        )

        first_response = {
            "Items": [{"event": "flows/created", "flow_ids": ["7622bd94-f80d-49fc-a1b0-66a38c929f3e"], "url": "https://example.com"}],
            "LastEvaluatedKey": "next-page"
        }
        second_response = {
            "Items": [{"event": "flows/created", "flow_ids": ["7622bd94-f80d-49fc-a1b0-66a38c929f3e"], "url": "https://example.com"}]
        }

        webhooks_table.query.side_effect = [
            first_response, second_response]

        # Execute
        result = app.get_matching_webhooks(event)

        # Verify
        assert len(result) == 2
        assert webhooks_table.query.call_count == 2

    @patch('webhooks.app.webhooks_table')
    def test_get_matching_webhooks_no_matches(self, webhooks_table):
        # Setup
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": "flows/created",
                "resources": ["arn:flow:123"]
            }
        )

        webhooks_table.query.return_value = {
            "Items": []
        }

        # Execute
        result = app.get_matching_webhooks(event)

        # Verify
        assert len(result) == 0

    @patch('webhooks.app.put_message')
    def test_post_event_calls_correctly(self, mock_put_message):
        get_urls = ["http://example.com/test"]
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": "flows/created",
                "resources": ["arn:flow:123"]
            }
        )

        item = Webhook(url="https://example.com/webhook", events=[])

        mock_put_message.return_value = {}

        app.post_event(event, item, get_urls)

        assert mock_put_message.call_count == 1

        call_args = mock_put_message.call_args[0]

        assert call_args[0] == os.environ["WEBHOOKS_QUEUE_URL"]
        assert call_args[1] == {
            "event": event.raw_event,
            "item": app.model_dump(item),
            "get_urls": get_urls
        }

    @pytest.mark.parametrize("webhook_count", [1, 2, 3, 4, 5])
    @patch('webhooks.app.post_event')
    def test_handler_posts_for_each_webhook(self, post_event, webhook_count):
        detail_type = "flows/created"

        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": detail_type,
                "resources": ["arn:flow:123"]
            }
        )

        return_items = []
        for i in range(webhook_count):
            return_items.append(Webhook(
                url=f"https://example.com/webhook{i}",
                events=[]
            ))

        # obtain undecorated lambda_handler and call
        unwrapped_handler = undecorated(app.lambda_handler)

        with patch('webhooks.app.get_matching_webhooks', return_value=return_items):
            unwrapped_handler(event, MagicMock())

        assert post_event.call_count == len(return_items)
