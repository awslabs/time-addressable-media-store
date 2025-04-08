import os
import pytest
from unittest.mock import MagicMock, Mock, patch
from schema import Webhookpost, Flow
from requests import Session, Response
from aws_lambda_powertools.utilities.data_classes.event_bridge_event import (
    EventBridgeEvent,
)

os.environ["WEBHOOKS_TABLE"] = 'TEST_TABLE'
os.environ["BUCKET"] = 'TEST_BUCKET'
os.environ["BUCKET_REGION"] = 'eu-west-1'

with patch('boto3.client') as mock_method:
    with patch('boto3.resource') as mock_resource:
        from webhooks import app


class TestWebhooks():

    def test_get_matching_webhooks_single_resource(self, mock_webhooks_table):
        # Setup
        event_type = "flow.created"
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": event_type,
                "resources": ["arn:flow:123"]
            }
        )

        expected_items = [{
            "event": event_type,
            "video_ids": ["123"],
            "url": "https://example.com"
        }]

        mock_webhooks_table.query.return_value = {
            "Items": expected_items
        }

        # Execute
        with patch('webhooks.app.webhooks_table', mock_webhooks_table):
            result = app.get_matching_webhooks(event)

        # Verify
        assert len(result) == 1
        assert result[0].events == [event_type]
        mock_webhooks_table.query.assert_called_once()

    def test_get_matching_webhooks_multiple_resources(self, mock_webhooks_table):
        # Setup
        event_type = "flow.created"
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": event_type,
                "resources": ["arn:flow:123", "arn:flow:456"]
            }
        )

        expected_items = [{
            "event": event_type,
            "video_ids": ["123", "456"],
            "url": "https://example.com"
        }]

        mock_webhooks_table.query.return_value = {
            "Items": expected_items
        }

        # Execute
        with patch('webhooks.app.webhooks_table', mock_webhooks_table):
            result = app.get_matching_webhooks(event)

        # Verify
        assert len(result) == 1
        assert result[0].events == [event_type]

    def test_get_matching_webhooks_pagination(self, mock_webhooks_table):
        # Setup
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": "flow.created",
                "resources": ["arn:flow:123"]
            }
        )

        first_response = {
            "Items": [{"event": "video.created", "video_ids": ["123"], "url": "https://example.com"}],
            "LastEvaluatedKey": "next-page"
        }
        second_response = {
            "Items": [{"event": "video.created", "video_ids": ["123"], "url": "https://example.com"}]
        }

        mock_webhooks_table.query.side_effect = [
            first_response, second_response]

        # Execute
        with patch('webhooks.app.webhooks_table', mock_webhooks_table):
            result = app.get_matching_webhooks(event)

        # Verify
        assert len(result) == 2
        assert mock_webhooks_table.query.call_count == 2

    def test_get_matching_webhooks_no_matches(self, mock_webhooks_table):
        # Setup
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": "video.created",
                "resources": ["arn:flow:123"]
            }
        )

        mock_webhooks_table.query.return_value = {
            "Items": []
        }

        # Execute
        with patch('webhooks.app.webhooks_table', mock_webhooks_table):
            result = app.get_matching_webhooks(event)

        # Verify
        assert len(result) == 0

    def test_post_event_detail_type(self, mock_session, mock_response_ok, stub_flow):
        # Setup
        mock_session.post.return_value = mock_response_ok
        detail_type = "flows/created"
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": detail_type,
                "detail": {"flow": stub_flow}
            }
        )
        item = Webhookpost(url="https://example.com/webhook", events=[])
        get_urls = None

        # Execute
        with patch('webhooks.app.model_dump') as mock_model_dump:
            mock_model_dump.return_value = {"id": "123", "name": "test_flow"}
            app.post_event(event, item, get_urls)

        # Verify
        mock_session.post.assert_called_once()
        called_args = mock_session.post.call_args
        assert called_args[0][0] == "https://example.com/webhook"
        assert called_args[1]["headers"] == {
            "Content-Type": "application/json"}
        assert "event_timestamp" in called_args[1]["json"]
        assert called_args[1]["json"]["event_type"] == detail_type

    def test_post_event_with_api_key(self, mock_session, stub_source, mock_response_ok):
        # Setup
        mock_session.post.return_value = mock_response_ok
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": "source/created",
                "detail": {
                    "source": stub_source
                }
            }
        )
        item = Webhookpost(
            url="https://example.com/webhook",
            api_key_name="X-API-Key",
            api_key_value="secret-key",
            events=[]
        )
        get_urls = None

        # Execute
        with patch('webhooks.app.model_dump') as mock_model_dump:
            mock_model_dump.return_value = {"id": "456", "type": "video"}
            app.post_event(event, item, get_urls)

        # Verify
        mock_session.post.assert_called_once()
        called_args = mock_session.post.call_args
        assert called_args[1]["headers"] == {
            "Content-Type": "application/json",
            "X-API-Key": "secret-key"
        }

    def test_post_event_segments_added(self, mock_session, stub_flow, stub_flowsegment, mock_response_ok):
        # Setup
        mock_session.post.return_value = mock_response_ok
        get_urls = [{"url": "https://example.com/video"}]
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": "flows/segments_added",
                "detail": {
                    "flow": stub_flow,
                    "segments": [stub_flowsegment]
                }
            }
        )
        item = Webhookpost(url="https://example.com/webhook", events=[])

        # Execute
        with patch('webhooks.app.model_dump') as mock_model_dump:
            mock_model_dump.return_value = {
                "id": "789",
                "type": "video",
                "duration": 60,
                "get_urls": get_urls
            }
            app.post_event(event, item, get_urls)

        # Verify
        mock_session.post.assert_called_once()
        called_args = mock_session.post.call_args
        assert "segments" in called_args[1]["json"]["event"]
        assert called_args[1]["json"]["event"]["segments"][0]["get_urls"] == get_urls

    @pytest.mark.parametrize("detail_type,detail_key", [
        ("flows/created", "flow"),
        ("flows/updated", "flow"),
    ])
    def test_post_event_flow_types(self, mock_session, mock_response_ok, stub_flow, detail_type, detail_key):
        # Setup
        mock_session.post.return_value = mock_response_ok
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": detail_type,
                "detail": {
                    "flow": stub_flow
                }
            }
        )
        item = Webhookpost(url="https://example.com/webhook", events=[])

        # Execute
        with patch('webhooks.app.model_dump') as mock_model_dump:
            mock_model_dump.return_value = {"id": "123", "name": "test"}
            app.post_event(event, item, None)

        # Verify
        mock_session.post.assert_called_once()
        called_args = mock_session.post.call_args
        assert called_args[1]["json"]["event_type"] == detail_type

    @pytest.mark.parametrize("detail_type,detail_key", [
        ("source/created", "source"),
        ("source/updated", "source"),
    ])
    def test_post_event_source_types(self, mock_session, mock_response_ok, stub_source, detail_type, detail_key):
        # Setup
        mock_session.post.return_value = mock_response_ok
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": detail_type,
                "detail": {
                    # "source": stub_source,
                    detail_key: {
                        "id": stub_source["id"],
                        "format": stub_source["format"]
                    },
                }
            }
        )
        item = Webhookpost(url="https://example.com/webhook", events=[])

        # Execute
        with patch('webhooks.app.model_dump') as mock_model_dump:
            mock_model_dump.return_value = {"id": "123", "name": "test"}
            app.post_event(event, item, None)

        # Verify
        mock_session.post.assert_called_once()
        called_args = mock_session.post.call_args
        assert called_args[1]["json"]["event_type"] == detail_type

    def test_post_event_timeout(self, stub_flow):
        # Setup
        event = EventBridgeEvent(
            {
                "time": "2023-01-01T00:00:00Z",
                "detail-type": "flows/created",
                "detail": {"flow": stub_flow}
            }
        )
        item = Webhookpost(url="https://example.com/webhook", events=[])

        # Execute and Verify
        with patch('webhooks.app.Session') as mock_session_class:
            session_instance = Mock()
            session_instance.post.side_effect = TimeoutError(
                "Request timed out")
            mock_session_class.return_value = session_instance

            with pytest.raises(TimeoutError):
                app.post_event(event, item, None)
