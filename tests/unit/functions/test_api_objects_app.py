import base64
import json
import os
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest

pytestmark = [
    pytest.mark.unit,
]

with patch.dict(
    os.environ,
    {
        "POWERTOOLS_LOG_LEVEL": "INFO",
        "POWERTOOLS_SERVICE_NAME": "tams",
        "POWERTOOLS_METRICS_NAMESPACE": "TAMS",
        "AWS_REGION": "eu-west-1",
        "NEPTUNE_ENDPOINT": "example.com",
        "SEGMENTS_TABLE": "example-table",
        "STORAGE_TABLE": "example-table",
        "BUCKET": "test-bucket",
    },
), patch("boto3.resource"), patch("boto3.client"):
    from api_objects import app


class TestGetObjectsById:
    """Test cases for the get_objects_by_id function."""

    @patch("api_objects.app.check_object_exists")
    def test_get_objects_by_id_not_exists(self, mock_check_object_exists):
        # Arrange
        object_id = "nonexistent-object"

        # Mock the object exists check
        mock_check_object_exists.return_value = False

        with pytest.raises(app.NotFoundError) as exc_info:
            # Act
            app.get_objects_by_id(object_id)

        # Assert
        assert str(exc_info.value) == "The requested media object does not exist."
        mock_check_object_exists.assert_called_once_with(app.bucket, object_id)

    @patch("api_objects.app.check_object_exists")
    @patch("api_objects.app.segments_table")
    @patch("api_objects.app.storage_table")
    def test_get_objects_by_id_GET_exists_basic(
        self,
        mock_storage_table,
        mock_segments_table,
        mock_check_object_exists,
    ):
        # Arrange
        object_id = "test-object-123"
        flow_ids = [
            "10000000-0000-1000-8000-000000000000",
            "10000000-0000-1000-8000-000000000001",
        ]

        # Mock the object exists check
        mock_check_object_exists.return_value = True

        # Mock segments table query response
        mock_segments_table.query.return_value = {
            "Items": [{"flow_id": flow_id} for flow_id in flow_ids]
        }

        # Mock storage table query response
        mock_storage_table.query.return_value = {
            "Items": [
                {
                    "object_id": object_id,
                    "flow_id": flow_ids[0],
                    "expire_at": None,
                }
            ]
        }

        # Mock the app's current_event
        mock_current_event = Mock()
        mock_current_event.request_context.http_method = "GET"

        with patch.object(app.app, "current_event", mock_current_event, create=True):
            # Act
            result = app.get_objects_by_id(object_id)

            # Assert
            assert result.status_code == HTTPStatus.OK.value
            assert result.content_type == "application/json"
            assert result.body.get("object_id") == object_id
            assert set(result.body.get("referenced_by_flows")) == set(flow_ids)
            assert result.body.get("first_referenced_by_flow") == flow_ids[0]
            mock_check_object_exists.assert_called_once_with(app.bucket, object_id)
            mock_segments_table.query.assert_called_once()
            mock_storage_table.query.assert_called_once()

    @patch("api_objects.app.check_object_exists")
    @patch("api_objects.app.segments_table")
    @patch("api_objects.app.storage_table")
    def test_get_objects_by_id_GET_exists_limit(
        self,
        mock_storage_table,
        mock_segments_table,
        mock_check_object_exists,
    ):
        # Arrange
        object_id = "test-object-123"
        limit = 1
        flow_ids = [
            "10000000-0000-1000-8000-000000000000",
            "10000000-0000-1000-8000-000000000001",
        ]

        # Mock the object exists check
        mock_check_object_exists.return_value = True

        # Mock segments table query response
        mock_segments_table.query.return_value = {
            "Items": [{"flow_id": flow_ids[0]}],
            "LastEvaluatedKey": {
                "flow_id": "test",
                "timerange_end": 0,
            },
        }

        # Mock storage table query response
        mock_storage_table.query.return_value = {
            "Items": [
                {
                    "object_id": object_id,
                    "flow_id": flow_ids[0],
                    "expire_at": None,
                }
            ]
        }

        # Mock the app's current_event
        mock_current_event = Mock()
        mock_current_event.request_context.http_method = "GET"
        mock_current_event.request_context.domain_name = "test.com"
        mock_current_event.request_context.path = f"/Prod/objects/{object_id}"
        mock_current_event.query_string_parameters = {"limit": str(limit)}

        with patch.object(app.app, "current_event", mock_current_event, create=True):
            # Act
            result = app.get_objects_by_id(object_id, param_limit=limit)

            # Assert
            assert result.status_code == HTTPStatus.OK.value
            assert result.content_type == "application/json"
            assert result.body.get("object_id") == object_id
            assert result.body.get("referenced_by_flows") == [flow_ids[0]]
            assert result.body.get("first_referenced_by_flow") == flow_ids[0]
            assert "X-Paging-NextKey" in result.headers
            assert "Link" in result.headers
            mock_check_object_exists.assert_called_once_with(app.bucket, object_id)
            mock_segments_table.query.assert_called_once()
            mock_storage_table.query.assert_called_once()

    @patch("api_objects.app.check_object_exists")
    @patch("api_objects.app.segments_table")
    @patch("api_objects.app.storage_table")
    def test_get_objects_by_id_GET_exists_limit_page(
        self,
        mock_storage_table,
        mock_segments_table,
        mock_check_object_exists,
    ):
        # Arrange
        object_id = "test-object-123"
        limit = 1
        page = base64.b64encode(
            json.dumps(
                {
                    "flow_id": "test",
                    "timerange_end": 0,
                },
                default=int,
            ).encode("utf-8")
        ).decode("utf-8")
        flow_ids = [
            "10000000-0000-1000-8000-000000000000",
            "10000000-0000-1000-8000-000000000001",
        ]

        # Mock the object exists check
        mock_check_object_exists.return_value = True

        # Mock segments table query response
        mock_segments_table.query.return_value = {
            "Items": [{"flow_id": flow_ids[1]}],
        }

        # Mock storage table query response
        mock_storage_table.query.return_value = {
            "Items": [
                {
                    "object_id": object_id,
                    "flow_id": flow_ids[0],
                    "expire_at": None,
                }
            ]
        }

        # Mock the app's current_event
        mock_current_event = Mock()
        mock_current_event.request_context.http_method = "GET"
        mock_current_event.request_context.domain_name = "test.com"
        mock_current_event.request_context.path = f"/Prod/objects/{object_id}"
        mock_current_event.query_string_parameters = {"limit": str(limit), "page": page}

        with patch.object(app.app, "current_event", mock_current_event, create=True):
            # Act
            result = app.get_objects_by_id(object_id, page, limit)

            # Assert
            assert result.status_code == HTTPStatus.OK.value
            assert result.content_type == "application/json"
            assert result.body.get("object_id") == object_id
            assert result.body.get("referenced_by_flows") == [flow_ids[1]]
            assert result.body.get("first_referenced_by_flow") == flow_ids[0]
            assert "X-Paging-NextKey" not in result.headers
            assert "Link" not in result.headers
            mock_check_object_exists.assert_called_once_with(app.bucket, object_id)
            mock_segments_table.query.assert_called_once()
            mock_storage_table.query.assert_called_once()

    @patch("api_objects.app.check_object_exists")
    @patch("api_objects.app.segments_table")
    @patch("api_objects.app.storage_table")
    def test_get_objects_by_id_HEAD_exists_basic(
        self,
        mock_storage_table,
        mock_segments_table,
        mock_check_object_exists,
    ):
        # Arrange
        object_id = "test-object-123"
        flow_ids = [
            "10000000-0000-1000-8000-000000000000",
            "10000000-0000-1000-8000-000000000001",
        ]

        # Mock the object exists check
        mock_check_object_exists.return_value = True

        # Mock segments table query response
        mock_segments_table.query.return_value = {
            "Items": [{"flow_id": flow_id} for flow_id in flow_ids]
        }

        # Mock the app's current_event
        mock_current_event = Mock()
        mock_current_event.request_context.http_method = "HEAD"

        with patch.object(app.app, "current_event", mock_current_event, create=True):
            # Act
            result = app.get_objects_by_id(object_id)

            # Assert
            assert result.status_code == HTTPStatus.OK.value
            assert result.content_type == "application/json"
            assert result.body is None
            mock_check_object_exists.assert_called_once_with(app.bucket, object_id)
            mock_segments_table.query.assert_called_once()
            mock_storage_table.assert_not_called()


class TestLambdaHandler:
    """Test cases for the lambda_handler function."""

    @patch.object(app.app, "resolve")
    def test_lambda_handler_calls_app_resolve(self, mock_resolve):
        """Test that lambda_handler properly calls app.resolve."""
        # Arrange
        test_event = {"test": "event"}
        test_context = Mock()
        expected_response = {"statusCode": 200, "body": "test"}
        mock_resolve.return_value = expected_response

        # Act
        result = app.lambda_handler(test_event, test_context)

        # Assert
        assert result == expected_response
        mock_resolve.assert_called_once_with(test_event, test_context)


class TestExceptionHandler:
    """Test cases for the exception handler."""

    def test_handle_validation_error_raises_bad_request(self):
        """Test that validation errors are converted to BadRequestError."""
        # Arrange
        mock_validation_error = Mock()
        mock_validation_error.errors.return_value = ["error1", "error2"]

        # Act & Assert
        with pytest.raises(app.BadRequestError):
            app.handle_validation_error(mock_validation_error)
