import base64
import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import boto3
import pytest
from aws_lambda_powertools.event_handler.exceptions import BadRequestError
from boto3.dynamodb.conditions import ConditionExpressionBuilder, Key
from botocore.exceptions import ClientError

# pylint: disable=no-name-in-module
from conftest import parse_dynamo_expression
from mediatimestamp.immutable import TimeRange, Timestamp
from schema import Flowsegmentpost

pytestmark = [
    pytest.mark.unit,
]

builder = ConditionExpressionBuilder()

MS_INCLUDE_START = 1
MS_INCLUDE_END = 2

with patch("boto3.client"):
    with patch("boto3.resource"):
        import constants
        import dynamodb


class TestDynamoDB:
    @patch("dynamodb.publish_event")
    @patch("dynamodb.segments_table")
    def test_delete_segment_items(self, mock_segments_table, mock_publish_event):
        items = [
            {
                "flow_id": "1",
                "timerange_end": "1",
                "object_id": "123",
                "timerange": "123",
            },
            {
                "flow_id": "2",
                "timerange_end": "1",
                "object_id": "456",
                "timerange": "123",
            },
            {
                "flow_id": "3",
                "timerange_end": "2",
                "object_id": "789",
                "timerange": "123",
            },
        ]

        delete_item_results = [{}, {"Attributes": []}, {"Attributes": []}]
        mock_segments_table.delete_item.side_effect = delete_item_results
        expected_success_count = len(
            [n for n in delete_item_results if "Attributes" in n]
        )

        object_ids = set()
        result = dynamodb.delete_segment_items(items, object_ids)

        assert mock_segments_table.delete_item.call_count == len(items)
        assert len(object_ids) == expected_success_count
        assert mock_publish_event.call_count == expected_success_count
        assert result is None

    @patch("dynamodb.segments_table")
    def test_delete_segment_items_returns_exception(self, mock_segments_table):
        items = [
            {
                "flow_id": "1",
                "timerange_end": "1",
                "object_id": "XXX",
                "timerange": "123",
            }
        ]

        error_code = "404"
        error_message = "help"
        mock_segments_table.delete_item.side_effect = ClientError(
            {
                "Error": {
                    "Code": error_code,
                    "Message": error_message,
                },
                "ResponseMetadata": {},
            },
            "delete_item",
        )

        result = dynamodb.delete_segment_items(items, set())

        assert result is not None
        assert result["type"] == error_code
        assert result["summary"] == error_message

    @patch("dynamodb.segments_table")
    def test_get_flow_timerange_with_first_and_last(self, mock_segments_table):
        now = datetime.now()
        first = TimeRange(
            start=Timestamp.from_datetime(now + timedelta(hours=1)),
            end=Timestamp.from_datetime(now + timedelta(hours=2)),
        )
        last = TimeRange(
            start=Timestamp.from_datetime(now + timedelta(hours=6)),
            end=Timestamp.from_datetime(now + timedelta(hours=7)),
        )

        first_segment_result = {"Items": [{"timerange": first.to_sec_nsec_range()}]}

        last_segment_result = {"Items": [{"timerange": last.to_sec_nsec_range()}]}

        mock_segments_table.query.side_effect = [
            first_segment_result,
            last_segment_result,
        ]

        expected_timetrange = first.extend_to_encompass_timerange(last)

        result = dynamodb.get_flow_timerange("123")
        assert expected_timetrange.to_sec_nsec_range() == result

    @patch("dynamodb.segments_table")
    def test_get_flow_timerange_with_first_only(self, mock_segments_table):
        now = datetime.now()
        first = TimeRange(
            start=Timestamp.from_datetime(now + timedelta(hours=1)),
            end=Timestamp.from_datetime(now + timedelta(hours=2)),
        )

        first_segment_result = {"Items": [{"timerange": first.to_sec_nsec_range()}]}

        last_segment_result = {"Items": []}

        mock_segments_table.query.side_effect = [
            first_segment_result,
            last_segment_result,
        ]

        result = dynamodb.get_flow_timerange("123")
        assert first.to_sec_nsec_range() == result

    @patch("dynamodb.segments_table")
    def test_get_flow_timerange_with_last_only(self, mock_segments_table):
        now = datetime.now()
        last = TimeRange(
            start=Timestamp.from_datetime(now + timedelta(hours=1)),
            end=Timestamp.from_datetime(now + timedelta(hours=2)),
        )

        first_segment_result = {"Items": []}

        last_segment_result = {"Items": [{"timerange": last.to_sec_nsec_range()}]}

        mock_segments_table.query.side_effect = [
            first_segment_result,
            last_segment_result,
        ]

        result = dynamodb.get_flow_timerange("123")
        assert last.to_sec_nsec_range() == result

    @pytest.mark.parametrize(
        "boundary, inclusivity, conditionType",
        [
            (
                [
                    dynamodb.TimeRangeBoundary.START,
                    MS_INCLUDE_START,
                    boto3.dynamodb.conditions.LessThan,
                ]
            ),
            (
                [
                    dynamodb.TimeRangeBoundary.START,
                    MS_INCLUDE_END,
                    boto3.dynamodb.conditions.LessThanEquals,
                ]
            ),
            (
                [
                    dynamodb.TimeRangeBoundary.END,
                    MS_INCLUDE_START,
                    boto3.dynamodb.conditions.GreaterThanEquals,
                ]
            ),
            (
                [
                    dynamodb.TimeRangeBoundary.END,
                    MS_INCLUDE_END,
                    boto3.dynamodb.conditions.GreaterThan,
                ]
            ),
        ],
    )
    def test_get_timerange_expression(self, boundary, inclusivity, conditionType):
        now = datetime.now()
        time_range = TimeRange(
            start=Timestamp.from_datetime(now + timedelta(hours=1)),
            end=Timestamp.from_datetime(now + timedelta(hours=2)),
            inclusivity=inclusivity,
        )
        other_boundary = (
            dynamodb.TimeRangeBoundary.START
            if boundary == dynamodb.TimeRangeBoundary.END
            else dynamodb.TimeRangeBoundary.END
        )

        result = dynamodb.get_timerange_expression(Key, boundary, time_range)

        expected_value = getattr(time_range, other_boundary.value).to_nanosec()
        # Form the expected result. We LessThan here regardless of inputs, but it doesn't impact the test
        expected_condition = Key(f"timerange_{boundary.value}").lt(expected_value)

        _, expected_names, expected_values = parse_dynamo_expression(expected_condition)
        _, actual_names, actual_values = parse_dynamo_expression(result)

        assert isinstance(result, conditionType)
        assert set(sorted(expected_names.values())) == set(
            sorted(actual_names.values())
        )
        assert set(sorted(expected_values.values())) == set(
            sorted(actual_values.values())
        )

    def test_get_key_and_args(self):
        flow_id = "test-flow"
        parameters = {}

        result = dynamodb.get_key_and_args(flow_id, parameters)

        assert (
            result["KeyConditionExpression"].get_expression()
            == Key("flow_id").eq(flow_id).get_expression()
        )
        assert result["ScanIndexForward"] is True
        assert result["Limit"] == constants.DEFAULT_PAGE_LIMIT
        assert "FilterExpression" not in result

    def test_get_key_and_args_pagination(self):
        """Test query with pagination parameters"""
        flow_id = "test-flow"
        parameters = {"limit": 5, "page": "1234567890"}

        result = dynamodb.get_key_and_args(flow_id, parameters)

        assert result["Limit"] == 5
        assert result["ExclusiveStartKey"] == {
            "flow_id": flow_id,
            "timerange_end": int(parameters["page"]),
        }

    def test_get_key_and_args_max_limit(self):
        flow_id = "test-flow"
        parameters = {"limit": constants.MAX_PAGE_LIMIT + 100}

        result = dynamodb.get_key_and_args(flow_id, parameters)

        assert result["Limit"] == constants.MAX_PAGE_LIMIT

    def test_get_key_and_args_reverse_order(self):
        flow_id = "test-flow"
        parameters = {"reverse_order": True}

        result = dynamodb.get_key_and_args(flow_id, parameters)

        assert result["ScanIndexForward"] is False

    def test_get_key_and_args_object_id_filter(self):
        flow_id = "test-flow"
        parameters = {"object_id": "test-object"}

        result = dynamodb.get_key_and_args(flow_id, parameters)

        _, actual_names, actual_values = parse_dynamo_expression(
            result["KeyConditionExpression"]
        )

        assert isinstance(
            result["KeyConditionExpression"], boto3.dynamodb.conditions.And
        )
        assert result["IndexName"] == "object-id-index"
        assert set(["flow_id", "object_id"]) == set(sorted(actual_names.values()))
        assert set([flow_id, parameters["object_id"]]) == set(
            sorted(actual_values.values())
        )

    def test_get_key_and_args_with_timerange(self):
        flow_id = "test-flow"

        now = datetime.now()
        time_range = TimeRange(
            start=Timestamp.from_datetime(now + timedelta(hours=1)),
            end=Timestamp.from_datetime(now + timedelta(hours=2)),
        )

        parameters = {"timerange": time_range.to_sec_nsec_range()}

        result = dynamodb.get_key_and_args(flow_id, parameters)

        assert "FilterExpression" in result

    def test_get_key_and_args_with_eternity(self):
        flow_id = "test-flow"

        time_range = TimeRange.eternity()

        parameters = {"timerange": time_range.to_sec_nsec_range()}

        result = dynamodb.get_key_and_args(flow_id, parameters)

        assert "FilterExpression" not in result

    @patch("dynamodb.segments_table")
    @patch("dynamodb.merge_delete_request")
    def test_delete_flow_segments_empty_result(
        self, merge_delete_request, mock_segments_table, time_range_one_day
    ):
        mock_segments_table.query.return_value = {"Items": []}

        dynamodb.delete_flow_segments(
            flow_id="test-flow",
            parameters={},
            timerange_to_delete=time_range_one_day,
            context=MagicMock(),
            s3_queue="s3-queue",
            del_queue="del-queue",
            item_dict={},
        )

        assert mock_segments_table.query.called
        assert merge_delete_request.called
        assert merge_delete_request.call_args[0][0]["status"] == "done"

    @pytest.mark.parametrize(
        "remaining_time_variance",
        [
            -1000,  # Below threshold
            1000,  # Above threshold
        ],
    )
    @patch("dynamodb.segments_table")
    @patch("dynamodb.delete_segment_items")
    @patch("dynamodb.merge_delete_request")
    def test_delete_flow_segments_timeout_handling(
        self,
        _,
        mock_delete_segment_items,
        mock_segments_table,
        time_range_one_day,
        remaining_time_variance,
    ):
        remaining_time = constants.LAMBDA_TIME_REMAINING + remaining_time_variance
        mock_lambda_context = MagicMock()
        mock_lambda_context.get_remaining_time_in_millis.return_value = remaining_time

        mock_segments_table.query.side_effect = [
            {
                "Items": [
                    {
                        "flow_id": "1",
                        "timerange": time_range_one_day.to_sec_nsec_range(),
                    }
                ],
                "LastEvaluatedKey": {"key": "value"},
            },
            {
                "Items": [
                    {
                        "flow_id": "1",
                        "timerange": time_range_one_day.to_sec_nsec_range(),
                    }
                ]
            },
        ]
        mock_delete_segment_items.return_value = None

        dynamodb.delete_flow_segments(
            flow_id="test-flow",
            parameters={},
            timerange_to_delete=time_range_one_day,
            context=mock_lambda_context,
            s3_queue="s3-queue",
            del_queue="del-queue",
            item_dict={},
        )

        if remaining_time <= constants.LAMBDA_TIME_REMAINING:
            assert mock_segments_table.query.call_count == 1
        else:
            assert mock_segments_table.query.call_count == 2

    @patch("dynamodb.delete_segment_items")
    @patch("dynamodb.segments_table")
    @patch("dynamodb.merge_delete_request")
    def test_delete_flow_segments_error_handling(
        self,
        mock_merge_delete_request,
        mock_segments_table,
        mock_delete_segment_items,
        time_range_one_day,
    ):
        mock_delete_segment_items.return_value = "Error deleting segments"
        mock_segments_table.query.return_value = {
            "Items": [
                {"flow_id": "1", "timerange": time_range_one_day.to_sec_nsec_range()}
            ]
        }

        dynamodb.delete_flow_segments(
            flow_id="test-flow",
            parameters={},
            timerange_to_delete=time_range_one_day,
            context=MagicMock(),
            s3_queue="s3-queue",
            del_queue="del-queue",
            item_dict={},
        )

        # Verify error status was
        assert mock_merge_delete_request.called
        assert mock_merge_delete_request.call_args[0][0]["status"] == "error"

    @patch("dynamodb.segments_table")
    def test_get_exact_timerange_end_first_item_differs(self, mock_segments_table):
        flow_id = "test-flow"
        time_range_end = 789

        first_item_timerange_end = 456
        return_items = {
            "Items": [
                {"timerange_end": first_item_timerange_end},
                {"timerange_end": time_range_end},
            ]
        }

        mock_segments_table.query.return_value = return_items

        result = dynamodb.get_exact_timerange_end(flow_id, time_range_end)

        assert result == first_item_timerange_end

    @patch("dynamodb.segments_table")
    def test_get_exact_timerange_end_first_item_matches(self, mock_segments_table):
        flow_id = "test-flow"
        time_range_end = 789

        return_items = {
            "Items": [
                {"timerange_end": time_range_end},
                {"timerange_end": time_range_end + 1},
            ]
        }

        mock_segments_table.query.return_value = return_items

        result = dynamodb.get_exact_timerange_end(flow_id, time_range_end)

        assert result == time_range_end

    @patch("dynamodb.storage_table")
    def test_validate_object_id_object_id_not_found(self, mock_storage_table):
        segment = Flowsegmentpost(object_id="abc", timerange="_")
        flow_id = "123"

        return_item = {}
        mock_storage_table.get_item.return_value = return_item

        result, _, _ = dynamodb.validate_object_id(segment, flow_id)

        assert 0 == mock_storage_table.update_item.call_count
        assert not result

    @patch("dynamodb.storage_table")
    def test_validate_object_id_matched_flow_id_expire_present(
        self, mock_storage_table
    ):
        segment = Flowsegmentpost(object_id="abc", timerange="_")
        flow_id = "123"

        return_item = {"Item": {"id": "abc", "flow_id": "123", "expire_at": 12345}}
        mock_storage_table.get_item.return_value = return_item

        result, _, _ = dynamodb.validate_object_id(segment, flow_id)

        assert 1 == mock_storage_table.update_item.call_count
        assert result

    @patch("dynamodb.storage_table")
    def test_validate_object_id_matched_flow_id_expire_not_present(
        self, mock_storage_table
    ):
        segment = Flowsegmentpost(object_id="abc", timerange="_")
        flow_id = "123"

        return_item = {"Item": {"id": "abc", "flow_id": "123"}}
        mock_storage_table.get_item.return_value = return_item

        result, _, _ = dynamodb.validate_object_id(segment, flow_id)

        assert 0 == mock_storage_table.update_item.call_count
        assert result

    @patch("dynamodb.storage_table")
    def test_validate_object_id_not_matched_flow_id_expire_present(
        self, mock_storage_table
    ):
        segment = Flowsegmentpost(object_id="abc", timerange="_")
        flow_id = "456"

        return_item = {"Item": {"id": "abc", "flow_id": "123", "expire_at": 12345}}
        mock_storage_table.get_item.return_value = return_item

        result, _, _ = dynamodb.validate_object_id(segment, flow_id)

        assert 0 == mock_storage_table.update_item.call_count
        assert not result

    @patch("dynamodb.storage_table")
    def test_validate_object_id_not_matched_flow_id_expire_not_present(
        self, mock_storage_table
    ):
        segment = Flowsegmentpost(object_id="abc", timerange="_")
        flow_id = "456"

        return_item = {"Item": {"id": "abc", "flow_id": "123"}}
        mock_storage_table.get_item.return_value = return_item

        result, _, _ = dynamodb.validate_object_id(segment, flow_id)

        assert 0 == mock_storage_table.update_item.call_count
        assert result

    @patch("dynamodb.segments_table")
    @patch("dynamodb.storage_table")
    def test_delete_flow_storage_record_delete(
        self, mock_storage_table, mock_segments_table
    ):
        mock_segments_table.query.return_value = {"Count": 0}

        dynamodb.delete_flow_storage_record("abc", "123")

        assert 1 == mock_storage_table.delete_item.call_count
        assert 0 == mock_storage_table.get_item.call_count
        assert 0 == mock_storage_table.update_item.call_count

    @patch("dynamodb.segments_table")
    @patch("dynamodb.storage_table")
    def test_delete_flow_storage_record_update_storage_id_present(
        self, mock_storage_table, mock_segments_table
    ):
        mock_segments_table.query.return_value = {"Count": 1}
        mock_storage_table.get_item.return_value = {
            "Item": {"id": "abc", "storage_id": "123"},
        }

        dynamodb.delete_flow_storage_record("abc", "123")

        assert 0 == mock_storage_table.delete_item.call_count
        assert 1 == mock_storage_table.get_item.call_count
        assert 1 == mock_storage_table.update_item.call_count

    @patch("dynamodb.segments_table")
    @patch("dynamodb.storage_table")
    def test_delete_flow_storage_record_update_storage_id_not_present(
        self, mock_storage_table, mock_segments_table
    ):
        mock_segments_table.query.return_value = {"Count": 1}
        mock_storage_table.get_item.return_value = {
            "Item": {"id": "abc", "storage_id": "456"},
        }

        dynamodb.delete_flow_storage_record("abc", "123")

        assert 0 == mock_storage_table.delete_item.call_count
        assert 1 == mock_storage_table.get_item.call_count
        assert 0 == mock_storage_table.update_item.call_count

    def test_decode_and_validate_page_valid(self):
        """Test decode_and_validate_page with valid base64 encoded pagination key"""
        object_id = "test-object-123"
        page_data = {
            "flow_id": "test-flow",
            "object_id": object_id,
            "timerange_end": 1234567890,
        }
        encoded_page = base64.b64encode(json.dumps(page_data).encode("utf-8")).decode(
            "utf-8"
        )

        result = dynamodb.decode_and_validate_page(encoded_page, object_id)

        assert result == page_data

    def test_decode_and_validate_page_invalid_base64(self):
        """Test decode_and_validate_page with invalid base64 encoding"""
        object_id = "test-object-123"
        invalid_page = "not-valid-base64!!!"

        with pytest.raises(BadRequestError, match="Invalid page parameter value"):
            dynamodb.decode_and_validate_page(invalid_page, object_id)

    def test_decode_and_validate_page_invalid_json(self):
        """Test decode_and_validate_page with invalid JSON content"""
        object_id = "test-object-123"
        invalid_json = base64.b64encode(b"not valid json").decode("utf-8")

        with pytest.raises(BadRequestError, match="Invalid page parameter value"):
            dynamodb.decode_and_validate_page(invalid_json, object_id)

    def test_decode_and_validate_page_missing_flow_id(self):
        """Test decode_and_validate_page with missing flow_id field"""
        object_id = "test-object-123"
        page_data = {"object_id": object_id, "timerange_end": 1234567890}
        encoded_page = base64.b64encode(json.dumps(page_data).encode("utf-8")).decode(
            "utf-8"
        )

        with pytest.raises(BadRequestError, match="Invalid page parameter value"):
            dynamodb.decode_and_validate_page(encoded_page, object_id)

    def test_decode_and_validate_page_missing_object_id(self):
        """Test decode_and_validate_page with missing object_id field"""
        object_id = "test-object-123"
        page_data = {"flow_id": "test-flow", "timerange_end": 1234567890}
        encoded_page = base64.b64encode(json.dumps(page_data).encode("utf-8")).decode(
            "utf-8"
        )

        with pytest.raises(BadRequestError, match="Invalid page parameter value"):
            dynamodb.decode_and_validate_page(encoded_page, object_id)

    def test_decode_and_validate_page_missing_timerange_end(self):
        """Test decode_and_validate_page with missing timerange_end field"""
        object_id = "test-object-123"
        page_data = {"flow_id": "test-flow", "object_id": object_id}
        encoded_page = base64.b64encode(json.dumps(page_data).encode("utf-8")).decode(
            "utf-8"
        )

        with pytest.raises(BadRequestError, match="Invalid page parameter value"):
            dynamodb.decode_and_validate_page(encoded_page, object_id)

    def test_decode_and_validate_page_mismatched_object_id(self):
        """Test decode_and_validate_page with object_id mismatch"""
        object_id = "test-object-123"
        different_object_id = "different-object-456"
        page_data = {
            "flow_id": "test-flow",
            "object_id": different_object_id,
            "timerange_end": 1234567890,
        }
        encoded_page = base64.b64encode(json.dumps(page_data).encode("utf-8")).decode(
            "utf-8"
        )

        with pytest.raises(BadRequestError, match="Invalid page parameter value"):
            dynamodb.decode_and_validate_page(encoded_page, object_id)

    @patch("dynamodb.segments_table")
    def test_query_segments_by_object_id_basic(self, mock_segments_table):
        """Test query_segments_by_object_id with basic parameters"""
        object_id = "test-object-123"
        mock_items = [{"object_id": object_id, "flow_id": "flow-1"}]
        mock_segments_table.query.return_value = {"Items": mock_items}

        items, last_key, limit = dynamodb.query_segments_by_object_id(object_id)

        assert items == mock_items
        assert last_key is None
        assert limit == constants.DEFAULT_PAGE_LIMIT
        assert mock_segments_table.query.call_count == 1

    @patch("dynamodb.segments_table")
    def test_query_segments_by_object_id_with_projection(self, mock_segments_table):
        """Test query_segments_by_object_id with projection expression"""
        object_id = "test-object-123"
        projection = "flow_id,timerange"
        mock_segments_table.query.return_value = {"Items": []}

        dynamodb.query_segments_by_object_id(object_id, projection=projection)

        call_kwargs = mock_segments_table.query.call_args[1]
        assert call_kwargs["ProjectionExpression"] == projection

    @patch("dynamodb.segments_table")
    def test_query_segments_by_object_id_with_limit(self, mock_segments_table):
        """Test query_segments_by_object_id with custom limit"""
        object_id = "test-object-123"
        limit = 5
        mock_segments_table.query.return_value = {"Items": []}

        _, _, returned_limit = dynamodb.query_segments_by_object_id(
            object_id, limit=limit
        )

        call_kwargs = mock_segments_table.query.call_args[1]
        assert call_kwargs["Limit"] == limit
        assert returned_limit == limit

    @patch("dynamodb.segments_table")
    def test_query_segments_by_object_id_limit_exceeds_max(self, mock_segments_table):
        """Test query_segments_by_object_id with limit exceeding MAX_PAGE_LIMIT"""
        object_id = "test-object-123"
        limit = constants.MAX_PAGE_LIMIT + 100
        mock_segments_table.query.return_value = {"Items": []}

        _, _, returned_limit = dynamodb.query_segments_by_object_id(
            object_id, limit=limit
        )

        call_kwargs = mock_segments_table.query.call_args[1]
        assert call_kwargs["Limit"] == constants.MAX_PAGE_LIMIT
        assert returned_limit == constants.MAX_PAGE_LIMIT

    @patch("dynamodb.decode_and_validate_page")
    @patch("dynamodb.segments_table")
    def test_query_segments_by_object_id_with_page(
        self, mock_segments_table, mock_decode
    ):
        """Test query_segments_by_object_id with pagination"""
        object_id = "test-object-123"
        page = "encoded-page-token"
        decoded_key = {
            "flow_id": "flow-1",
            "object_id": object_id,
            "timerange_end": 123,
        }
        mock_decode.return_value = decoded_key
        mock_segments_table.query.return_value = {"Items": []}

        dynamodb.query_segments_by_object_id(object_id, page=page)

        mock_decode.assert_called_once_with(page, object_id)
        call_kwargs = mock_segments_table.query.call_args[1]
        assert call_kwargs["ExclusiveStartKey"] == decoded_key

    @patch("dynamodb.segments_table")
    def test_query_segments_by_object_id_with_last_evaluated_key(
        self, mock_segments_table
    ):
        """Test query_segments_by_object_id returns LastEvaluatedKey when limit reached"""
        object_id = "test-object-123"
        limit = 5
        last_key = {"flow_id": "flow-1", "object_id": object_id, "timerange_end": 999}
        mock_segments_table.query.return_value = {
            "Items": [{"id": str(i)} for i in range(limit)],
            "LastEvaluatedKey": last_key,
        }

        _, returned_last_key, _ = dynamodb.query_segments_by_object_id(
            object_id, limit=limit
        )

        assert returned_last_key == last_key

    @patch("dynamodb.segments_table")
    def test_query_segments_by_object_id_fetch_all(self, mock_segments_table):
        """Test query_segments_by_object_id with fetch_all=True"""
        object_id = "test-object-123"
        mock_segments_table.query.side_effect = [
            {"Items": [{"id": "1"}], "LastEvaluatedKey": {"key": "val"}},
            {"Items": [{"id": "2"}]},
        ]

        items, last_key, limit = dynamodb.query_segments_by_object_id(
            object_id, fetch_all=True
        )

        assert len(items) == 2
        assert last_key is None
        assert limit is None
        assert mock_segments_table.query.call_count == 2

    @patch("dynamodb.segments_table")
    def test_query_segments_by_object_id_pagination_until_limit(
        self, mock_segments_table
    ):
        """Test query_segments_by_object_id continues pagination until limit reached"""
        object_id = "test-object-123"
        limit = 10
        mock_segments_table.query.side_effect = [
            {
                "Items": [{"id": str(i)} for i in range(3)],
                "LastEvaluatedKey": {"key": "1"},
            },
            {
                "Items": [{"id": str(i)} for i in range(3, 8)],
                "LastEvaluatedKey": {"key": "2"},
            },
            {"Items": [{"id": str(i)} for i in range(8, 12)]},
        ]

        items, _, _ = dynamodb.query_segments_by_object_id(object_id, limit=limit)

        assert len(items) == 12
        assert mock_segments_table.query.call_count == 3
