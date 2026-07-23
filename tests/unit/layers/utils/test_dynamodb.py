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

    @patch("dynamodb.publish_event")
    @patch("dynamodb.segments_table")
    def test_delete_segment_items_captures_init_object(
        self, mock_segments_table, mock_publish_event
    ):
        items = [
            {
                "flow_id": "1",
                "timerange_end": "1",
                "object_id": "media-1",
                "storage_ids": ["s-1"],
                "init_object_id": "init-1",
                "init_storage_ids": ["s-init"],
                "timerange": "123",
            },
        ]
        mock_segments_table.delete_item.return_value = {"Attributes": []}

        object_ids = set()
        dynamodb.delete_segment_items(items, object_ids)

        assert ("media-1", ("s-1",)) in object_ids
        assert ("init-1", ("s-init",)) in object_ids
        assert 1 == mock_publish_event.call_count

    @patch("dynamodb.enhance_resources")
    @patch("dynamodb.publish_event")
    @patch("dynamodb.segments_table")
    def test_delete_segment_items_uses_supplied_resources(
        self, mock_segments_table, mock_publish_event, mock_enhance_resources
    ):
        """When resources are supplied (Flow being deleted), they are used
        verbatim for the flows/segments_deleted event and enhance_resources is
        not called (the Flow no longer exists to resolve them from)."""
        items = [
            {
                "flow_id": "1",
                "timerange_end": "1",
                "object_id": "media-1",
                "timerange": "123",
            },
        ]
        mock_segments_table.delete_item.return_value = {"Attributes": []}
        resources = ["tams:flow:1", "tams:source:src-1"]

        dynamodb.delete_segment_items(items, set(), resources)

        assert 0 == mock_enhance_resources.call_count
        assert resources == mock_publish_event.call_args[0][2]

    @patch("dynamodb.enhance_resources")
    @patch("dynamodb.publish_event")
    @patch("dynamodb.segments_table")
    def test_delete_segment_items_falls_back_to_enhance_resources(
        self, mock_segments_table, mock_publish_event, mock_enhance_resources
    ):
        """When no resources are supplied (segment-only deletion, Flow still
        exists), resources are resolved live via enhance_resources."""
        items = [
            {
                "flow_id": "1",
                "timerange_end": "1",
                "object_id": "media-1",
                "timerange": "123",
            },
        ]
        mock_segments_table.delete_item.return_value = {"Attributes": []}
        mock_enhance_resources.return_value = ["tams:flow:1", "tams:source:src-1"]

        dynamodb.delete_segment_items(items, set())

        assert 1 == mock_enhance_resources.call_count
        assert ["tams:flow:1"] == mock_enhance_resources.call_args[0][0]

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

    @patch("dynamodb.segments_table")
    def test_get_key_and_args_with_timerange(self, mock_segments_table):
        flow_id = "test-flow"

        now = datetime.now()
        end_ns = Timestamp.from_datetime(now + timedelta(hours=2)).to_nanosec()
        # The both-bounds path probes get_exact_timerange_end for the upper key
        # bound; return an exact-match so upper == end_ns.
        mock_segments_table.query.return_value = {"Items": [{"timerange_end": end_ns}]}
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
    def test_get_key_and_args_both_bounds_bounds_key_window(self, mock_segments_table):
        """A timerange with both a start and an end bounds the sort key on both
        sides (BETWEEN) so pagination cannot scan past the requested range, and
        retains the end boundary as a FilterExpression."""
        flow_id = "test-flow"
        start_ns = 1 * 3600 * 1_000_000_000  # 1h
        end_ns = 2 * 3600 * 1_000_000_000  # 2h
        # get_exact_timerange_end probes the first segment with
        # timerange_end >= end_ns; return an exact-match so upper == end_ns.
        mock_segments_table.query.return_value = {"Items": [{"timerange_end": end_ns}]}
        time_range = TimeRange(
            start=Timestamp.from_nanosec(start_ns),
            end=Timestamp.from_nanosec(end_ns),
        )
        parameters = {"timerange": time_range.to_sec_nsec_range()}

        result = dynamodb.get_key_and_args(flow_id, parameters)

        # Key condition is flow_id = X AND timerange_end BETWEEN start AND end.
        key_expr, key_names, key_values = parse_dynamo_expression(
            result["KeyConditionExpression"]
        )
        assert "BETWEEN" in key_expr
        assert set(["flow_id", "timerange_end"]) == set(key_names.values())
        # start is inclusive so lower == start_ns; upper == end_ns from the probe.
        assert start_ns in key_values.values()
        assert end_ns in key_values.values()
        # The end boundary is retained as a filter on timerange_start.
        _, filter_names, _ = parse_dynamo_expression(result["FilterExpression"])
        assert set(filter_names.values()) == {"timerange_start"}
        assert "IndexName" not in result

    @patch("dynamodb.segments_table")
    def test_get_key_and_args_both_bounds_exclusive_start(self, mock_segments_table):
        """An exclusive start boundary is folded into the BETWEEN lower bound as
        start + 1ns, mirroring how timerange_start is stored on write."""
        flow_id = "test-flow"
        start_ns = 1 * 3600 * 1_000_000_000
        end_ns = 2 * 3600 * 1_000_000_000
        mock_segments_table.query.return_value = {"Items": [{"timerange_end": end_ns}]}
        time_range = TimeRange(
            start=Timestamp.from_nanosec(start_ns),
            end=Timestamp.from_nanosec(end_ns),
            inclusivity=TimeRange.INCLUDE_END,  # exclusive start, inclusive end
        )
        parameters = {"timerange": time_range.to_sec_nsec_range()}

        result = dynamodb.get_key_and_args(flow_id, parameters)

        _, _, key_values = parse_dynamo_expression(result["KeyConditionExpression"])
        assert (start_ns + 1) in key_values.values()

    @patch("dynamodb.segments_table")
    def test_get_key_and_args_empty_range_clamps_between_bounds(
        self, mock_segments_table
    ):
        """An empty range (e.g. "[5:0_5:0)") normalises to () with start == end ==
        0 yet is not eternity, so it reaches the both-bounds branch. The
        exclusive-start fold makes lower = 1 while the empty flow yields upper =
        0, so lower > upper. DynamoDB rejects BETWEEN when lower > upper, so upper
        is clamped up to lower; the retained FilterExpression then yields the
        empty result an empty range must produce (regression test)."""
        instant_ns = 5 * 3600 * 1_000_000_000  # 5h
        # An empty flow: get_exact_timerange_end finds no segment and echoes the
        # requested end back. The empty range normalises to end == 0, so the raw
        # upper is 0 while the folded lower is 1.
        mock_segments_table.query.return_value = {"Items": []}
        time_range = TimeRange(
            start=Timestamp.from_nanosec(instant_ns),
            end=Timestamp.from_nanosec(instant_ns),
            inclusivity=TimeRange.INCLUDE_START,  # empty half-open range "[5:0_5:0)"
        )
        parameters = {"timerange": time_range.to_sec_nsec_range()}

        result = dynamodb.get_key_and_args("test-flow", parameters)

        _, _, key_values = parse_dynamo_expression(result["KeyConditionExpression"])
        between_values = [v for v in key_values.values() if isinstance(v, int)]
        # Both BETWEEN operands collapse to the same clamped value: lower == upper,
        # never lower > upper (which DynamoDB would reject as invalid).
        assert len(between_values) == 2
        assert between_values[0] == between_values[1] == 1

    @patch("dynamodb.segments_table")
    def test_get_key_and_args_object_id_and_timerange_ands_filters(
        self, mock_segments_table
    ):
        """object_id + a both-bounded timerange applies BOTH timerange
        conditions as a single ANDed FilterExpression on the object-id-index
        (regression test: the second condition must not overwrite the first)."""
        flow_id = "test-flow"
        now = datetime.now()
        time_range = TimeRange(
            start=Timestamp.from_datetime(now + timedelta(hours=1)),
            end=Timestamp.from_datetime(now + timedelta(hours=2)),
        )
        parameters = {
            "object_id": "test-object",
            "timerange": time_range.to_sec_nsec_range(),
        }

        result = dynamodb.get_key_and_args(flow_id, parameters)

        assert result["IndexName"] == "object-id-index"
        # The object-id-index sort key is flow_id, so no timerange goes on the key.
        _, key_names, _ = parse_dynamo_expression(result["KeyConditionExpression"])
        assert set(key_names.values()) == {"flow_id", "object_id"}
        # Both timerange conditions survive as a single ANDed filter.
        assert isinstance(result["FilterExpression"], boto3.dynamodb.conditions.And)
        _, filter_names, _ = parse_dynamo_expression(result["FilterExpression"])
        assert set(filter_names.values()) == {"timerange_start", "timerange_end"}
        # get_exact_timerange_end must NOT be called on the object_id path.
        assert not mock_segments_table.query.called

    @patch("dynamodb.segments_table")
    def test_get_key_and_args_object_id_and_start_only_timerange(
        self, mock_segments_table
    ):
        """object_id + a start-only timerange applies a single timerange_end
        filter (not wrapped in an And)."""
        flow_id = "test-flow"
        now = datetime.now()
        time_range = TimeRange.from_start(
            Timestamp.from_datetime(now + timedelta(hours=1))
        )
        parameters = {
            "object_id": "test-object",
            "timerange": time_range.to_sec_nsec_range(),
        }

        result = dynamodb.get_key_and_args(flow_id, parameters)

        assert result["IndexName"] == "object-id-index"
        _, filter_names, _ = parse_dynamo_expression(result["FilterExpression"])
        assert set(filter_names.values()) == {"timerange_end"}

    @patch("dynamodb.segments_table")
    def test_get_key_and_args_object_id_and_end_only_timerange(
        self, mock_segments_table
    ):
        """object_id + an end-only timerange applies a single timerange_start
        filter (regression: previously the timerange was silently ignored)."""
        flow_id = "test-flow"
        now = datetime.now()
        time_range = TimeRange.from_end(
            Timestamp.from_datetime(now + timedelta(hours=2))
        )
        parameters = {
            "object_id": "test-object",
            "timerange": time_range.to_sec_nsec_range(),
        }

        result = dynamodb.get_key_and_args(flow_id, parameters)

        assert result["IndexName"] == "object-id-index"
        assert "FilterExpression" in result
        _, filter_names, _ = parse_dynamo_expression(result["FilterExpression"])
        assert set(filter_names.values()) == {"timerange_start"}

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

    @patch("dynamodb.segments_table")
    @patch("dynamodb.delete_segment_items")
    @patch("dynamodb.merge_delete_request")
    def test_delete_flow_segments_forwards_resources(
        self,
        _,
        mock_delete_segment_items,
        mock_segments_table,
        time_range_one_day,
    ):
        """Pre-resolved resources are forwarded to delete_segment_items so the
        flows/segments_deleted events carry them when the Flow is deleted."""
        mock_segments_table.query.return_value = {
            "Items": [
                {
                    "flow_id": "1",
                    "timerange": time_range_one_day.to_sec_nsec_range(),
                }
            ]
        }
        mock_delete_segment_items.return_value = None
        resources = ["tams:flow:1", "tams:source:src-1"]

        dynamodb.delete_flow_segments(
            flow_id="test-flow",
            parameters={},
            timerange_to_delete=time_range_one_day,
            context=MagicMock(),
            s3_queue="s3-queue",
            del_queue="del-queue",
            item_dict={},
            resources=resources,
        )

        assert resources == mock_delete_segment_items.call_args[0][2]

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

    @patch("dynamodb.put_message")
    @patch("dynamodb.delete_segment_items")
    @patch("dynamodb.segments_table")
    @patch("dynamodb.merge_delete_request")
    def test_delete_flow_segments_empty_final_page_resumes_from_cursor(
        self,
        mock_merge_delete_request,
        mock_segments_table,
        mock_delete_segment_items,
        mock_put_message,
    ):
        """When the final scanned page holds no deletable items but a
        LastEvaluatedKey remains, the continuation must resume from just after
        the last scanned key instead of raising IndexError on an empty page."""
        timerange_to_delete = TimeRange.from_str("[0:0_1000:0)")
        cursor_end = 500 * 1_000_000_000  # last scanned timerange_end (inclusive)
        mock_segments_table.query.side_effect = [
            # First page: has deletable items and a LastEvaluatedKey.
            {
                "Items": [{"flow_id": "1", "timerange": "[0:0_1:0)"}],
                "LastEvaluatedKey": {"flow_id": "1", "timerange_end": 1},
            },
            # Final page: all items filtered/popped out, but more data remains.
            {
                "Items": [],
                "LastEvaluatedKey": {"flow_id": "1", "timerange_end": cursor_end},
            },
        ]
        mock_delete_segment_items.return_value = None
        mock_context = MagicMock()
        # Enter the loop once (time remaining), then exit on the time check so
        # the loop stops on the empty final page while a LastEvaluatedKey is
        # still present (the tail-scan-times-out scenario).
        mock_context.get_remaining_time_in_millis.side_effect = [
            constants.LAMBDA_TIME_REMAINING + 10000,
            constants.LAMBDA_TIME_REMAINING - 1,
        ]
        item_dict = {"id": "dr-1"}

        # Must not raise.
        dynamodb.delete_flow_segments(
            flow_id="test-flow",
            parameters={},
            timerange_to_delete=timerange_to_delete,
            context=mock_context,
            s3_queue="s3-queue",
            del_queue="del-queue",
            item_dict=item_dict,
        )

        # Continuation is queued to resume from cursor + 1ns, not marked done.
        assert mock_put_message.called
        queued = mock_put_message.call_args[0][1]
        expected_remaining = timerange_to_delete.intersect_with(
            TimeRange.from_start(Timestamp.from_nanosec(cursor_end + 1))
        )
        assert queued["timerange_remaining"] == str(expected_remaining)

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

        result = dynamodb.validate_object_id(segment, flow_id)

        assert 0 == mock_storage_table.update_item.call_count
        assert not result["valid"]

    @patch("dynamodb.storage_table")
    def test_validate_object_id_matched_flow_id_expire_present(
        self, mock_storage_table
    ):
        segment = Flowsegmentpost(object_id="abc", timerange="_")
        flow_id = "123"

        return_item = {"Item": {"id": "abc", "flow_id": "123", "expire_at": 12345}}
        mock_storage_table.get_item.return_value = return_item

        result = dynamodb.validate_object_id(segment, flow_id)

        # validate_object_id performs no writes; it queues a claim instead
        assert 0 == mock_storage_table.update_item.call_count
        assert result["valid"]
        assert 1 == len(result["claim"])
        assert "update" == result["claim"][0]["op"]
        assert {"id": "abc"} == result["claim"][0]["key"]

    @patch("dynamodb.storage_table")
    def test_validate_object_id_matched_flow_id_expire_not_present(
        self, mock_storage_table
    ):
        segment = Flowsegmentpost(object_id="abc", timerange="_")
        flow_id = "123"

        return_item = {"Item": {"id": "abc", "flow_id": "123"}}
        mock_storage_table.get_item.return_value = return_item

        result = dynamodb.validate_object_id(segment, flow_id)

        assert 0 == mock_storage_table.update_item.call_count
        assert result["valid"]

    @patch("dynamodb.storage_table")
    def test_validate_object_id_not_matched_flow_id_expire_present(
        self, mock_storage_table
    ):
        segment = Flowsegmentpost(object_id="abc", timerange="_")
        flow_id = "456"

        return_item = {"Item": {"id": "abc", "flow_id": "123", "expire_at": 12345}}
        mock_storage_table.get_item.return_value = return_item

        result = dynamodb.validate_object_id(segment, flow_id)

        assert 0 == mock_storage_table.update_item.call_count
        assert not result["valid"]

    @patch("dynamodb.storage_table")
    def test_validate_object_id_not_matched_flow_id_expire_not_present(
        self, mock_storage_table
    ):
        segment = Flowsegmentpost(object_id="abc", timerange="_")
        flow_id = "456"

        return_item = {"Item": {"id": "abc", "flow_id": "123"}}
        mock_storage_table.get_item.return_value = return_item

        result = dynamodb.validate_object_id(segment, flow_id)

        assert 0 == mock_storage_table.update_item.call_count
        assert result["valid"]

    @patch("dynamodb.storage_table")
    def test_validate_object_id_new_object_with_init_object_id(
        self, mock_storage_table
    ):
        """First use of new object with init_object_id stores it in put_item"""
        segment = Flowsegmentpost(
            object_id="abc",
            timerange="_",
            init_object_id="init-123",
            get_urls=[{"label": "test", "url": "http://example.com"}],
        )
        flow_id = "123"

        mock_storage_table.get_item.side_effect = [
            {},  # object_id doesn't exist
            {"Item": {"id": "init-123", "flow_id": "123", "expire_at": 99999}},
        ]

        result = dynamodb.validate_object_id(segment, flow_id)

        assert result["valid"]
        assert result["init_storage_id"] is None
        # The new-object storage record is queued as a put claim, not written
        assert 0 == mock_storage_table.put_item.call_count
        assert 1 == len(result["claim"])
        assert "put" == result["claim"][0]["op"]
        assert result["claim"][0]["item"]["init_object_id"] == "init-123"

    @patch("dynamodb.storage_table")
    def test_validate_object_id_first_use_with_init_object_id(self, mock_storage_table):
        """First use with expire_at stores init_object_id in update_item"""
        segment = Flowsegmentpost(
            object_id="abc",
            timerange="_",
            init_object_id="init-123",
        )
        flow_id = "123"

        mock_storage_table.get_item.side_effect = [
            {
                "Item": {
                    "id": "abc",
                    "flow_id": "123",
                    "expire_at": 12345,
                    "storage_id": "sid-1",
                }
            },
            {
                "Item": {
                    "id": "init-123",
                    "flow_id": "123",
                    "is_init_object": True,
                    "storage_id": "init-sid",
                }
            },
        ]

        result = dynamodb.validate_object_id(segment, flow_id)

        assert result["valid"]
        assert result["init_storage_id"] == "init-sid"
        # The media object claim carries the init_object_id in its update
        assert 0 == mock_storage_table.update_item.call_count
        media_claim = next(c for c in result["claim"] if c["key"] == {"id": "abc"})
        assert ":init_object_id" in media_claim["ExpressionAttributeValues"]

    @patch("dynamodb.storage_table")
    def test_validate_object_id_is_init_object_rejected_as_media(
        self, mock_storage_table
    ):
        """An object marked as init cannot be used as a media object"""
        segment = Flowsegmentpost(object_id="abc", timerange="_")
        flow_id = "123"

        mock_storage_table.get_item.return_value = {
            "Item": {"id": "abc", "flow_id": "123", "is_init_object": True}
        }

        result = dynamodb.validate_object_id(segment, flow_id)

        assert not result["valid"]
        assert (
            "initialisation segment Object cannot be used as a media segment"
            in result["message"]
        )

    @patch("dynamodb.storage_table")
    def test_validate_object_id_init_object_id_not_found(self, mock_storage_table):
        """init_object_id that doesn't exist returns error"""
        segment = Flowsegmentpost(
            object_id="abc",
            timerange="_",
            init_object_id="nonexistent",
        )
        flow_id = "123"

        mock_storage_table.get_item.side_effect = [
            {
                "Item": {
                    "id": "abc",
                    "flow_id": "123",
                    "expire_at": 12345,
                    "storage_id": "sid-1",
                }
            },
            {},  # init object not found
        ]

        result = dynamodb.validate_object_id(segment, flow_id)

        assert not result["valid"]
        assert "init_object_id does not exist" in result["message"]

    @patch("dynamodb.storage_table")
    def test_validate_object_id_init_object_already_media_object(
        self, mock_storage_table
    ):
        """init_object_id pointing to an existing media object returns error"""
        segment = Flowsegmentpost(
            object_id="abc",
            timerange="_",
            init_object_id="media-obj",
        )
        flow_id = "123"

        mock_storage_table.get_item.side_effect = [
            {
                "Item": {
                    "id": "abc",
                    "flow_id": "123",
                    "expire_at": 12345,
                    "storage_id": "sid-1",
                }
            },
            {"Item": {"id": "media-obj", "flow_id": "123"}},
        ]

        result = dynamodb.validate_object_id(segment, flow_id)

        assert not result["valid"]
        assert (
            "media segment Object cannot be used as an initialisation segment"
            in result["message"]
        )

    @patch("dynamodb.storage_table")
    def test_validate_object_id_init_object_first_use_wrong_flow(
        self, mock_storage_table
    ):
        """init_object_id first use with mismatched flow returns error"""
        segment = Flowsegmentpost(
            object_id="abc",
            timerange="_",
            init_object_id="init-123",
        )
        flow_id = "flow-A"

        mock_storage_table.get_item.side_effect = [
            {
                "Item": {
                    "id": "abc",
                    "flow_id": "flow-A",
                    "expire_at": 12345,
                    "storage_id": "sid-1",
                }
            },
            {"Item": {"id": "init-123", "flow_id": "flow-B", "expire_at": 99999}},
        ]

        result = dynamodb.validate_object_id(segment, flow_id)

        assert not result["valid"]
        assert (
            "init_object_id is not valid to be used for the flow id"
            in result["message"]
        )

    @patch("dynamodb.storage_table")
    def test_validate_object_id_init_object_first_use_marks_as_init(
        self, mock_storage_table
    ):
        """init_object_id first use with matching flow marks object as init"""
        segment = Flowsegmentpost(
            object_id="abc",
            timerange="_",
            init_object_id="init-123",
        )
        flow_id = "123"

        mock_storage_table.get_item.side_effect = [
            {
                "Item": {
                    "id": "abc",
                    "flow_id": "123",
                    "expire_at": 12345,
                    "storage_id": "sid-1",
                }
            },
            {
                "Item": {
                    "id": "init-123",
                    "flow_id": "123",
                    "expire_at": 99999,
                    "storage_id": "init-sid",
                }
            },
        ]

        result = dynamodb.validate_object_id(segment, flow_id)

        assert result["valid"]
        assert result["init_storage_id"] == "init-sid"
        # Two claims queued (media object first-use + flag the init object),
        # neither written by validate_object_id itself
        assert 0 == mock_storage_table.update_item.call_count
        assert 2 == len(result["claim"])
        init_claim = next(c for c in result["claim"] if c["key"] == {"id": "init-123"})
        assert ":flag" in init_claim["ExpressionAttributeValues"]

    @patch("dynamodb.storage_table")
    def test_validate_object_id_reuse_with_changed_init_object_id(
        self, mock_storage_table
    ):
        """Object re-use with different init_object_id returns error"""
        segment = Flowsegmentpost(
            object_id="abc",
            timerange="_",
            init_object_id="new-init",
        )
        flow_id = "123"

        mock_storage_table.get_item.return_value = {
            "Item": {"id": "abc", "flow_id": "123", "init_object_id": "old-init"}
        }

        result = dynamodb.validate_object_id(segment, flow_id)

        assert not result["valid"]
        assert "init_object_id must not change" in result["message"]

    @patch("dynamodb.storage_table")
    def test_validate_object_id_reuse_with_same_init_object_id(
        self, mock_storage_table
    ):
        """Object re-use with same init_object_id succeeds"""
        segment = Flowsegmentpost(
            object_id="abc",
            timerange="_",
            init_object_id="init-123",
        )
        flow_id = "123"

        mock_storage_table.get_item.side_effect = [
            {
                "Item": {
                    "id": "abc",
                    "flow_id": "123",
                    "init_object_id": "init-123",
                    "storage_id": "sid-1",
                }
            },
            {
                "Item": {
                    "id": "init-123",
                    "flow_id": "123",
                    "is_init_object": True,
                    "storage_id": "init-sid",
                }
            },
        ]

        result = dynamodb.validate_object_id(segment, flow_id)

        assert result["valid"]
        assert result["init_storage_id"] == "init-sid"

    @patch("dynamodb.segments_table")
    @patch("dynamodb.storage_table")
    def test_delete_flow_storage_record_delete(
        self, mock_storage_table, mock_segments_table
    ):
        mock_segments_table.query.side_effect = [
            {"Count": 0},  # object-id-index
            {"Count": 0},  # init-object-id-index
        ]

        dynamodb.delete_flow_storage_record("abc", "123")

        assert 2 == mock_segments_table.query.call_count
        assert 1 == mock_storage_table.delete_item.call_count
        assert 0 == mock_storage_table.update_item.call_count

    @patch("dynamodb.segments_table")
    @patch("dynamodb.storage_table")
    def test_delete_flow_storage_record_update_storage_id(
        self, mock_storage_table, mock_segments_table
    ):
        mock_segments_table.query.side_effect = [
            {"Count": 1},  # object-id-index — still referenced as media
            {"Count": 0},  # init-object-id-index
        ]

        dynamodb.delete_flow_storage_record("abc", "123")

        assert 0 == mock_storage_table.delete_item.call_count
        assert 1 == mock_storage_table.update_item.call_count

    @patch("dynamodb.segments_table")
    @patch("dynamodb.storage_table")
    def test_delete_flow_storage_record_still_referenced_as_init(
        self, mock_storage_table, mock_segments_table
    ):
        """An object no longer used as a media object but still referenced as an init object must NOT be deleted."""
        mock_segments_table.query.side_effect = [
            {"Count": 0},  # object-id-index — no media references
            {"Count": 2},  # init-object-id-index — still used as init by 2 segments
        ]

        dynamodb.delete_flow_storage_record("abc", "123")

        assert 0 == mock_storage_table.delete_item.call_count
        assert 1 == mock_storage_table.update_item.call_count

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
