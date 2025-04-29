import os
import math
import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch
from botocore.exceptions import ClientError
from mediatimestamp.immutable import TimeRange, Timestamp

with patch('boto3.client'):
    with patch('boto3.resource'):
        import utils


class TestUtils():
    @pytest.mark.parametrize("items", [
        # Empty
        ([]),
        # Definitely 1
        ([{"id": 1}]),
        # Varying multiples definitely exceeding 1 batch
        ([{"id": i, "data": "x" * int(utils.constants.MAX_MESSAGE_SIZE * 0.9)}
         for i in range(2)]),
        ([{"id": i, "data": "x" * int(utils.constants.MAX_MESSAGE_SIZE * 0.9)}
         for i in range(5)]),
        ([{"id": i, "data": "x" * int(utils.constants.MAX_MESSAGE_SIZE * 0.9)}
         for i in range(10)]),
    ])
    def test_get_message_batch_sizes(self, items):
        if len(items) == 0:
            expected = 0
        else:
            expected = math.ceil(
                len(json.dumps(items, default=str)) / utils.constants.MAX_MESSAGE_SIZE)

        result = utils.get_message_batches(items)
        assert len(result) == expected

    @pytest.mark.parametrize("items", [
        ([]),
        ([{"id": 1}]),
        ([{"id": i, "data": "test"} for i in range(2)]),
        ([{"id": i, "data": "test"} for i in range(5)]),
        ([{"id": i, "data": "test"} for i in range(10)]),
    ])
    @patch('utils.get_message_batches')
    def test_put_message_calls_once_per_batch(self, mock_get_batches, items):
        # Mock batching into 1 item per batch
        mock_get_batches.return_value = [[item] for item in items]

        queue_url = "test-queue"

        with patch('utils.sqs') as mock_sqs:
            utils.put_message_batches(queue_url, items)

            assert mock_sqs.send_message.call_count == len(items)

    @patch('utils.idp')
    def test_get_user_pool_calls_correctly(self, mock_idp):
        expected = 'userpool123'
        mock_idp.describe_user_pool.return_value = {"UserPool": expected}

        result = utils.get_user_pool()
        kw_args = mock_idp.describe_user_pool.call_args[1]

        assert mock_idp.describe_user_pool.call_count == 1
        assert kw_args["UserPoolId"] == os.environ["USER_POOL_ID"]
        assert result == expected

    @patch('utils.lmda')
    def test_get_username_calls_lambda(self, mock_lmda):
        utils.get_username.cache_clear()
        claims_tuple = ('user123', 'client123')
        expected = {"username": "XXXXXXX"}

        response_payload_mock = MagicMock()
        response_payload_mock.read.return_value = json.dumps(
            expected).encode('utf-8')
        mock_lmda.invoke.return_value = {
            'StatusCode': 200,
            'Payload': response_payload_mock
        }

        result = utils.get_username(claims_tuple)
        kw_args = mock_lmda.invoke.call_args[1]

        assert mock_lmda.invoke.call_count == 1
        assert result == expected
        assert kw_args["FunctionName"] == os.environ["COGNITO_LAMBDA_NAME"]

    @patch('utils.lmda')
    def test_get_username_throws_on_status(self, mock_lmda):
        utils.get_username.cache_clear()
        claims_tuple = ('user123', 'client123')

        mock_lmda.invoke.return_value = {
            'StatusCode': 500,
            'FunctionError': {}
        }
        with pytest.raises(ClientError):
            result = utils.get_username(claims_tuple)

    def test_pop_outliers_empty_list(self, time_range_one_day):
        items = []
        result = utils.pop_outliers(time_range_one_day, items)
        assert result == []

    def test_pop_outliers_single_item_within_range(self, time_range_one_day):
        now = datetime.now()
        items = [
            {
                "timerange": TimeRange(
                    start=Timestamp.from_datetime(now + timedelta(hours=12)),
                    end=Timestamp.from_datetime(now + timedelta(hours=13))
                ).to_sec_nsec_range()
            }
        ]

        result = utils.pop_outliers(time_range_one_day, items)
        assert result == items

    def test_pop_outliers_single_item_outside_range(self, time_range_one_day):
        now = datetime.now()
        items = [
            {
                "timerange": TimeRange(
                    start=Timestamp.from_datetime(now - timedelta(hours=2)),
                    end=Timestamp.from_datetime(now - timedelta(hours=1))
                ).to_sec_nsec_range()
            }
        ]

        result = utils.pop_outliers(time_range_one_day, items)
        assert result == []

    def test_pop_outliers_removes_partial_end_item(self, time_range_one_day):
        now = datetime.now()
        items = [
            {
                "timerange": TimeRange(
                    start=Timestamp.from_datetime(now + timedelta(hours=12)),
                    end=Timestamp.from_datetime(now + timedelta(hours=13))
                ).to_sec_nsec_range()
            },
            {
                "timerange": TimeRange(
                    start=Timestamp.from_datetime(now + timedelta(hours=22)),
                    end=Timestamp.from_datetime(now + timedelta(hours=25))
                ).to_sec_nsec_range()
            }
        ]

        result = utils.pop_outliers(time_range_one_day, items)
        assert len(result) == 1
        assert result == items[:-1]

    def test_pop_outliers_removes_partial_first_item(self, time_range_one_day):
        now = datetime.now()
        items = [
            {
                "timerange": TimeRange(
                    start=Timestamp.from_datetime(now - timedelta(hours=2)),
                    end=Timestamp.from_datetime(now + timedelta(hours=1))
                ).to_sec_nsec_range()
            },
            {
                "timerange": TimeRange(
                    start=Timestamp.from_datetime(now + timedelta(hours=12)),
                    end=Timestamp.from_datetime(now + timedelta(hours=13))
                ).to_sec_nsec_range()
            }
        ]

        result = utils.pop_outliers(time_range_one_day, items)
        assert len(result) == 1
        assert result == items[1:]
