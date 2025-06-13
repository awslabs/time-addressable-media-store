import json
import math
import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from aws_lambda_powertools.event_handler.exceptions import BadRequestError
from botocore.exceptions import ClientError
from mediatimestamp.immutable import TimeRange, Timestamp

pytestmark = [
    pytest.mark.unit,
]

with patch("boto3.client"):
    with patch("boto3.resource"):
        import constants
        import utils


class TestUtils:
    @pytest.mark.parametrize(
        "items",
        [
            # Empty
            ([]),
            # Definitely 1
            ([{"id": 1}]),
            # Varying multiples definitely exceeding 1 batch
            (
                [
                    {"id": i, "data": "x" * int(utils.constants.MAX_MESSAGE_SIZE * 0.9)}
                    for i in range(2)
                ]
            ),
            (
                [
                    {"id": i, "data": "x" * int(utils.constants.MAX_MESSAGE_SIZE * 0.9)}
                    for i in range(5)
                ]
            ),
            (
                [
                    {"id": i, "data": "x" * int(utils.constants.MAX_MESSAGE_SIZE * 0.9)}
                    for i in range(10)
                ]
            ),
        ],
    )
    def test_get_message_batch_sizes(self, items):
        if len(items) == 0:
            expected = 0
        else:
            expected = math.ceil(
                len(json.dumps(items, default=str)) / utils.constants.MAX_MESSAGE_SIZE
            )

        result = utils.get_message_batches(items)
        assert len(result) == expected

    @pytest.mark.parametrize(
        "items",
        [
            ([]),
            ([{"id": 1}]),
            ([{"id": i, "data": "test"} for i in range(2)]),
            ([{"id": i, "data": "test"} for i in range(5)]),
            ([{"id": i, "data": "test"} for i in range(10)]),
        ],
    )
    @patch("utils.get_message_batches")
    def test_put_message_calls_once_per_batch(self, mock_get_batches, items):
        # Mock batching into 1 item per batch
        mock_get_batches.return_value = [[item] for item in items]

        queue_url = "test-queue"

        with patch("utils.sqs") as mock_sqs:
            utils.put_message_batches(queue_url, items)

            assert mock_sqs.send_message.call_count == len(items)

    @patch("utils.idp")
    def test_get_user_pool_calls_correctly(self, mock_idp):
        expected = "userpool123"
        mock_idp.describe_user_pool.return_value = {"UserPool": expected}

        result = utils.get_user_pool()
        kw_args = mock_idp.describe_user_pool.call_args[1]

        assert mock_idp.describe_user_pool.call_count == 1
        assert kw_args["UserPoolId"] == os.environ["USER_POOL_ID"]
        assert result == expected

    @patch("utils.lmda")
    def test_get_username_calls_lambda(self, mock_lmda):
        utils.get_username.cache_clear()
        claims_tuple = ("user123", "client123")
        expected = {"username": "XXXXXXX"}

        response_payload_mock = MagicMock()
        response_payload_mock.read.return_value = json.dumps(expected).encode("utf-8")
        mock_lmda.invoke.return_value = {
            "StatusCode": 200,
            "Payload": response_payload_mock,
        }

        result = utils.get_username(claims_tuple)
        kw_args = mock_lmda.invoke.call_args[1]

        assert mock_lmda.invoke.call_count == 1
        assert result == expected
        assert kw_args["FunctionName"] == os.environ["COGNITO_LAMBDA_NAME"]

    @patch("utils.lmda")
    def test_get_username_throws_on_status(self, mock_lmda):
        utils.get_username.cache_clear()
        claims_tuple = ("user123", "client123")

        mock_lmda.invoke.return_value = {"StatusCode": 500, "FunctionError": {}}
        with pytest.raises(ClientError):
            utils.get_username(claims_tuple)

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
                    end=Timestamp.from_datetime(now + timedelta(hours=13)),
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
                    end=Timestamp.from_datetime(now - timedelta(hours=1)),
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
                    end=Timestamp.from_datetime(now + timedelta(hours=13)),
                ).to_sec_nsec_range()
            },
            {
                "timerange": TimeRange(
                    start=Timestamp.from_datetime(now + timedelta(hours=22)),
                    end=Timestamp.from_datetime(now + timedelta(hours=25)),
                ).to_sec_nsec_range()
            },
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
                    end=Timestamp.from_datetime(now + timedelta(hours=1)),
                ).to_sec_nsec_range()
            },
            {
                "timerange": TimeRange(
                    start=Timestamp.from_datetime(now + timedelta(hours=12)),
                    end=Timestamp.from_datetime(now + timedelta(hours=13)),
                ).to_sec_nsec_range()
            },
        ]

        result = utils.pop_outliers(time_range_one_day, items)
        assert len(result) == 1
        assert result == items[1:]

    @pytest.mark.parametrize(
        "exists",
        [
            True,
            False,
        ],
    )
    def test_parse_tag_parameters(self, exists):
        expected_values = {"abc": "123"}
        expected_exists = {"xyz": exists}

        params = {f"tag.{k}": v for k, v in expected_values.items()}

        for k, v in expected_exists.items():
            params[f"tag_exists.{k}"] = str(v).lower()

        values, exists = utils.parse_tag_parameters(params)
        assert values == expected_values
        assert exists == expected_exists

    def test_parse_tag_parameters_throws_on_bad_bool(self):
        params = {"tag_exists.xyz": "notabool"}

        with pytest.raises(BadRequestError):
            utils.parse_tag_parameters(params)

    def test_json_number_returns_float(self):
        input_float = "1.23"
        result = utils.json_number(input_float)

        assert isinstance(result, float)
        assert result == float(input_float)

    def test_json_number_returns_int(self):
        input_int = "1"
        result = utils.json_number(input_int)

        assert isinstance(result, int)
        assert result == int(input_int)

    def test_serialise_neptune_obj(self):
        child_key = "child"
        serialised_key = f"{constants.SERIALISE_PREFIX}{child_key}"
        input_dict = {"id": "123", child_key: {"hello": "world"}}

        result = utils.serialise_neptune_obj(input_dict)

        assert isinstance(result, dict)
        assert result["id"] == input_dict["id"]
        assert isinstance(result[serialised_key], str)
        assert json.loads(result[serialised_key]) == input_dict[child_key]

    def test_deserialise_neptune_obj(self):
        child_dict = {"hello": "world"}

        child_key = "child"
        serialised_key = f"{constants.SERIALISE_PREFIX}{child_key}"
        input_dict = {"id": "123", serialised_key: json.dumps(child_dict)}

        result = utils.deserialise_neptune_obj(input_dict)

        assert isinstance(result, dict)
        assert result["id"] == input_dict["id"]
        assert result[child_key] == child_dict

    def test_parse_api_gw_parameters(self):
        essence_param_key = "sample_rate"
        essence_param_value = "0"
        query_parameters = {
            essence_param_key: essence_param_value,
            "tag_values": {"a": "1", "b": "2"},
            "tag_exists": {"y": True, "z": False},
            "misc": "123",
        }

        return_dict, where_literals = utils.parse_api_gw_parameters(query_parameters)

        # Ensure essence prop is structured and processed
        assert return_dict["essence_properties"][essence_param_key] == int(
            essence_param_value
        )

        # Ensure tag values are structurd appropriately
        for k, v in query_parameters["tag_values"].items():
            assert return_dict["tag_properties"][k] == v

        # Ensure tag_exists values are reflected in where literals
        assert where_literals == ["t.y IS NOT NULL", "t.z IS NULL"]

        # Ensure misc properties are included appropriately
        assert return_dict["properties"]["misc"] == query_parameters["misc"]

    def test_filter_dict_removes_keys(self):
        input_dict = {"a": 1, "b": 2, "c": 3}

        keys_to_remove = ["a", "c"]
        result = utils.filter_dict(input_dict, keys_to_remove)

        assert result == {"b": 2}

    @patch("utils.s3")
    def test_check_object_exists(self, mock_s3):
        mock_s3.head_object.return_value = {}
        result = utils.check_object_exists("bucket", "key")

        assert mock_s3.head_object.call_count == 1
        assert result

    @patch("utils.s3")
    def test_check_object_does_not_exist(self, mock_s3):
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "head_object"
        )
        result = utils.check_object_exists("bucket", "key1")

        assert mock_s3.head_object.call_count == 1
        assert not result

    @patch("utils.s3")
    def test_get_presigned_url(self, mock_s3):
        expected = "https://example.com"
        mock_s3.generate_presigned_url.return_value = expected
        result = utils.generate_presigned_url("method", "bucket", "key")

        kw_args = mock_s3.generate_presigned_url.call_args[1]

        assert mock_s3.generate_presigned_url.call_count == 1
        assert result == expected
        assert kw_args["ExpiresIn"] == utils.constants.PRESIGNED_URL_EXPIRES_IN
