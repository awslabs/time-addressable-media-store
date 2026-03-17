import json
import math
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from aws_lambda_powertools.event_handler.exceptions import (
    BadRequestError,
    ForbiddenError,
)
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
        serialised_key = f"`{constants.SERIALISE_PREFIX}{child_key}`"
        input_dict = {"id": "123", child_key: {"hello": "world"}}

        result = utils.serialise_neptune_obj(input_dict)

        assert isinstance(result, dict)
        assert result["`id`"] == input_dict["id"]
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

        # Ensure tag and tag_exists values are reflected in where literals
        assert where_literals == [
            '(t.`a` CONTAINS "\\"1\\"")',
            '(t.`b` CONTAINS "\\"2\\"")',
            "t.`y` IS NOT NULL",
            "t.`z` IS NULL",
        ]

        # Ensure misc properties are included appropriately
        assert return_dict["properties"]["misc"] == query_parameters["misc"]

    def test_filter_dict_removes_keys(self):
        input_dict = {"a": 1, "b": 2, "c": 3}

        keys_to_remove = ["a", "c"]
        result = utils.filter_dict(input_dict, keys_to_remove)

        assert result == {"b": 2}

    @patch("utils.s3")
    def test_get_presigned_url(self, mock_s3):
        expected = "https://example.com"
        mock_s3.generate_presigned_url.return_value = expected
        result = utils.generate_presigned_url("method", "bucket", "key")

        kw_args = mock_s3.generate_presigned_url.call_args[1]

        assert mock_s3.generate_presigned_url.call_count == 1
        assert result == expected
        assert kw_args["ExpiresIn"] == utils.constants.MIN_PRESIGNED_URL_TIMEOUT_SECS

    @pytest.mark.parametrize(
        "auth_classes_json,expected",
        [
            ('["class1", "class2"]', {"class1", "class2"}),
            ("[]", set()),
            ('["single"]', {"single"}),
        ],
    )
    def test_get_auth_classes(self, auth_classes_json, expected):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {"auth_classes": auth_classes_json}
        # Act
        result = utils.get_auth_classes(mock_context)
        # Assert
        assert result == expected

    def test_get_auth_classes_missing_key(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {}
        # Act
        result = utils.get_auth_classes(mock_context)
        # Assert
        assert result == set()

    @pytest.mark.parametrize(
        "scopes_json,expected",
        [
            (
                '["tams-api/admin", "tams-api/read"]',
                {"tams-api/admin", "tams-api/read"},
            ),
            ("[]", set()),
            ('["tams-api/admin"]', {"tams-api/admin"}),
        ],
    )
    def test_get_scopes(self, scopes_json, expected):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {"scopes": scopes_json}
        # Act
        result = utils.get_scopes(mock_context)
        # Assert
        assert result == expected

    def test_get_scopes_missing_key(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {}
        # Act
        result = utils.get_scopes(mock_context)
        # Assert
        assert result == set()

    def test_is_admin_with_admin_scope(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {"scopes": '["tams-api/admin"]'}
        # Act
        result = utils.is_admin(mock_context)
        # Assert
        assert result is True

    def test_is_admin_without_admin_scope(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {"scopes": '["tams-api/read"]'}
        # Act
        result = utils.is_admin(mock_context)
        # Assert
        assert result is False

    def test_is_admin_with_empty_scopes(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {"scopes": "[]"}
        # Act
        result = utils.is_admin(mock_context)
        # Assert
        assert result is False

    def test_check_entity_authorization_admin_user(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {"scopes": '["tams-api/admin"]'}
        entity = {"tags": {}}
        # Act
        result = utils.check_entity_authorization(mock_context, entity)
        # Assert
        assert result is True

    def test_check_entity_authorization_matching_classes_list(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1", "class2"]',
        }
        entity = {"tags": {"auth_classes": ["class2", "class3"]}}
        # Act
        result = utils.check_entity_authorization(mock_context, entity)
        # Assert
        assert result is True

    def test_check_entity_authorization_matching_classes_string(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1"]',
        }
        entity = {"tags": {"auth_classes": "class1"}}
        # Act
        result = utils.check_entity_authorization(mock_context, entity)
        # Assert
        assert result is True

    def test_check_entity_authorization_no_match(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1"]',
        }
        entity = {"tags": {"auth_classes": ["class2", "class3"]}}
        # Act
        result = utils.check_entity_authorization(mock_context, entity)
        # Assert
        assert result is False

    def test_check_entity_authorization_no_auth_classes_tag(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1"]',
        }
        entity = {"tags": {}}
        # Act
        result = utils.check_entity_authorization(mock_context, entity)
        # Assert
        assert result is False

    def test_check_entity_authorization_empty_user_classes(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {"scopes": "[]", "auth_classes": "[]"}
        entity = {"tags": {"auth_classes": ["class1"]}}
        # Act
        result = utils.check_entity_authorization(mock_context, entity)
        # Assert
        assert result is False

    def test_require_entity_authorization_authorized(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1"]',
        }
        entity = {"tags": {"auth_classes": ["class1"]}}
        # Act
        utils.require_entity_authorization(mock_context, entity)

    def test_require_entity_authorization_unauthorized(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1"]',
        }
        entity = {"tags": {"auth_classes": ["class2"]}}
        # Act
        with pytest.raises(ForbiddenError) as exc_info:
            utils.require_entity_authorization(mock_context, entity)
        # Assert
        assert "You do not have permission to access this resource" in str(
            exc_info.value
        )

    def test_apply_auth_classes_filter_no_user_filter(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1", "class2"]',
        }
        tag_values = {}
        # Act
        result_values = utils.apply_auth_classes_filter(mock_context, tag_values)
        # Assert
        assert set(result_values["auth_classes"].split(",")) == {"class1", "class2"}

    def test_apply_auth_classes_filter_with_user_filter_intersection(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1", "class2", "class3"]',
        }
        tag_values = {"auth_classes": "class2,class4"}
        # Act
        result_values = utils.apply_auth_classes_filter(mock_context, tag_values)
        # Assert
        assert result_values["auth_classes"] == "class2"

    def test_apply_auth_classes_filter_with_user_filter_no_intersection(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1"]',
        }
        tag_values = {"auth_classes": "class2,class3"}
        # Act
        result_values = utils.apply_auth_classes_filter(mock_context, tag_values)
        # Assert
        assert result_values["auth_classes"] is None

    def test_apply_auth_classes_filter_empty_user_classes(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {"scopes": "[]", "auth_classes": "[]"}
        tag_values = {}
        # Act
        result_values = utils.apply_auth_classes_filter(mock_context, tag_values)
        # Assert
        assert result_values["auth_classes"] is None

    def test_apply_auth_classes_filter_admin_bypass(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {"scopes": '["tams-api/admin"]'}
        tag_values = {"test_tag": "test"}
        # Act
        result_values = utils.apply_auth_classes_filter(mock_context, tag_values)
        # Assert
        assert result_values == tag_values

    def test_require_auth_classes_tag_update_permission_admin_bypass(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {"scopes": '["tams-api/admin"]'}
        entity = {"tags": {"auth_classes": ["class1"]}}
        # Act
        utils.require_auth_classes_tag_update_permission(
            mock_context, entity, ["class2"]
        )

    def test_require_auth_classes_tag_update_permission_valid_update_list(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1", "class2"]',
        }
        entity = {"tags": {"auth_classes": ["class1"]}}
        # Act
        utils.require_auth_classes_tag_update_permission(
            mock_context, entity, ["class2"]
        )

    def test_require_auth_classes_tag_update_permission_valid_update_string(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1", "class2"]',
        }
        # Act
        entity = {"tags": {"auth_classes": "class1"}}

        utils.require_auth_classes_tag_update_permission(mock_context, entity, "class2")

    def test_require_auth_classes_tag_update_permission_no_existing_access(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class3"]',
        }
        entity = {"tags": {"auth_classes": ["class1", "class2"]}}
        # Act
        with pytest.raises(ForbiddenError) as exc_info:
            utils.require_auth_classes_tag_update_permission(
                mock_context, entity, ["class3"]
            )
        # Assert
        assert "existing auth_classes" in str(exc_info.value)

    def test_require_auth_classes_tag_update_permission_no_new_access(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1"]',
        }
        entity = {"tags": {"auth_classes": ["class1"]}}
        # Act
        with pytest.raises(ForbiddenError) as exc_info:
            utils.require_auth_classes_tag_update_permission(
                mock_context, entity, ["class2", "class3"]
            )
        # Asset
        assert "new auth_classes" in str(exc_info.value)

    def test_require_auth_classes_tag_update_permission_no_existing_tag(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1"]',
        }
        entity = {"tags": {}}
        # Act
        utils.require_auth_classes_tag_update_permission(
            mock_context, entity, ["class1"]
        )

    def test_require_auth_classes_tag_update_permission_empty_new_classes(self):
        # Arrange
        mock_context = MagicMock()
        mock_context.authorizer.raw_event = {
            "scopes": "[]",
            "auth_classes": '["class1"]',
        }
        entity = {"tags": {"auth_classes": ["class1"]}}
        # Act
        utils.require_auth_classes_tag_update_permission(mock_context, entity, [])
