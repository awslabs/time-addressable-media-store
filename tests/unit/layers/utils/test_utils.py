import json
import math
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from aws_lambda_powertools.event_handler.exceptions import (
    BadRequestError,
    ForbiddenError,
    NotFoundError,
)
from aws_lambda_powertools.event_handler.openapi.exceptions import (
    RequestValidationError,
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

    @pytest.mark.parametrize("value", [",", "a,", ",a", "a,,b"])
    @pytest.mark.parametrize("prefix", ["tag", "flow_tag"])
    def test_parse_tag_parameters_throws_on_empty_list_item(self, prefix, value):
        with pytest.raises(BadRequestError):
            utils.parse_tag_parameters({f"{prefix}.xyz": value})

    def test_parse_tag_parameters_throws_on_empty_list_item_custom_prefix(self):
        with pytest.raises(BadRequestError):
            utils.parse_tag_parameters(
                {"storage_backend_tag.xyz": ","},
                value_prefixes=("storage_backend_tag",),
                exists_prefixes=("storage_backend_tag_exists",),
            )

    @pytest.mark.parametrize("value", ["a", "a,b", "a b,c"])
    def test_parse_tag_parameters_accepts_valid_list(self, value):
        values, _ = utils.parse_tag_parameters({"tag.xyz": value})

        assert values == {"xyz": value}

    def test_parse_tag_parameters_custom_prefixes_isolate_namespaces(self):
        """storage_backend_tag.{name} must parse only under its own prefixes and
        never collide with the tag.{name} filters sharing the same request (the
        objects endpoint uses both at once)."""
        params = {
            "tag.env": "prod",
            "tag_exists.region": "true",
            "storage_backend_tag.tier": "gold",
            "storage_backend_tag_exists.zone": "false",
        }
        # Default prefixes see only the plain tag.* params.
        values, exists = utils.parse_tag_parameters(params)
        assert values == {"env": "prod"}
        assert exists == {"region": True}
        # Storage-backend prefixes see only their own params.
        sb_values, sb_exists = utils.parse_tag_parameters(
            params,
            value_prefixes=("storage_backend_tag",),
            exists_prefixes=("storage_backend_tag_exists",),
        )
        assert sb_values == {"tier": "gold"}
        assert sb_exists == {"zone": False}

    @pytest.mark.parametrize(
        "item_tags,tag_values,tag_exists,expected",
        [
            # No filters -> everything matches
            ({"env": "prod"}, {}, {}, True),
            # String value, comma-separated OR
            ({"env": "test"}, {"env": "prod,test"}, {}, True),
            ({"env": "dev"}, {"env": "prod,test"}, {}, False),
            # Array-valued tag: match on any member
            ({"tier": ["silver", "bronze"]}, {"tier": "gold,silver"}, {}, True),
            ({"tier": ["bronze"]}, {"tier": "gold,silver"}, {}, False),
            # Whole-value match, never a substring
            ({"env": "production"}, {"env": "prod"}, {}, False),
            # Missing tag never satisfies a value filter
            ({}, {"env": "prod"}, {}, False),
            # Existence filters
            ({"env": "prod"}, {}, {"env": True}, True),
            ({}, {}, {"env": True}, False),
            ({}, {}, {"env": False}, True),
            ({"env": "prod"}, {}, {"env": False}, False),
            # AND across tag names: every supplied filter must hold
            ({"env": "prod", "tier": "gold"}, {"env": "prod"}, {"tier": True}, True),
            ({"env": "prod"}, {"env": "prod"}, {"tier": True}, False),
            # None item tags behaves as empty
            (None, {}, {"env": False}, True),
            (None, {"env": "prod"}, {}, False),
        ],
    )
    def test_tags_match(self, item_tags, tag_values, tag_exists, expected):
        assert utils.tags_match(item_tags, tag_values, tag_exists) is expected

    def test_tags_match_strips_whitespace_around_values(self):
        assert utils.tags_match({"env": "test"}, {"env": "prod, test"}, {}) is True

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

    def test_parse_api_gw_parameters_bool_true_is_property_match(self):
        return_dict, where_literals = utils.parse_api_gw_parameters(
            {"init_segments": True}
        )

        assert return_dict["essence_properties"] == {"init_segments": True}
        assert where_literals == []

    @pytest.mark.parametrize("value", [False, "false"])
    def test_parse_api_gw_parameters_bool_false_matches_omitted(self, value):
        return_dict, where_literals = utils.parse_api_gw_parameters(
            {"init_segments": value}
        )

        assert "init_segments" not in return_dict["essence_properties"]
        assert where_literals == [
            "(e.init_segments IS NULL OR e.init_segments = false)"
        ]

    def test_parse_api_gw_parameters_tag_value_matches_array_member(self):
        """A tag filter must match a member of an array-valued tag.

        Tag values may be arrays from spec 8.2, but there is no array-specific
        branch in parse_api_gw_parameters. It works because serialise_tags_dict
        stores every value as JSON and the filter searches for the value wrapped
        in quotes, which is a substring of the serialised array. That is an
        implicit contract between two functions, so it is asserted directly
        against json.dumps output rather than against a hand-written literal.
        """
        _, where_literals = utils.parse_api_gw_parameters(
            {"tag_values": {"colour": "green"}}
        )

        assert where_literals == ['(t.`colour` CONTAINS "\\"green\\"")']

        # The literal's operand is what CONTAINS is applied to in Neptune, so
        # substring behaviour against the stored form can be checked here.
        operand = json.loads(where_literals[0].split(" CONTAINS ", 1)[1][:-1])
        assert operand in json.dumps(["red", "green"])  # member of an array
        assert operand in json.dumps("green")  # unchanged for a scalar
        # Quoting is what prevents a partial match; without it "green" would be
        # found inside "greenish" and inside a same-prefixed sibling tag value.
        assert operand not in json.dumps("greenish")
        assert operand not in json.dumps(["greenish", "evergreen"])

    def test_filter_dict_removes_keys(self):
        input_dict = {"a": 1, "b": 2, "c": 3}

        keys_to_remove = ["a", "c"]
        result = utils.filter_dict(input_dict, keys_to_remove)

        assert result == {"b": 2}

    @pytest.mark.parametrize("http_method", ["GET", "HEAD", "DELETE"])
    def test_raise_validation_error_path_is_404(self, http_method):
        ex = RequestValidationError([{"loc": ("path", "flowId"), "msg": "bad"}])

        with pytest.raises(NotFoundError):
            utils.raise_validation_error(ex, http_method)

    @pytest.mark.parametrize(
        "http_method, loc",
        [
            ("PUT", ("path", "flowId")),
            ("POST", ("path", "flowId")),
            ("GET", ("query", "limit")),
            ("GET", ()),
        ],
    )
    def test_raise_validation_error_is_400(self, http_method, loc):
        ex = RequestValidationError([{"loc": loc, "msg": "bad"}])

        with pytest.raises(BadRequestError):
            utils.raise_validation_error(ex, http_method)

    def test_raise_validation_error_mixed_locations_is_400(self):
        ex = RequestValidationError(
            [
                {"loc": ("path", "flowId"), "msg": "bad"},
                {"loc": ("query", "limit"), "msg": "bad"},
            ]
        )

        with pytest.raises(BadRequestError):
            utils.raise_validation_error(ex, "GET")

    def test_remove_null_preserves_empty_essence_parameters(self):
        obj = {
            "format": "urn:x-nmos:format:data",
            "essence_parameters": {"unc_parameters": {}},
            "tags": {},
            "label": None,
        }

        utils.remove_null(obj)

        assert obj == {"format": "urn:x-nmos:format:data", "essence_parameters": {}}

    def test_remove_null_removes_empty_essence_parameters_for_multi(self):
        obj = {"format": "urn:x-nmos:format:multi", "essence_parameters": {}}

        utils.remove_null(obj)

        assert obj == {"format": "urn:x-nmos:format:multi"}

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

    @pytest.mark.parametrize(
        "codec, essence_parameters",
        [
            # Not a raw codec: nothing is required
            ("audio/aac", {"sample_rate": 48000}),
            (None, {}),
            (
                "audio/x-raw-int",
                {"bit_depth": 16, "unc_parameters": {"unc_type": "interleaved"}},
            ),
            (
                "audio/x-raw-float",
                {"bit_depth": 32, "unc_parameters": {"unc_type": "planar"}},
            ),
            (
                "video/raw",
                {
                    "bit_depth": 10,
                    "unc_parameters": {"unc_type": "v210"},
                    "horiz_chroma_subs": 2,
                    "vert_chroma_subs": 1,
                },
            ),
            # RGB and planar packing do not require chroma subsampling
            ("video/raw", {"bit_depth": 8, "unc_parameters": {"unc_type": "RGB"}}),
            ("video/raw", {"bit_depth": 8, "unc_parameters": {"unc_type": "planar"}}),
        ],
    )
    def test_validate_raw_essence_parameters_accepts(self, codec, essence_parameters):
        utils.validate_raw_essence_parameters(codec, essence_parameters)

    @pytest.mark.parametrize(
        "codec, essence_parameters, missing",
        [
            (
                "audio/x-raw-int",
                {"unc_parameters": {"unc_type": "interleaved"}},
                "bit_depth",
            ),
            ("audio/x-raw-int", {"bit_depth": 16}, "unc_type"),
            (
                "audio/x-raw-float",
                {"unc_parameters": {"unc_type": "interleaved"}},
                "bit_depth",
            ),
            (
                "audio/x-raw-float",
                {"bit_depth": 24, "unc_parameters": {"unc_type": "interleaved"}},
                "32 or 64",
            ),
            ("video/raw", {"unc_parameters": {"unc_type": "RGB"}}, "bit_depth"),
            ("video/raw", {"bit_depth": 8}, "unc_type"),
            (
                "video/raw",
                {
                    "bit_depth": 8,
                    "unc_parameters": {"unc_type": "UYVY"},
                    "vert_chroma_subs": 1,
                },
                "horiz_chroma_subs",
            ),
            (
                "video/raw",
                {
                    "bit_depth": 8,
                    "unc_parameters": {"unc_type": "UYVY"},
                    "horiz_chroma_subs": 2,
                },
                "vert_chroma_subs",
            ),
        ],
    )
    def test_validate_raw_essence_parameters_rejects(
        self, codec, essence_parameters, missing
    ):
        with pytest.raises(BadRequestError) as ex:
            utils.validate_raw_essence_parameters(codec, essence_parameters)

        assert missing in str(ex.value)

    @pytest.mark.parametrize(
        "body, expected",
        [
            ({"a": 1, "b": {"c": [1, "x"]}}, None),
            ([], None),
            ("text", None),
            ({"status": None}, ("status",)),
            ({"a": {"b": None}}, ("a", "b")),
            ([{"id": "x", "role": None}], (0, "role")),
            ({"ids": ["a", None]}, ("ids", 1)),
        ],
    )
    def test_find_null_path(self, body, expected):
        assert utils.find_null_path(body) == expected

    @pytest.mark.parametrize("value", ["(1:0)", "[1:0)", "(1:0]", "()"])
    def test_parse_timerange_exclusive_single_timestamp_is_empty(self, value):
        assert utils.parse_timerange(value).is_empty()

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("[1:0]", "[1:0]"),
            ("1:0", "[1:0]"),
            ("(1:0_2:0)", "(1:0_2:0)"),
            ("_0:0)", "_0:0)"),
            ("_", "_"),
        ],
    )
    def test_parse_timerange_defers_to_mediatimestamp(self, value, expected):
        assert str(utils.parse_timerange(value)) == expected

    def test_generate_link_url_encodes_query_values(self):
        event = MagicMock()
        event.request_context.domain_name = "api.example.com"
        event.request_context.path = "/Prod/flows/abc/segments"
        event.query_string_parameters = {
            "timerange": "[0:0_5:0)",
            "flow_tag.reference/x": "alpha,beta",
            "label": "a b&c+d%e#f",
            "page": "old-page",
        }

        result = utils.generate_link_url(event, "eyJhIjogMX0=+/")

        assert result == (
            "<https://api.example.com/Prod/flows/abc/segments?"
            "timerange=%5B0%3A0_5%3A0%29"
            "&flow_tag.reference%2Fx=alpha,beta"
            "&label=a%20b%26c%2Bd%25e%23f"
            '&page=eyJhIjogMX0%3D%2B%2F>; rel="next"'
        )

    def test_generate_link_url_without_query_parameters(self):
        event = MagicMock()
        event.request_context.domain_name = "api.example.com"
        event.request_context.path = "/Prod/flows"
        event.query_string_parameters = None

        result = utils.generate_link_url(event, "30")

        assert result == '<https://api.example.com/Prod/flows?page=30>; rel="next"'
