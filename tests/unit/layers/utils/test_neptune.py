import pytest

# pylint: disable=no-name-in-module
from neptune import build_order_by

pytestmark = [
    pytest.mark.unit,
]


class TestBuildOrderBy:
    """Tests for build_order_by.

    build_order_by feeds cymple's order_by, which applies the ASC/DESC keyword
    only to the LAST column. Every column except the id tie-breaker must
    therefore carry its direction inline, and id takes the returned `ascending`
    flag. These tests assert both the column list and the flag so a regression
    to the broken single-direction form is caught.

    The leading `(col IS NULL)` term implements the spec fallback rule: records
    with an unset sort value sort after those with one by default, and before
    when reverse_order is set. Its direction depends only on reverse_order.
    """

    def test_datetime_default_is_descending(self):
        # created is a datetime key -> newest first (DESC) by default
        cols, ascending = build_order_by("source", None, False, {"created", "updated"})
        assert cols == [
            "(source.created IS NULL) ASC",
            "source.created DESC",
            "source.id",
        ]
        assert ascending is False

    def test_string_default_is_ascending(self):
        # label is a string key -> alphabetical (ASC) by default
        cols, ascending = build_order_by(
            "source", "label", False, {"created", "updated"}
        )
        assert cols == [
            "(source.label IS NULL) ASC",
            "source.label ASC",
            "source.id",
        ]
        assert ascending is True

    def test_reverse_flips_datetime_default(self):
        cols, ascending = build_order_by(
            "source", "created", True, {"created", "updated"}
        )
        assert cols == [
            "(source.created IS NULL) DESC",
            "source.created ASC",
            "source.id",
        ]
        assert ascending is True

    def test_reverse_flips_string_default(self):
        cols, ascending = build_order_by(
            "source", "label", True, {"created", "updated"}
        )
        assert cols == [
            "(source.label IS NULL) DESC",
            "source.label DESC",
            "source.id",
        ]
        assert ascending is False

    def test_sort_by_none_uses_default_col(self):
        cols, ascending = build_order_by(
            "flow", None, False, {"created", "metadata_updated"}
        )
        assert cols == [
            "(flow.created IS NULL) ASC",
            "flow.created DESC",
            "flow.id",
        ]
        assert ascending is False

    def test_explicit_default_col_for_fixed_key(self):
        # Webhooks have a single fixed string key (url) supplied via default_col
        cols, ascending = build_order_by(
            "webhook", None, False, set(), default_col="url"
        )
        assert cols == [
            "(webhook.url IS NULL) ASC",
            "webhook.url ASC",
            "webhook.id",
        ]
        assert ascending is True

    def test_fixed_key_reversed(self):
        cols, ascending = build_order_by(
            "webhook", None, True, set(), default_col="url"
        )
        assert cols == [
            "(webhook.url IS NULL) DESC",
            "webhook.url DESC",
            "webhook.id",
        ]
        assert ascending is False

    def test_tie_breaker_direction_matches_ascending_flag(self):
        # The id tie-breaker is the LAST column and has no inline direction; it
        # must resolve (via the ascending flag) to the same direction as the
        # primary column so pagination order is a total order.
        cols, ascending = build_order_by(
            "delete_request", "expiry", False, {"created", "expiry"}
        )
        assert cols[-1] == "delete_request.id"
        assert cols[1].endswith("DESC")
        assert ascending is False

    def test_id_always_present_as_secondary_key(self):
        for sort_by in [None, "created", "updated", "label"]:
            cols, _ = build_order_by("source", sort_by, False, {"created", "updated"})
            assert cols[-1] == "source.id"

    def test_null_group_sorts_last_by_default(self):
        # (col IS NULL) is False(0) for values, True(1) for NULLs: ASC keeps
        # NULLs last when not reversed, regardless of the value direction.
        for sort_by, datetime_cols in [
            ("label", {"created", "updated"}),
            ("created", {"created", "updated"}),
        ]:
            cols, _ = build_order_by("source", sort_by, False, datetime_cols)
            assert cols[0] == f"(source.{sort_by} IS NULL) ASC"

    def test_null_group_sorts_first_when_reversed(self):
        for sort_by, datetime_cols in [
            ("label", {"created", "updated"}),
            ("created", {"created", "updated"}),
        ]:
            cols, _ = build_order_by("source", sort_by, True, datetime_cols)
            assert cols[0] == f"(source.{sort_by} IS NULL) DESC"
