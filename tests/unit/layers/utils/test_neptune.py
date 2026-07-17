import pytest

# pylint: disable=no-name-in-module
from neptune import build_order_by

pytestmark = [
    pytest.mark.unit,
]


class TestBuildOrderBy:
    """Tests for build_order_by.

    build_order_by feeds cymple's order_by, which applies the ASC/DESC keyword
    only to the LAST column. The primary column must therefore carry its
    direction inline, and the id tie-breaker takes the returned `ascending`
    flag. These tests assert both the column list and the flag so a regression
    to the broken single-direction form is caught.
    """

    def test_datetime_default_is_descending(self):
        # created is a datetime key -> newest first (DESC) by default
        cols, ascending = build_order_by("source", None, False, {"created", "updated"})
        assert cols == ["source.created DESC", "source.id"]
        assert ascending is False

    def test_string_default_is_ascending(self):
        # label is a string key -> alphabetical (ASC) by default
        cols, ascending = build_order_by(
            "source", "label", False, {"created", "updated"}
        )
        assert cols == ["source.label ASC", "source.id"]
        assert ascending is True

    def test_reverse_flips_datetime_default(self):
        cols, ascending = build_order_by(
            "source", "created", True, {"created", "updated"}
        )
        assert cols == ["source.created ASC", "source.id"]
        assert ascending is True

    def test_reverse_flips_string_default(self):
        cols, ascending = build_order_by(
            "source", "label", True, {"created", "updated"}
        )
        assert cols == ["source.label DESC", "source.id"]
        assert ascending is False

    def test_sort_by_none_uses_default_col(self):
        cols, ascending = build_order_by(
            "flow", None, False, {"created", "metadata_updated"}
        )
        assert cols == ["flow.created DESC", "flow.id"]
        assert ascending is False

    def test_explicit_default_col_for_fixed_key(self):
        # Webhooks have a single fixed string key (url) supplied via default_col
        cols, ascending = build_order_by(
            "webhook", None, False, set(), default_col="url"
        )
        assert cols == ["webhook.url ASC", "webhook.id"]
        assert ascending is True

    def test_fixed_key_reversed(self):
        cols, ascending = build_order_by(
            "webhook", None, True, set(), default_col="url"
        )
        assert cols == ["webhook.url DESC", "webhook.id"]
        assert ascending is False

    def test_tie_breaker_direction_matches_ascending_flag(self):
        # The id tie-breaker is the LAST column and has no inline direction; it
        # must resolve (via the ascending flag) to the same direction as the
        # primary column so pagination order is a total order.
        cols, ascending = build_order_by(
            "delete_request", "expiry", False, {"created", "expiry"}
        )
        assert cols[-1] == "delete_request.id"
        assert cols[0].endswith("DESC")
        assert ascending is False

    def test_id_always_present_as_secondary_key(self):
        for sort_by in [None, "created", "updated", "label"]:
            cols, _ = build_order_by("source", sort_by, False, {"created", "updated"})
            assert cols[-1] == "source.id"
