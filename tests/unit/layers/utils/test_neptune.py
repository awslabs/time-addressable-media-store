import pytest

# pylint: disable=no-name-in-module
from neptune import build_order_by, collected_by_where_literal

pytestmark = [
    pytest.mark.unit,
]


class TestBuildOrderBy:
    """Tests for build_order_by.

    build_order_by feeds cymple's order_by, which applies the ASC/DESC keyword
    only to the LAST column. Every column except the id tie-breaker must
    therefore carry its direction inline, and id takes the returned flag.
    These tests assert both the column list and the flag so a regression
    to the broken single-direction form is caught.

    The leading "is NULL" (0/1) term implements the spec fallback rule: records
    with an unset sort value sort after those with one by default, and before
    when reverse_order is set. Its direction depends only on reverse_order, as
    does the id fallback/tie-breaker (ascending unless reversed).
    """

    def test_datetime_default_is_descending(self):
        # created is a datetime key -> newest first (DESC) by default, while
        # the id fallback stays ascending
        cols, id_ascending = build_order_by(
            "source", None, False, {"created", "updated"}
        )
        assert cols == [
            "CASE WHEN source.created IS NULL THEN 1 ELSE 0 END ASC",
            "CASE WHEN source.created IS NULL THEN '' ELSE source.created END DESC",
            "source.id",
        ]
        assert id_ascending is True

    def test_string_default_is_ascending(self):
        # label is a string key -> alphabetical (ASC) by default
        cols, ascending = build_order_by(
            "source", "label", False, {"created", "updated"}
        )
        assert cols == [
            "CASE WHEN source.label IS NULL THEN 1 ELSE 0 END ASC",
            "CASE WHEN source.label IS NULL THEN '' ELSE source.label END ASC",
            "source.id",
        ]
        assert ascending is True

    def test_reverse_flips_datetime_default(self):
        cols, id_ascending = build_order_by(
            "source", "created", True, {"created", "updated"}
        )
        assert cols == [
            "CASE WHEN source.created IS NULL THEN 1 ELSE 0 END DESC",
            "CASE WHEN source.created IS NULL THEN '' ELSE source.created END ASC",
            "source.id",
        ]
        assert id_ascending is False

    def test_reverse_flips_string_default(self):
        cols, ascending = build_order_by(
            "source", "label", True, {"created", "updated"}
        )
        assert cols == [
            "CASE WHEN source.label IS NULL THEN 1 ELSE 0 END DESC",
            "CASE WHEN source.label IS NULL THEN '' ELSE source.label END DESC",
            "source.id",
        ]
        assert ascending is False

    def test_sort_by_none_uses_default_col(self):
        cols, id_ascending = build_order_by(
            "flow", None, False, {"created", "metadata_updated"}
        )
        assert cols == [
            "CASE WHEN flow.created IS NULL THEN 1 ELSE 0 END ASC",
            "CASE WHEN flow.created IS NULL THEN '' ELSE flow.created END DESC",
            "flow.id",
        ]
        assert id_ascending is True

    def test_explicit_default_col_for_fixed_key(self):
        # Webhooks have a single fixed string key (url) supplied via default_col
        cols, ascending = build_order_by(
            "webhook", None, False, set(), default_col="url"
        )
        assert cols == [
            "CASE WHEN webhook.url IS NULL THEN 1 ELSE 0 END ASC",
            "CASE WHEN webhook.url IS NULL THEN '' ELSE webhook.url END ASC",
            "webhook.id",
        ]
        assert ascending is True

    def test_fixed_key_reversed(self):
        cols, ascending = build_order_by(
            "webhook", None, True, set(), default_col="url"
        )
        assert cols == [
            "CASE WHEN webhook.url IS NULL THEN 1 ELSE 0 END DESC",
            "CASE WHEN webhook.url IS NULL THEN '' ELSE webhook.url END DESC",
            "webhook.id",
        ]
        assert ascending is False

    @pytest.mark.parametrize("reverse_order", [False, True])
    def test_id_fallback_direction_depends_only_on_reverse_order(self, reverse_order):
        # The id tie-breaker is the LAST column and has no inline direction; it
        # resolves via the returned flag. Unset records fall back to id order,
        # ascending unless reversed, even when the primary key is DESC (e.g.
        # delete requests without an expiry sorted by expiry).
        cols, id_ascending = build_order_by(
            "delete_request", "expiry", reverse_order, {"created", "expiry"}
        )
        assert cols[-1] == "delete_request.id"
        assert cols[1].endswith("ASC" if reverse_order else "DESC")
        assert id_ascending is not reverse_order

    @pytest.mark.parametrize("reverse_order", [False, True])
    def test_sort_keys_never_yield_null(self, reverse_order):
        # Neptune does not order NULLs stably under SKIP/LIMIT, so every sort
        # key must map an unset value to a constant (regression: pages repeated
        # and dropped records when sorting on a sometimes-unset column).
        cols, _ = build_order_by(
            "flow", "metadata_updated", reverse_order, {"created", "metadata_updated"}
        )
        assert cols[0].startswith("CASE WHEN flow.metadata_updated IS NULL THEN 1")
        assert cols[1].startswith(
            "CASE WHEN flow.metadata_updated IS NULL THEN '' ELSE flow.metadata_updated END"
        )

    def test_id_always_present_as_secondary_key(self):
        for sort_by in [None, "created", "updated", "label"]:
            cols, _ = build_order_by("source", sort_by, False, {"created", "updated"})
            assert cols[-1] == "source.id"

    def test_null_group_sorts_last_by_default(self):
        # The leading term is 0 for values and 1 for NULLs: ASC keeps
        # NULLs last when not reversed, regardless of the value direction.
        for sort_by, datetime_cols in [
            ("label", {"created", "updated"}),
            ("created", {"created", "updated"}),
        ]:
            cols, _ = build_order_by("source", sort_by, False, datetime_cols)
            assert (
                cols[0] == f"CASE WHEN source.{sort_by} IS NULL THEN 1 ELSE 0 END ASC"
            )

    def test_null_group_sorts_first_when_reversed(self):
        for sort_by, datetime_cols in [
            ("label", {"created", "updated"}),
            ("created", {"created", "updated"}),
        ]:
            cols, _ = build_order_by("source", sort_by, True, datetime_cols)
            assert (
                cols[0] == f"CASE WHEN source.{sort_by} IS NULL THEN 1 ELSE 0 END DESC"
            )


class TestCollectedByWhereLiteral:
    """Tests for collected_by_where_literal.

    The filter is three-state and every state is driven by the raw query string,
    so None and "" must stay distinct: absent means "apply no filter", empty
    means "only entities that nothing collects". Both are falsy, so a truthiness
    check (`if not collected_by_ids`) would collapse them and silently turn an
    unfiltered list request into an uncollected-only one. That split is asserted
    explicitly rather than left to coincidence.

    The returned literal is appended to a list of WHERE clauses that are joined
    with AND, so the id form must be bracketed as a single unit -- unbracketed,
    `a OR b` would bind as `... AND a OR b` and widen the result set. Hence the
    assertions on exact bracketing rather than on substrings. The uncollected
    form needs no brackets because `NOT exists(...)` is already a single term.

    The patterns are the real ones from query_flows, so the expected values are
    the literals actually sent to Neptune.
    """

    NOT_COLLECTED = "(flow)-[:collected_by]->(:flow)"
    ID_A = "10000000-0000-1000-8000-000000000003"
    ID_B = "10000000-0000-1000-8000-000000000009"

    @staticmethod
    def by_id(collected_by_id):
        return f'(flow)-[:collected_by]->(:flow {{id: "{collected_by_id}"}})'

    def test_none_applies_no_filter(self):
        assert collected_by_where_literal(None, self.NOT_COLLECTED, self.by_id) is None

    def test_empty_string_selects_uncollected(self):
        assert (
            collected_by_where_literal("", self.NOT_COLLECTED, self.by_id)
            == "NOT exists((flow)-[:collected_by]->(:flow))"
        )

    def test_none_and_empty_string_are_not_equivalent(self):
        # Both are falsy but only "" is a filter; guards the collapse regression.
        assert collected_by_where_literal(
            None, self.NOT_COLLECTED, self.by_id
        ) != collected_by_where_literal("", self.NOT_COLLECTED, self.by_id)

    def test_single_id_is_wrapped_in_exists(self):
        assert collected_by_where_literal(
            self.ID_A, self.NOT_COLLECTED, self.by_id
        ) == (f'(exists((flow)-[:collected_by]->(:flow {{id: "{self.ID_A}"}})))')

    def test_multiple_ids_are_or_ed_within_one_group(self):
        assert collected_by_where_literal(
            f"{self.ID_A},{self.ID_B}", self.NOT_COLLECTED, self.by_id
        ) == (
            f'(exists((flow)-[:collected_by]->(:flow {{id: "{self.ID_A}"}}))'
            f' OR exists((flow)-[:collected_by]->(:flow {{id: "{self.ID_B}"}})))'
        )

    def test_id_form_is_bracketed_for_any_number_of_ids(self):
        for value in [self.ID_A, f"{self.ID_A},{self.ID_B}"]:
            literal = collected_by_where_literal(value, self.NOT_COLLECTED, self.by_id)
            assert literal.startswith("(")
            assert literal.endswith(")")
