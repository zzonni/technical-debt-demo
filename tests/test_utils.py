import pytest
from utils import summarize_counts, search_items, filter_and_sort_items, compute_item_metrics, format_items_for_display


class TestSummarizeCounts:
    def test_all_open(self):
        items = [{"status": "open"}, {"status": "open"}]
        result = summarize_counts(items)
        assert result == {"open": 2, "done": 0, "total": 2}

    def test_all_done(self):
        items = [{"status": "done"}, {"status": "done"}]
        result = summarize_counts(items)
        assert result == {"open": 0, "done": 2, "total": 2}

    def test_mixed(self):
        items = [{"status": "open"}, {"status": "done"}, {"status": "open"}]
        result = summarize_counts(items)
        assert result == {"open": 2, "done": 1, "total": 3}

    def test_empty(self):
        result = summarize_counts([])
        assert result == {"open": 0, "done": 0, "total": 0}

    def test_missing_status_counts_as_open(self):
        items = [{}]
        result = summarize_counts(items)
        assert result == {"open": 1, "done": 0, "total": 1}


class TestSearchItems:
    def test_finds_matching_items(self):
        items = [{"text": "Buy milk"}, {"text": "Walk dog"}]
        result = search_items(items, "milk")
        assert len(result) == 1
        assert result[0]["text"] == "Buy milk"

    def test_case_insensitive(self):
        items = [{"text": "Buy MILK"}]
        result = search_items(items, "milk")
        assert len(result) == 1

    def test_empty_query_returns_all(self):
        items = [{"text": "a"}, {"text": "b"}]
        result = search_items(items, "")
        assert len(result) == 2

    def test_none_query_returns_all(self):
        items = [{"text": "a"}]
        result = search_items(items, None)
        assert len(result) == 1

    def test_no_match(self):
        items = [{"text": "Buy milk"}]
        result = search_items(items, "xyz")
        assert len(result) == 0


class TestFilterAndSortItems:
    def _items(self):
        return [
            {"text": "Buy milk", "status": "open", "category": "Shopping", "owner": "alice", "priority": 3},
            {"text": "Fix bug", "status": "done", "category": "Work", "owner": "bob", "priority": 7},
            {"text": "Walk dog", "status": "open", "category": "Personal", "owner": "alice", "priority": 1},
        ]

    def test_no_filters(self):
        result = filter_and_sort_items(
            self._items(), None, None, None, None, None, None, None, None, None, 0
        )
        assert result["total_matched"] == 3
        assert result["returned"] == 3

    def test_status_filter(self):
        result = filter_and_sort_items(
            self._items(), "open", None, None, None, None, None, None, None, None, 0
        )
        assert result["total_matched"] == 2

    def test_category_filter(self):
        result = filter_and_sort_items(
            self._items(), None, "Work", None, None, None, None, None, None, None, 0
        )
        assert result["total_matched"] == 1

    def test_owner_filter(self):
        result = filter_and_sort_items(
            self._items(), None, None, "alice", None, None, None, None, None, None, 0
        )
        assert result["total_matched"] == 2

    def test_priority_range(self):
        result = filter_and_sort_items(
            self._items(), None, None, None, 2, 5, None, None, None, None, 0
        )
        assert result["total_matched"] == 1

    def test_text_query(self):
        result = filter_and_sort_items(
            self._items(), None, None, None, None, None, "milk", None, None, None, 0
        )
        assert result["total_matched"] == 1

    def test_sort_asc(self):
        result = filter_and_sort_items(
            self._items(), None, None, None, None, None, None, "priority", "asc", None, 0
        )
        assert result["items"][0]["priority"] == 1

    def test_sort_desc(self):
        result = filter_and_sort_items(
            self._items(), None, None, None, None, None, None, "priority", "desc", None, 0
        )
        assert result["items"][0]["priority"] == 7

    def test_pagination(self):
        result = filter_and_sort_items(
            self._items(), None, None, None, None, None, None, None, None, 2, 0
        )
        assert result["returned"] == 2
        result2 = filter_and_sort_items(
            self._items(), None, None, None, None, None, None, None, None, 2, 2
        )
        assert result2["returned"] == 1


class TestComputeItemMetrics:
    def test_empty_items(self):
        assert compute_item_metrics([]) == {}

    def test_basic_metrics(self):
        items = [
            {"status": "open", "category": "A", "priority": 5, "text": "hello"},
            {"status": "done", "category": "B", "priority": 3, "text": "world!"},
        ]
        result = compute_item_metrics(items)
        assert result["total"] == 2
        assert result["status_distribution"]["open"] == 1
        assert result["status_distribution"]["done"] == 1
        assert result["category_distribution"]["A"] == 1
        assert result["avg_priority"] == 4.0
        assert result["max_text_length"] == 6
        assert result["min_text_length"] == 5

    def test_defaults(self):
        items = [{}]
        result = compute_item_metrics(items)
        assert result["status_distribution"]["unknown"] == 1
        assert result["category_distribution"]["uncategorized"] == 1


class TestFormatItemsForDisplay:
    def _items(self):
        return [
            {"id": 1, "text": "Buy milk", "status": "open", "priority": 3, "created_at": "2024-01-01"},
            {"id": 2, "text": "Fix bug", "status": "done", "priority": 7, "created_at": "2024-02-01"},
        ]

    def test_compact_format(self):
        result = format_items_for_display(
            self._items(), "compact", None, False, False, False, False,
            None, 0, "-", "plain"
        )
        assert "[open] Buy milk" in result
        assert "[done] Fix bug" in result

    def test_detailed_format(self):
        result = format_items_for_display(
            self._items(), "detailed", None, False, False, False, False,
            None, 0, "-", "plain"
        )
        assert "ID: 1" in result

    def test_minimal_format(self):
        result = format_items_for_display(
            self._items(), "minimal", None, False, False, False, False,
            None, 0, "-", "plain"
        )
        assert "Buy milk" in result

    def test_unknown_format(self):
        result = format_items_for_display(
            self._items(), "unknown", None, False, False, False, False,
            None, 0, "-", "plain"
        )
        assert "Buy milk" in result

    def test_text_truncation(self):
        result = format_items_for_display(
            self._items(), "compact", 3, False, False, False, False,
            None, 0, "-", "plain"
        )
        assert "Buy..." in result

    def test_show_priority(self):
        result = format_items_for_display(
            self._items(), "compact", None, False, True, False, False,
            None, 0, "-", "plain"
        )
        assert "(P3)" in result

    def test_show_dates(self):
        result = format_items_for_display(
            self._items(), "compact", None, False, False, True, False,
            None, 0, "-", "plain"
        )
        assert "[2024-01-01]" in result

    def test_group_by_status(self):
        result = format_items_for_display(
            self._items(), "compact", None, False, False, False, False,
            "status", 0, "-", "plain"
        )
        assert "open" in result
        assert "done" in result

    def test_header_format_uppercase(self):
        result = format_items_for_display(
            self._items(), "compact", None, False, False, False, False,
            "status", 0, "-", "uppercase"
        )
        assert "OPEN" in result

    def test_header_format_title(self):
        result = format_items_for_display(
            self._items(), "compact", None, False, False, False, False,
            "status", 0, "-", "title"
        )
        assert "Open" in result

    def test_indent(self):
        result = format_items_for_display(
            self._items(), "compact", None, False, False, False, False,
            None, 4, "-", "plain"
        )
        assert "    [open]" in result
