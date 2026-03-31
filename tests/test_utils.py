import pytest
from utils import summarize_counts, search_items


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
