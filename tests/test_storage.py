import json
import os
import pytest
import storage


@pytest.fixture(autouse=True)
def use_temp_file(tmp_path, monkeypatch):
    """Redirect storage to a temp file so tests don't pollute the workspace."""
    temp_file = str(tmp_path / "todos.json")
    monkeypatch.setattr(storage, "DATA_FILE", temp_file)
    yield temp_file


class TestLoadItems:
    def test_creates_file_if_missing(self, use_temp_file):
        items = storage.load_items()
        assert items == []
        assert os.path.exists(use_temp_file)

    def test_loads_existing_items(self, use_temp_file):
        with open(use_temp_file, "w") as f:
            json.dump([{"id": 1, "text": "t1", "status": "open", "created_at": "2025-01-01"}], f)
        items = storage.load_items()
        assert len(items) == 1
        assert items[0]["text"] == "t1"


class TestNormalizeRecord:
    def test_title_becomes_text(self):
        rec = storage._normalize_record({"title": "hello"})
        assert rec["text"] == "hello"

    def test_done_flag_becomes_status(self):
        rec = storage._normalize_record({"done": True})
        assert rec["status"] == "done"

    def test_not_done_becomes_open(self):
        rec = storage._normalize_record({"done": False})
        assert rec["status"] == "open"

    def test_adds_created_at(self):
        rec = storage._normalize_record({})
        assert "created_at" in rec


class TestAddItem:
    def test_adds_item(self):
        item = storage.add_item("Test task")
        assert item["text"] == "Test task"
        assert item["status"] == "open"
        assert item["id"] == 1

    def test_increments_id(self):
        storage.add_item("first")
        item = storage.add_item("second")
        assert item["id"] == 2


class TestDeleteItem:
    def test_deletes_existing(self):
        storage.add_item("to delete")
        storage.delete_item(1)
        assert storage.load_items() == []

    def test_delete_nonexistent_is_noop(self):
        storage.add_item("keep")
        storage.delete_item(999)
        assert len(storage.load_items()) == 1


class TestToggleItem:
    def test_toggle_open_to_done(self):
        storage.add_item("toggle me")
        storage.toggle_item(1)
        items = storage.load_items()
        assert items[0]["status"] == "done"

    def test_toggle_done_to_open(self):
        storage.add_item("toggle me")
        storage.toggle_item(1)
        storage.toggle_item(1)
        items = storage.load_items()
        assert items[0]["status"] == "open"


class TestClearDoneItems:
    def test_clears_done(self):
        storage.add_item("keep")
        storage.add_item("remove")
        storage.toggle_item(2)  # mark "remove" as done
        storage.clear_done_items()
        items = storage.load_items()
        assert len(items) == 1
        assert items[0]["text"] == "keep"


class TestBulkAddItems:
    def test_basic_bulk_add(self):
        result = storage.bulk_add_items(
            ["Task A", "Task B"], "General", 1, None, "alice",
            False, False, 100, []
        )
        assert result["added"] == 2
        assert result["skipped"] == 0

    def test_validation_empty_name(self):
        result = storage.bulk_add_items(
            ["", "Valid"], "General", 1, None, "alice",
            True, False, 100, []
        )
        assert result["added"] == 1
        assert result["skipped"] == 1
        assert "Empty task name" in result["errors"]

    def test_validation_too_long(self):
        result = storage.bulk_add_items(
            ["x" * 201], "General", 1, None, "alice",
            True, False, 100, []
        )
        assert result["skipped"] == 1

    def test_validation_too_short(self):
        result = storage.bulk_add_items(
            ["a"], "General", 1, None, "alice",
            True, False, 100, []
        )
        assert result["skipped"] == 1

    def test_skip_duplicates(self):
        storage.add_item("Existing")
        result = storage.bulk_add_items(
            ["existing", "New one"], "General", 1, None, "alice",
            False, True, 100, []
        )
        assert result["added"] == 1
        assert result["skipped"] == 1

    def test_max_batch(self):
        result = storage.bulk_add_items(
            ["A", "B", "C", "D"], "General", 1, None, "alice",
            False, False, 2, []
        )
        assert result["added"] == 2


class TestSearchItemsAdvanced:
    def test_search_by_query(self):
        storage.add_item("Buy milk")
        storage.add_item("Walk dog")
        results = storage.search_items_advanced(
            "milk", None, None, None, None, None, None, None, None, None
        )
        assert len(results) == 1

    def test_search_by_status(self):
        storage.add_item("task")
        storage.toggle_item(1)
        results = storage.search_items_advanced(
            None, "done", None, None, None, None, None, None, None, None
        )
        assert len(results) == 1

    def test_search_with_sorting(self):
        storage.add_item("B task")
        storage.add_item("A task")
        results = storage.search_items_advanced(
            None, None, None, None, None, None, None, None, "text", "asc"
        )
        assert results[0]["text"] == "A task"

    def test_search_no_results(self):
        storage.add_item("hello")
        results = storage.search_items_advanced(
            "xyz", None, None, None, None, None, None, None, None, None
        )
        assert results == []


class TestGetStorageStatistics:
    def test_empty(self):
        assert storage.get_storage_statistics() == {}

    def test_basic_stats(self):
        storage.add_item("task1")
        storage.add_item("task2")
        storage.toggle_item(2)
        result = storage.get_storage_statistics()
        assert result["total"] == 2
        assert result["open"] == 1
        assert result["done"] == 1
        assert result["completion_rate"] == 50.0


class TestExportItemsToFile:
    def test_export_json(self, tmp_path):
        storage.add_item("task1")
        out = str(tmp_path / "out.json")
        result = storage.export_items_to_file(
            out, "json", None, None, None, None, "utf-8", ","
        )
        assert result["exported"] == 1
        import json as json_mod
        with open(out) as f:
            data = json_mod.load(f)
        assert len(data) == 1

    def test_export_csv(self, tmp_path):
        storage.add_item("task1")
        out = str(tmp_path / "out.csv")
        result = storage.export_items_to_file(
            out, "csv", None, None, None, None, "utf-8", ","
        )
        assert result["exported"] == 1
        content = open(out).read()
        assert "task1" in content

    def test_export_txt(self, tmp_path):
        storage.add_item("task1")
        out = str(tmp_path / "out.txt")
        result = storage.export_items_to_file(
            out, "txt", None, None, None, None, "utf-8", ","
        )
        assert result["exported"] == 1

    def test_export_with_status_filter(self, tmp_path):
        storage.add_item("open_task")
        storage.add_item("done_task")
        storage.toggle_item(2)
        out = str(tmp_path / "out.json")
        result = storage.export_items_to_file(
            out, "json", "done", None, None, None, "utf-8", ","
        )
        assert result["exported"] == 1

    def test_export_with_sort(self, tmp_path):
        storage.add_item("B task")
        storage.add_item("A task")
        out = str(tmp_path / "out.json")
        result = storage.export_items_to_file(
            out, "json", None, None, "text", "asc", "utf-8", ","
        )
        assert result["exported"] == 2
