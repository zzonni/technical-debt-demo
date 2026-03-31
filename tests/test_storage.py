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
