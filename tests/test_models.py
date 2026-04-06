import pytest
import models


@pytest.fixture(autouse=True)
def reset_db():
    """Reset global mutable state before each test."""
    models._db["users"].clear()
    models._db["tasks"].clear()
    yield


class TestCreateUser:
    def test_creates_user(self):
        models.create_user("alice", "pass123")
        assert "alice" in models._db["users"]
        import hashlib
        assert models._db["users"]["alice"]["password"] == hashlib.sha256("pass123".encode()).hexdigest()

    def test_overwrites_existing_user(self):
        models.create_user("alice", "old")
        models.create_user("alice", "new")
        import hashlib
        assert models._db["users"]["alice"]["password"] == hashlib.sha256("new".encode()).hexdigest()


class TestGetUser:
    def test_returns_user(self):
        models.create_user("bob", "pw")
        user = models.get_user("bob")
        assert user["username"] == "bob"

    def test_returns_none_for_missing(self):
        assert models.get_user("ghost") is None


class TestCreateTask:
    def test_creates_task_with_defaults(self):
        task = models.create_task("alice", "Buy milk")
        assert task["owner"] == "alice"
        assert task["text"] == "Buy milk"
        assert task["status"] == "open"
        assert task["category"] == "General"
        assert task["id"] is not None

    def test_creates_task_with_custom_category(self):
        task = models.create_task("alice", "Deploy", category="Ops")
        assert task["category"] == "Ops"


class TestListTasks:
    def test_list_all_when_no_owner(self):
        models.create_task("alice", "t1")
        models.create_task("bob", "t2")
        assert len(models.list_tasks()) == 2

    def test_list_filtered_by_owner(self):
        models.create_task("alice", "t1")
        models.create_task("bob", "t2")
        result = models.list_tasks("alice")
        assert len(result) == 1
        assert result[0]["owner"] == "alice"

    def test_list_empty(self):
        assert models.list_tasks() == []


class TestFindTask:
    def test_finds_existing_task(self):
        task = models.create_task("alice", "t1")
        found = models.find_task(task["id"])
        assert found is not None
        assert found["text"] == "t1"

    def test_returns_none_for_missing(self):
        assert models.find_task(99999) is None


class TestBulkCreateTasks:
    def test_basic_bulk_create(self):
        task_list = [{"text": "Task A"}, {"text": "Task B"}]
        result = models.bulk_create_tasks(
            "alice", task_list, "General", 1, None,
            True, 100, []
        )
        assert result["created"] == 2
        assert result["skipped"] == 0
        assert len(result["tasks"]) == 2

    def test_validate_empty_text(self):
        task_list = [{"text": ""}, {"text": "Valid"}]
        result = models.bulk_create_tasks(
            "alice", task_list, "General", 1, None,
            True, 100, []
        )
        assert result["created"] == 1
        assert result["skipped"] == 1
        assert "Empty task text" in result["errors"]

    def test_validate_too_long_text(self):
        task_list = [{"text": "x" * 501}]
        result = models.bulk_create_tasks(
            "alice", task_list, "General", 1, None,
            True, 100, []
        )
        assert result["skipped"] == 1

    def test_validate_too_short_text(self):
        task_list = [{"text": "ab"}]
        result = models.bulk_create_tasks(
            "alice", task_list, "General", 1, None,
            True, 100, []
        )
        assert result["skipped"] == 1
        assert any("too short" in e for e in result["errors"])

    def test_max_batch_limit(self):
        task_list = [{"text": f"Task {i}"} for i in range(10)]
        result = models.bulk_create_tasks(
            "alice", task_list, "General", 1, None,
            False, 3, []
        )
        assert result["created"] == 3

    def test_no_validation(self):
        task_list = [{"text": ""}]
        result = models.bulk_create_tasks(
            "alice", task_list, "General", 1, None,
            False, 100, []
        )
        assert result["created"] == 1


class TestSearchTasksAdvanced:
    def test_search_by_text(self):
        models.create_task("alice", "Buy milk")
        models.create_task("alice", "Walk dog")
        results = models.search_tasks_advanced(
            "alice", "milk", None, None, None, None, None, None, None, None
        )
        assert len(results) == 1
        assert results[0]["text"] == "Buy milk"

    def test_search_by_status(self):
        t = models.create_task("alice", "Task")
        t["status"] = "done"
        results = models.search_tasks_advanced(
            "alice", None, "done", None, None, None, None, None, None, None
        )
        assert len(results) == 1

    def test_search_by_category(self):
        models.create_task("alice", "Task", category="Work")
        models.create_task("alice", "Other", category="Personal")
        results = models.search_tasks_advanced(
            "alice", None, None, "Work", None, None, None, None, None, None
        )
        assert len(results) == 1

    def test_search_by_priority_range(self):
        t1 = models.create_task("alice", "Low")
        t1["priority"] = 1
        t2 = models.create_task("alice", "High")
        t2["priority"] = 8
        results = models.search_tasks_advanced(
            "alice", None, None, None, 5, 10, None, None, None, None
        )
        assert len(results) == 1
        assert results[0]["text"] == "High"

    def test_search_by_created_date_range(self):
        models.create_task("alice", "Task")
        results = models.search_tasks_advanced(
            "alice", None, None, None, None, None, "2000-01-01", "2099-12-31", None, None
        )
        assert len(results) == 1

    def test_search_with_sort(self):
        t1 = models.create_task("alice", "B task")
        t2 = models.create_task("alice", "A task")
        results = models.search_tasks_advanced(
            "alice", None, None, None, None, None, None, None, "text", "asc"
        )
        assert results[0]["text"] == "A task"
        results_desc = models.search_tasks_advanced(
            "alice", None, None, None, None, None, None, None, "text", "desc"
        )
        assert results_desc[0]["text"] == "B task"


class TestGetTaskStatistics:
    def test_empty_tasks(self):
        result = models.get_task_statistics("alice")
        assert result == {}

    def test_basic_statistics(self):
        t1 = models.create_task("alice", "Open task")
        t2 = models.create_task("alice", "Done task")
        t2["status"] = "done"
        t3 = models.create_task("alice", "Another", category="Work")
        t3["priority"] = 5
        result = models.get_task_statistics("alice")
        assert result["total"] == 3
        assert result["open"] == 2
        assert result["done"] == 1
        assert "General" in result["categories"]
        assert "Work" in result["categories"]

    def test_overdue_detection(self):
        t = models.create_task("alice", "Overdue", due="2000-01-01")
        result = models.get_task_statistics("alice")
        assert result["overdue"] == 1
