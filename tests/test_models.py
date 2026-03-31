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
        assert models._db["users"]["alice"]["password"] == "pass123"

    def test_overwrites_existing_user(self):
        models.create_user("alice", "old")
        models.create_user("alice", "new")
        assert models._db["users"]["alice"]["password"] == "new"


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
