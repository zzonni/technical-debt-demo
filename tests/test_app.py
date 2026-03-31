import pytest
import models


@pytest.fixture(autouse=True)
def reset_db():
    models._db["users"].clear()
    models._db["tasks"].clear()
    yield


@pytest.fixture
def client():
    # Import inside fixture to avoid module-level side effects
    from app import app
    app.config["TESTING"] = True
    app.config["SECRET_KEY"] = "test-secret"
    with app.test_client() as c:
        yield c


class TestIndex:
    def test_index_returns_200(self, client):
        resp = client.get("/")
        assert resp.status_code == 200


class TestAdd:
    def test_add_redirects(self, client):
        resp = client.post("/add", data={"text": "New task"})
        assert resp.status_code == 302

    def test_add_creates_task(self, client):
        with client.session_transaction() as sess:
            sess["user"] = "alice"
        client.post("/add", data={"text": "New task"})
        tasks = models.list_tasks("alice")
        assert len(tasks) == 1
        assert tasks[0]["text"] == "New task"


class TestToggle:
    def test_toggle_task(self, client):
        task = models.create_task("alice", "t1")
        resp = client.post(f"/toggle/{task['id']}")
        assert resp.status_code == 302
        assert models.find_task(task["id"])["status"] == "done"


class TestAuth:
    def test_login(self, client):
        models.create_user("alice", "pw")
        resp = client.post("/auth/login", data={"username": "alice", "password": "pw"})
        assert resp.status_code == 302

    def test_login_wrong_password(self, client):
        models.create_user("alice", "pw")
        resp = client.post("/auth/login", data={"username": "alice", "password": "wrong"})
        assert resp.status_code == 302

    def test_logout(self, client):
        resp = client.get("/auth/logout")
        assert resp.status_code == 302
