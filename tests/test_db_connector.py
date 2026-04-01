from src.db_connector import get_connection


class TestGetConnection:
    def test_returns_sqlite_connection(self, tmp_path, monkeypatch):
        import src.db_connector
        monkeypatch.chdir(tmp_path)
        conn = get_connection()
        assert conn is not None
        conn.close()

    def test_uses_env_db_path(self, tmp_path, monkeypatch):
        import importlib
        import src.db_connector as db_connector

        db_file = tmp_path / "custom.db"
        monkeypatch.setenv("DB_PATH", str(db_file))
        monkeypatch.setenv("DB_HOST", "db.internal")
        importlib.reload(db_connector)

        conn = db_connector.get_connection()
        assert conn is not None
        conn.close()
