from src.db_connector import get_connection


class TestGetConnection:
    def test_returns_sqlite_connection(self, tmp_path, monkeypatch):
        import src.db_connector
        monkeypatch.chdir(tmp_path)
        conn = get_connection()
        assert conn is not None
        conn.close()
