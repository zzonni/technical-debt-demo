import pytest
from unittest.mock import patch, MagicMock, mock_open
import user_manager


class TestValidateUserPermissions:
    @patch("user_manager.find_user_by_name")
    def test_admin_can_do_anything(self, mock_find):
        mock_find.return_value = {"id": 1, "username": "admin", "email": "a@b.com", "role": "admin"}
        assert user_manager.validate_user_permissions("admin", "orders", "delete") is True
        assert user_manager.validate_user_permissions("admin", "users", "write") is True

    @patch("user_manager.find_user_by_name")
    def test_manager_read_write_update(self, mock_find):
        mock_find.return_value = {"id": 2, "username": "mgr", "email": "m@b.com", "role": "manager"}
        assert user_manager.validate_user_permissions("mgr", "orders", "read") is True
        assert user_manager.validate_user_permissions("mgr", "orders", "write") is True
        assert user_manager.validate_user_permissions("mgr", "orders", "update") is True

    @patch("user_manager.find_user_by_name")
    def test_manager_cannot_delete(self, mock_find):
        mock_find.return_value = {"id": 2, "username": "mgr", "email": "m@b.com", "role": "manager"}
        assert user_manager.validate_user_permissions("mgr", "orders", "delete") is False

    @patch("user_manager.find_user_by_name")
    def test_user_can_read_only(self, mock_find):
        mock_find.return_value = {"id": 3, "username": "usr", "email": "u@b.com", "role": "user"}
        assert user_manager.validate_user_permissions("usr", "orders", "read") is True
        assert user_manager.validate_user_permissions("usr", "orders", "write") is False
        assert user_manager.validate_user_permissions("usr", "orders", "update") is False
        assert user_manager.validate_user_permissions("usr", "orders", "delete") is False

    @patch("user_manager.find_user_by_name")
    def test_nonexistent_user(self, mock_find):
        mock_find.return_value = None
        assert user_manager.validate_user_permissions("ghost", "orders", "read") is False

    @patch("user_manager.find_user_by_name")
    def test_unknown_role(self, mock_find):
        mock_find.return_value = {"id": 4, "username": "x", "email": "x@b.com", "role": "guest"}
        assert user_manager.validate_user_permissions("x", "orders", "read") is False


class TestCreateUserAccount:
    @patch("user_manager.get_db")
    def test_create(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.create_user_account("alice", "pass123", "a@b.com", "user")
        assert result["username"] == "alice"
        assert result["email"] == "a@b.com"
        assert result["role"] == "user"
        mock_cursor.execute.assert_called_once()
        mock_get_db.return_value.commit.assert_called_once()


class TestUpdateUserAccount:
    @patch("user_manager.get_db")
    def test_update(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        user_manager.update_user_account("alice", "new@b.com", "admin")
        mock_cursor.execute.assert_called_once()
        mock_get_db.return_value.commit.assert_called_once()


class TestDeleteUserAccount:
    @patch("user_manager.get_db")
    def test_delete(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        user_manager.delete_user_account("alice")
        mock_cursor.execute.assert_called_once()
        mock_get_db.return_value.commit.assert_called_once()


class TestFindUserByName:
    @patch("user_manager.get_db")
    def test_found(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, "alice", "hash", "a@b.com", "user")
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.find_user_by_name("alice")
        assert result["username"] == "alice"
        assert result["email"] == "a@b.com"

    @patch("user_manager.get_db")
    def test_not_found(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.find_user_by_name("ghost")
        assert result is None


class TestFindUserByEmail:
    @patch("user_manager.get_db")
    def test_found(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, "bob", "hash", "b@b.com", "admin")
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.find_user_by_email("b@b.com")
        assert result["username"] == "bob"
        assert result["role"] == "admin"

    @patch("user_manager.get_db")
    def test_not_found(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_get_db.return_value.cursor.return_value = mock_cursor
        assert user_manager.find_user_by_email("no@no.com") is None


class TestListAllUsers:
    @patch("user_manager.get_db")
    def test_list_all(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "alice", "h", "a@b.com", "user"),
            (2, "bob", "h", "b@b.com", "admin"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.list_all_users()
        assert len(result) == 2
        assert result[0]["username"] == "alice"
        assert result[1]["role"] == "admin"

    @patch("user_manager.get_db")
    def test_list_filtered(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(2, "bob", "h", "b@b.com", "admin")]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.list_all_users(role_filter="admin")
        assert len(result) == 1
        assert result[0]["role"] == "admin"

    @patch("user_manager.get_db")
    def test_list_empty(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_db.return_value.cursor.return_value = mock_cursor
        assert user_manager.list_all_users() == []


class TestExportUsersCsv:
    @patch("user_manager.list_all_users")
    def test_export(self, mock_list, tmp_path):
        mock_list.return_value = [
            {"id": 1, "username": "alice", "email": "a@b.com", "role": "user"},
        ]
        out = tmp_path / "users.csv"
        count = user_manager.export_users_csv(str(out))
        assert count == 1
        content = out.read_text()
        assert "id,username,email,role" in content
        assert "alice" in content


class TestImportUsersCsv:
    @patch("user_manager.create_user_account")
    def test_import(self, mock_create, tmp_path):
        f = tmp_path / "users.csv"
        f.write_text("id,username,email,role\n1,alice,a@b.com,user\n2,bob,b@b.com,admin\n")
        count = user_manager.import_users_csv(str(f))
        assert count == 2
        assert mock_create.call_count == 2


class TestBackupUserDatabase:
    @patch("user_manager.shutil.copy2")
    def test_backup(self, mock_copy2):
        result = user_manager.backup_user_database("/tmp/backups")
        assert result == "/tmp/backups"
        mock_copy2.assert_called_once()


class TestRestoreUserDatabase:
    @patch("user_manager.shutil.copy2")
    def test_restore(self, mock_copy2):
        result = user_manager.restore_user_database("/tmp/backups/users.db")
        assert result is True
        mock_copy2.assert_called_once_with("/tmp/backups/users.db", user_manager.DB_FILE)


class TestGetUserActivityLog:
    @patch("user_manager.get_db")
    def test_activity_log(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "alice", "login", "dashboard", "2024-01-01T10:00:00"),
            (2, "alice", "view", "orders", "2024-01-01T10:05:00"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.get_user_activity_log("alice")
        assert len(result) == 2
        assert result[0]["action"] == "login"
        assert result[1]["resource"] == "orders"

    @patch("user_manager.get_db")
    def test_empty_log(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_db.return_value.cursor.return_value = mock_cursor
        assert user_manager.get_user_activity_log("ghost") == []


class TestGetAdminActivityLog:
    @patch("user_manager.get_db")
    def test_admin_log(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "admin", "delete_user", "users", "2024-01-01T10:00:00"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.get_admin_activity_log("admin")
        assert len(result) == 1
        assert result[0]["action"] == "delete_user"


class TestGetDb:
    @patch("user_manager.sqlite3.connect")
    def test_returns_connection(self, mock_connect):
        mock_connect.return_value = MagicMock()
        conn = user_manager.get_db()
        mock_connect.assert_called_once_with("ecommerce.db")
        assert conn is not None


class TestBulkUpdateUsers:
    def _user_row(self, username="alice", email="a@b.com", role="user"):
        return (1, username, "hash", email, role, "2024-01-01")

    @patch("user_manager.get_db")
    def test_basic_update(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._user_row()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.bulk_update_users(
            [{"username": "alice", "email": "new@b.com", "role": "admin"}],
            dry_run=False, validate_email=False, send_notification=False,
            admin_user="admin", reason="test", batch_id="b1",
            log_changes=True, rollback_on_error=False, strict_mode=False,
        )
        assert result["updated"] == 1
        assert result["skipped"] == 0
        assert len(result["changes"]) == 1

    @patch("user_manager.get_db")
    def test_dry_run(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._user_row()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.bulk_update_users(
            [{"username": "alice", "email": "new@b.com", "role": "admin"}],
            dry_run=True, validate_email=False, send_notification=False,
            admin_user="admin", reason="test", batch_id="b1",
            log_changes=False, rollback_on_error=False, strict_mode=False,
        )
        assert result["updated"] == 1

    @patch("user_manager.get_db")
    def test_user_not_found(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.bulk_update_users(
            [{"username": "ghost", "email": "x@b.com", "role": "user"}],
            dry_run=False, validate_email=False, send_notification=False,
            admin_user="admin", reason="test", batch_id="b1",
            log_changes=False, rollback_on_error=False, strict_mode=False,
        )
        assert result["skipped"] == 1
        assert "ghost" in result["errors"][0]

    @patch("user_manager.get_db")
    def test_user_not_found_rollback_strict(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.bulk_update_users(
            [{"username": "ghost", "email": "x@b.com", "role": "user"}],
            dry_run=False, validate_email=False, send_notification=False,
            admin_user="admin", reason="test", batch_id="b1",
            log_changes=False, rollback_on_error=True, strict_mode=True,
        )
        assert result["status"] == "rolled_back"

    @patch("user_manager.get_db")
    def test_email_validation_invalid(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._user_row()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.bulk_update_users(
            [{"username": "alice", "email": "invalid", "role": "user"}],
            dry_run=False, validate_email=True, send_notification=False,
            admin_user="admin", reason="test", batch_id="b1",
            log_changes=False, rollback_on_error=False, strict_mode=False,
        )
        assert result["skipped"] == 1

    @patch("user_manager.get_db")
    def test_email_validation_too_long(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._user_row()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.bulk_update_users(
            [{"username": "alice", "email": "a" * 250 + "@b.com", "role": "user"}],
            dry_run=False, validate_email=True, send_notification=False,
            admin_user="admin", reason="test", batch_id="b1",
            log_changes=False, rollback_on_error=False, strict_mode=False,
        )
        assert result["skipped"] == 1

    @patch("user_manager.get_db")
    def test_email_validation_no_dot_in_domain(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._user_row()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.bulk_update_users(
            [{"username": "alice", "email": "a@nodot", "role": "user"}],
            dry_run=False, validate_email=True, send_notification=False,
            admin_user="admin", reason="test", batch_id="b1",
            log_changes=False, rollback_on_error=False, strict_mode=False,
        )
        assert result["skipped"] == 1

    @patch("user_manager.get_db")
    def test_invalid_role(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._user_row()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.bulk_update_users(
            [{"username": "alice", "email": "a@b.com", "role": "superuser"}],
            dry_run=False, validate_email=False, send_notification=False,
            admin_user="admin", reason="test", batch_id="b1",
            log_changes=False, rollback_on_error=False, strict_mode=False,
        )
        assert result["skipped"] == 1


class TestGenerateUserAnalytics:
    @patch("user_manager.get_db")
    def test_basic_analytics(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "alice", "login", "dashboard", "2024-01-01T10:00:00"),
            (2, "alice", "view", "orders", "2024-01-01T11:00:00"),
            (3, "bob", "login", "dashboard", "2024-01-01T12:00:00"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.generate_user_analytics(
            "2024-01-01", "2024-12-31", "day", ["actions"],
            include_inactive=True, min_activity=0, output_format="json",
            timezone="UTC", sampling_rate=1.0, anonymize=False,
        )
        assert result["total_actions"] == 3
        assert result["unique_users"] == 2
        assert result["active_users"] == 2

    @patch("user_manager.get_db")
    def test_filter_inactive(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "alice", "login", "dashboard", "2024-01-01T10:00:00"),
            (2, "alice", "view", "orders", "2024-01-01T11:00:00"),
            (3, "bob", "login", "dashboard", "2024-01-01T12:00:00"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.generate_user_analytics(
            "2024-01-01", "2024-12-31", "day", ["actions"],
            include_inactive=False, min_activity=2, output_format="json",
            timezone="UTC", sampling_rate=1.0, anonymize=False,
        )
        assert result["active_users"] == 1

    @patch("user_manager.get_db")
    def test_anonymize(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "alice", "login", "dashboard", "2024-01-01T10:00:00"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.generate_user_analytics(
            "2024-01-01", "2024-12-31", "day", ["actions"],
            include_inactive=True, min_activity=0, output_format="json",
            timezone="UTC", sampling_rate=1.0, anonymize=True,
        )
        assert result["top_users"][0]["user"] == "User_1"

    @patch("user_manager.get_db")
    def test_empty_data(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = user_manager.generate_user_analytics(
            "2024-01-01", "2024-12-31", "day", ["actions"],
            include_inactive=True, min_activity=0, output_format="json",
            timezone="UTC", sampling_rate=1.0, anonymize=False,
        )
        assert result["total_actions"] == 0
        assert result["active_users"] == 0
