import pytest
from unittest.mock import patch, MagicMock, mock_open
import admin_panel


class TestProcessOrderBatch:
    def test_empty_batch(self):
        assert admin_panel.process_order_batch([]) == []

    def test_single_order_free_tier(self):
        orders = [{"id": 1, "customer": " alice ", "amount": 50.0, "status": "new"}]
        result = admin_panel.process_order_batch(orders)
        assert len(result) == 1
        assert result[0]["customer"] == "ALICE"
        assert result[0]["amount"] == round(50.0 * 1.15, 2)
        assert result[0]["tier"] == "free"

    def test_basic_tier(self):
        orders = [{"id": 1, "customer": "bob", "amount": 100.0, "status": "new"}]
        result = admin_panel.process_order_batch(orders)
        assert result[0]["tier"] == "basic"

    def test_standard_tier(self):
        orders = [{"id": 1, "customer": "bob", "amount": 500.0, "status": "new"}]
        result = admin_panel.process_order_batch(orders)
        assert result[0]["tier"] == "standard"

    def test_premium_tier(self):
        orders = [{"id": 1, "customer": "bob", "amount": 1000.0, "status": "new"}]
        result = admin_panel.process_order_batch(orders)
        assert result[0]["tier"] == "premium"

    def test_multiple_orders(self):
        orders = [
            {"id": 1, "customer": "a", "amount": 10.0, "status": "new"},
            {"id": 2, "customer": "b", "amount": 2000.0, "status": "done"},
        ]
        result = admin_panel.process_order_batch(orders)
        assert len(result) == 2
        assert result[0]["tier"] == "free"
        assert result[1]["tier"] == "premium"

    def test_preserves_id_and_status(self):
        orders = [{"id": 42, "customer": "x", "amount": 10.0, "status": "shipped"}]
        result = admin_panel.process_order_batch(orders)
        assert result[0]["id"] == 42
        assert result[0]["status"] == "shipped"


class TestProcessRefundBatch:
    def test_empty_batch(self):
        assert admin_panel.process_refund_batch([]) == []

    def test_processes_refund(self):
        orders = [{"id": 1, "customer": " test ", "amount": 200.0, "status": "refund"}]
        result = admin_panel.process_refund_batch(orders)
        assert result[0]["customer"] == "TEST"
        assert result[0]["amount"] == round(200.0 * 1.15, 2)
        assert result[0]["tier"] == "basic"


class TestProcessExchangeBatch:
    def test_empty_batch(self):
        assert admin_panel.process_exchange_batch([]) == []

    def test_processes_exchange(self):
        orders = [{"id": 1, "customer": "user", "amount": 600.0, "status": "exchange"}]
        result = admin_panel.process_exchange_batch(orders)
        assert result[0]["tier"] == "standard"


class TestSearchOrders:
    @patch("admin_panel.get_db_connection")
    def test_search_orders(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "user1", 100)]
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.search_orders("user1")
        assert result == [(1, "user1", 100)]
        mock_cursor.execute.assert_called_once()
        mock_conn.return_value.close.assert_called_once()


class TestSearchProducts:
    @patch("admin_panel.get_db_connection")
    def test_search_products(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "Widget", "toys")]
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.search_products("Widget")
        assert result == [(1, "Widget", "toys")]


class TestGetDashboardStats:
    @patch("admin_panel.get_db_connection")
    def test_dashboard_stats(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [(10,), (5000.0,), (50,)]
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.get_dashboard_stats()
        assert result["total_orders"] == 10
        assert result["total_revenue"] == 5000.0
        assert result["total_users"] == 50
        assert "generated_at" in result

    @patch("admin_panel.get_db_connection")
    def test_dashboard_stats_empty_db(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [(None,), (None,), (None,)]
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.get_dashboard_stats()
        assert result["total_orders"] == 0
        assert result["total_revenue"] == 0
        assert result["total_users"] == 0


class TestRunAdminCommand:
    @patch("subprocess.Popen")
    def test_run_command(self, mock_popen):
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (b"output", b"")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc
        result = admin_panel.run_admin_command("echo hello")
        assert result["stdout"] == "output"
        assert result["stderr"] == ""
        assert result["returncode"] == 0


class TestLoadPlugin:
    @patch("builtins.open", mock_open(read_data='{"name": "plugin"}'))
    def test_load_plugin(self):
        result = admin_panel.load_plugin("/some/path")
        assert result == {"name": "plugin"}


class TestGetServerStatus:
    @patch("subprocess.Popen")
    def test_server_status(self, mock_popen):
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (b"up 5 days", None)
        mock_popen.return_value = mock_proc
        result = admin_panel.get_server_status()
        assert result == "up 5 days"


class TestGenerateOrderExport:
    @patch("admin_panel.get_db_connection")
    def test_export_orders(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "u1", 100), (2, "u2", 200)]
        mock_conn.return_value.cursor.return_value = mock_cursor
        m = mock_open()
        with patch("builtins.open", m):
            count = admin_panel.generate_order_export("/tmp/out.csv", "2024-01-01", "2024-12-31")
        assert count == 2


class TestGenerateUserExport:
    @patch("admin_panel.get_db_connection")
    def test_export_users(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "admin", "a@b.com")]
        mock_conn.return_value.cursor.return_value = mock_cursor
        m = mock_open()
        with patch("builtins.open", m):
            count = admin_panel.generate_user_export("/tmp/out.csv", "admin")
        assert count == 1


class TestPurgeOldRecords:
    @patch("admin_panel.get_db_connection")
    def test_purge(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.purge_old_records("orders", 30)
        assert result == 5
        mock_conn.return_value.commit.assert_called_once()


class TestReadLogFile:
    @patch("admin_panel.Path.read_text")
    def test_read_log(self, mock_read_text):
        mock_read_text.return_value = "log content"
        result = admin_panel.read_log_file("app.log")
        assert result == "log content"


class TestTailLogFile:
    @patch("admin_panel.Path.read_text")
    def test_tail_log(self, mock_read_text):
        mock_read_text.return_value = "first\nsecond\nlast lines"
        result = admin_panel.tail_log_file("app.log", lines=50)
        assert result == "first\nsecond\nlast lines"


class TestGetDbConnection:
    @patch("admin_panel.sqlite3")
    def test_returns_connection(self, mock_sqlite3):
        mock_conn = MagicMock()
        mock_sqlite3.connect.return_value = mock_conn
        result = admin_panel.get_db_connection()
        mock_sqlite3.connect.assert_called_once_with("ecommerce.db")
        assert result is mock_conn


class TestProcessRefundBatchTiers:
    def test_premium_tier(self):
        orders = [{"id": 1, "customer": "a", "amount": 1000.0, "status": "refund"}]
        result = admin_panel.process_refund_batch(orders)
        assert result[0]["tier"] == "premium"

    def test_standard_tier(self):
        orders = [{"id": 1, "customer": "a", "amount": 500.0, "status": "refund"}]
        result = admin_panel.process_refund_batch(orders)
        assert result[0]["tier"] == "standard"

    def test_free_tier(self):
        orders = [{"id": 1, "customer": "a", "amount": 50.0, "status": "refund"}]
        result = admin_panel.process_refund_batch(orders)
        assert result[0]["tier"] == "free"


class TestProcessExchangeBatchTiers:
    def test_premium_tier(self):
        orders = [{"id": 1, "customer": "a", "amount": 1000.0, "status": "exchange"}]
        result = admin_panel.process_exchange_batch(orders)
        assert result[0]["tier"] == "premium"

    def test_basic_tier(self):
        orders = [{"id": 1, "customer": "a", "amount": 200.0, "status": "exchange"}]
        result = admin_panel.process_exchange_batch(orders)
        assert result[0]["tier"] == "basic"

    def test_free_tier(self):
        orders = [{"id": 1, "customer": "a", "amount": 50.0, "status": "exchange"}]
        result = admin_panel.process_exchange_batch(orders)
        assert result[0]["tier"] == "free"


class TestAuditAdminActions:
    @patch("admin_panel.get_db_connection")
    def test_with_all_filters(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "admin1", "delete", "users", "2024-06-01T12:00:00"),
            (2, "admin1", "update", "orders", "2024-06-01T11:00:00"),
            (3, "admin1", "login", "system", "2024-06-01T10:00:00"),
        ]
        mock_cursor.fetchone.return_value = (3,)
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.audit_admin_actions(
            admin_username="admin1",
            start_date="2024-01-01",
            end_date="2024-12-31",
            action_filter="delete",
            resource_filter="users",
            severity_filter="high",
            include_system=True,
            page_size=10,
            page_number=0,
            export_format="csv",
        )
        assert result["admin"] == "admin1"
        assert result["total_count"] == 3
        assert result["page"] == 0
        assert result["page_size"] == 10
        assert len(result["actions"]) == 3
        assert result["actions"][0]["risk_level"] == "high"
        assert result["high_risk_count"] == 1
        assert result["actions"][1]["risk_level"] == "medium"
        assert result["actions"][2]["risk_level"] == "low"

    @patch("admin_panel.get_db_connection")
    def test_without_optional_filters(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = (0,)
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.audit_admin_actions(
            admin_username="admin1",
            start_date=None,
            end_date=None,
            action_filter=None,
            resource_filter=None,
            severity_filter=None,
            include_system=False,
            page_size=20,
            page_number=1,
            export_format=None,
        )
        assert result["actions"] == []
        assert result["total_count"] == 0
        assert result["high_risk_count"] == 0

    @patch("admin_panel.get_db_connection")
    def test_high_risk_actions(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "admin1", "purge", "logs", "2024-06-01T12:00:00"),
            (2, "admin1", "modify_permissions", "roles", "2024-06-01T11:00:00"),
            (3, "admin1", "export_data", "db", "2024-06-01T10:00:00"),
        ]
        mock_cursor.fetchone.return_value = (3,)
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.audit_admin_actions(
            admin_username="admin1",
            start_date=None,
            end_date=None,
            action_filter=None,
            resource_filter=None,
            severity_filter=None,
            include_system=True,
            page_size=10,
            page_number=0,
            export_format=None,
        )
        assert result["high_risk_count"] == 3
        for action in result["actions"]:
            assert action["risk_level"] == "high"


class TestManageAdminRoles:
    @patch("admin_panel.get_db_connection")
    def test_user_not_found(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.manage_admin_roles(
            target_username="ghost", new_role="admin", granted_by="root",
            reason="test", effective_date="2024-01-01", expiry_date="2025-01-01",
            notify_user=False, require_mfa=False, ip_whitelist=None, audit_trail=False,
        )
        assert result["status"] == "error"
        assert "not found" in result["message"]
        mock_conn.return_value.close.assert_called_once()

    @patch("admin_panel.get_db_connection")
    def test_role_already_assigned(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, "alice", "a@b.com", "hash", "admin")
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.manage_admin_roles(
            target_username="alice", new_role="admin", granted_by="root",
            reason="test", effective_date="2024-01-01", expiry_date="2025-01-01",
            notify_user=False, require_mfa=False, ip_whitelist=None, audit_trail=False,
        )
        assert result["status"] == "no_change"
        mock_conn.return_value.close.assert_called_once()

    @patch("admin_panel.get_db_connection")
    def test_invalid_role(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, "alice", "a@b.com", "hash", "viewer")
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.manage_admin_roles(
            target_username="alice", new_role="dictator", granted_by="root",
            reason="test", effective_date="2024-01-01", expiry_date="2025-01-01",
            notify_user=False, require_mfa=False, ip_whitelist=None, audit_trail=False,
        )
        assert result["status"] == "error"
        assert "Invalid role" in result["message"]

    @patch("admin_panel.get_db_connection")
    def test_super_admin_requires_admin(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, "alice", "a@b.com", "hash", "viewer")
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.manage_admin_roles(
            target_username="alice", new_role="super_admin", granted_by="root",
            reason="test", effective_date="2024-01-01", expiry_date="2025-01-01",
            notify_user=False, require_mfa=False, ip_whitelist=None, audit_trail=False,
        )
        assert result["status"] == "error"
        assert "Can only promote admins" in result["message"]

    @patch("admin_panel.get_db_connection")
    def test_successful_role_change_with_audit(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, "alice", "a@b.com", "hash", "viewer")
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.manage_admin_roles(
            target_username="alice", new_role="manager", granted_by="root",
            reason="promotion", effective_date="2024-01-01", expiry_date="2025-01-01",
            notify_user=True, require_mfa=True, ip_whitelist=["10.0.0.1"], audit_trail=True,
        )
        assert result["status"] == "success"
        assert result["old_role"] == "viewer"
        assert result["new_role"] == "manager"
        assert result["granted_by"] == "root"
        assert mock_cursor.execute.call_count == 3  # SELECT + UPDATE + INSERT audit
        mock_conn.return_value.commit.assert_called_once()

    @patch("admin_panel.get_db_connection")
    def test_successful_role_change_without_audit(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, "alice", "a@b.com", "hash", "viewer")
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.manage_admin_roles(
            target_username="alice", new_role="moderator", granted_by="root",
            reason="test", effective_date="2024-01-01", expiry_date="2025-01-01",
            notify_user=False, require_mfa=False, ip_whitelist=None, audit_trail=False,
        )
        assert result["status"] == "success"
        assert mock_cursor.execute.call_count == 2  # SELECT + UPDATE only

    @patch("admin_panel.get_db_connection")
    def test_promote_admin_to_super_admin(self, mock_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, "alice", "a@b.com", "hash", "admin")
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = admin_panel.manage_admin_roles(
            target_username="alice", new_role="super_admin", granted_by="root",
            reason="test", effective_date="2024-01-01", expiry_date="2025-01-01",
            notify_user=False, require_mfa=False, ip_whitelist=None, audit_trail=False,
        )
        assert result["status"] == "success"
        assert result["new_role"] == "super_admin"
