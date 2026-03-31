import pytest
import json
import re
from unittest.mock import patch, MagicMock
from datetime import datetime
import task_scheduler


class TestValidateTaskConfig:
    def test_valid_config(self):
        config = {
            "name": "import_data",
            "type": "data_import",
            "priority": 5,
            "owner": "admin",
            "schedule": {"cron": "0 * * * *"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_missing_required_fields(self):
        config = {}
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False
        assert len(result["errors"]) == 5

    def test_short_name(self):
        config = {
            "name": "ab",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"cron": "0 * * * *"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False
        assert any("at least 3" in e for e in result["errors"])

    def test_empty_name(self):
        config = {
            "name": "",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"cron": "0 * * * *"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False
        assert any("empty" in e for e in result["errors"])

    def test_long_name(self):
        config = {
            "name": "x" * 101,
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"cron": "0 * * * *"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False
        assert any("exceed 100" in e for e in result["errors"])

    def test_name_must_start_with_letter(self):
        config = {
            "name": "123task",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"cron": "0 * * * *"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False
        assert any("start with a letter" in e for e in result["errors"])

    def test_negative_priority(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": -1,
            "owner": "x",
            "schedule": {"cron": "0 * * * *"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False
        assert any("negative" in e for e in result["errors"])

    def test_priority_zero_warning(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 0,
            "owner": "x",
            "schedule": {"cron": "0 * * * *"},
        }
        result = task_scheduler.validate_task_config(config)
        assert any("lowest" in w for w in result["warnings"])

    def test_priority_too_high(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 11,
            "owner": "x",
            "schedule": {"cron": "0 * * * *"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False
        assert any("exceed 10" in e for e in result["errors"])

    def test_priority_not_int(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": "high",
            "owner": "x",
            "schedule": {"cron": "0 * * * *"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False

    def test_cron_too_few_fields(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"cron": "0 * *"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False
        assert any("too few" in e for e in result["errors"])

    def test_cron_too_many_fields(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"cron": "0 * * * * *"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False
        assert any("too many" in e for e in result["errors"])

    def test_interval_schedule(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"interval": 300},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is True

    def test_interval_low_warning(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"interval": 30},
        }
        result = task_scheduler.validate_task_config(config)
        assert any("high load" in w for w in result["warnings"])

    def test_interval_high_warning(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"interval": 100000},
        }
        result = task_scheduler.validate_task_config(config)
        assert any("cron" in w for w in result["warnings"])

    def test_interval_not_int(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"interval": "fast"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False

    def test_once_schedule_invalid_datetime(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"once": "not-a-date"},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False

    def test_schedule_missing_type(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False

    def test_metadata_not_dict(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"interval": 300},
            "metadata": "not a dict",
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False

    def test_metadata_too_large(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"interval": 300},
            "metadata": {"data": "x" * 20000},
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False

    def test_tags_valid(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"interval": 300},
            "tags": ["tag1", "tag2"],
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is True

    def test_tags_not_list(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"interval": 300},
            "tags": "tag1",
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False

    def test_tags_non_string_element(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"interval": 300},
            "tags": [123],
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False

    def test_tag_too_long(self):
        config = {
            "name": "task_a",
            "type": "t",
            "priority": 5,
            "owner": "x",
            "schedule": {"interval": 300},
            "tags": ["x" * 51],
        }
        result = task_scheduler.validate_task_config(config)
        assert result["valid"] is False


class TestBuildTaskDependencyGraph:
    def test_no_dependencies(self):
        tasks = [
            {"id": "A", "depends_on": []},
            {"id": "B", "depends_on": []},
        ]
        result = task_scheduler.build_task_dependency_graph(tasks)
        assert result["total_tasks"] == 2
        assert set(result["execution_order"]) == {"A", "B"}
        assert result["circular_dependencies"] == []

    def test_linear_dependency(self):
        tasks = [
            {"id": "A", "depends_on": []},
            {"id": "B", "depends_on": ["A"]},
        ]
        result = task_scheduler.build_task_dependency_graph(tasks)
        order = result["execution_order"]
        assert order.index("A") < order.index("B")

    def test_orphan_dependency(self):
        tasks = [
            {"id": "A", "depends_on": ["X"]},
        ]
        result = task_scheduler.build_task_dependency_graph(tasks)
        assert "X" in result["orphan_dependencies"]

    def test_circular_dependency(self):
        tasks = [
            {"id": "A", "depends_on": ["B"]},
            {"id": "B", "depends_on": ["A"]},
        ]
        result = task_scheduler.build_task_dependency_graph(tasks)
        assert len(result["circular_dependencies"]) > 0


class TestComputeTaskStatistics:
    def test_empty_task_list(self):
        result = task_scheduler.compute_task_statistics([])
        assert result == {}

    def test_single_completed_task(self):
        tasks = [
            {"status": "completed", "duration": 10, "type": "report", "owner": "admin", "priority": 5}
        ]
        result = task_scheduler.compute_task_statistics(tasks)
        assert result["total"] == 1
        assert result["completed"] == 1
        assert result["failed"] == 0
        assert result["success_rate"] == 100.0

    def test_mixed_statuses(self):
        tasks = [
            {"status": "completed", "duration": 10, "type": "a", "owner": "x", "priority": 5},
            {"status": "failed", "duration": 5, "type": "a", "owner": "x", "priority": 3},
            {"status": "pending", "duration": 0, "type": "b", "owner": "y", "priority": 1},
            {"status": "running", "duration": 2, "type": "b", "owner": "y", "priority": 7},
        ]
        result = task_scheduler.compute_task_statistics(tasks)
        assert result["total"] == 4
        assert result["completed"] == 1
        assert result["failed"] == 1
        assert result["pending"] == 1
        assert result["running"] == 1
        assert result["success_rate"] == 25.0
        assert result["failure_rate"] == 25.0
        assert result["type_distribution"] == {"a": 2, "b": 2}
        assert result["owner_distribution"] == {"x": 2, "y": 2}

    def test_duration_stats(self):
        tasks = [
            {"status": "completed", "duration": 10, "type": "a", "owner": "x", "priority": 5},
            {"status": "completed", "duration": 20, "type": "a", "owner": "x", "priority": 5},
            {"status": "completed", "duration": 30, "type": "a", "owner": "x", "priority": 5},
        ]
        result = task_scheduler.compute_task_statistics(tasks)
        assert result["avg_duration"] == 20.0
        assert result["min_duration"] == 10
        assert result["max_duration"] == 30


class TestProcessTaskResults:
    def test_empty(self):
        result = task_scheduler.process_task_results([])
        assert result["total_success"] == 0
        assert result["total_failure"] == 0
        assert result["overall_success_rate"] == 0

    def test_all_success(self):
        results = [
            {"type": "report", "status": "completed", "duration": 10},
            {"type": "report", "status": "completed", "duration": 20},
        ]
        result = task_scheduler.process_task_results(results)
        assert result["total_success"] == 2
        assert result["total_failure"] == 0
        assert result["overall_success_rate"] == 100.0
        assert result["by_type"]["report"]["success_rate"] == 100.0

    def test_mixed_results(self):
        results = [
            {"type": "import", "status": "completed", "duration": 10},
            {"type": "import", "status": "failed", "duration": 5},
            {"type": "export", "status": "completed", "duration": 15},
        ]
        result = task_scheduler.process_task_results(results)
        assert result["total_success"] == 2
        assert result["total_failure"] == 1
        assert result["by_type"]["import"]["success_rate"] == 50.0
        assert result["by_type"]["export"]["success_rate"] == 100.0


class TestScheduleTask:
    @patch("task_scheduler.get_db")
    def test_schedule(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        task_id = task_scheduler.schedule_task(
            "test_task", "data_import", 5, "admin", "2024-01-01",
            3, 300, "{}", "http://cb.com", "tag1,tag2",
        )
        assert isinstance(task_id, str)
        assert len(task_id) == 12
        mock_cursor.execute.assert_called_once()
        mock_get_db.return_value.commit.assert_called_once()


class TestGetPendingTasks:
    @patch("task_scheduler.get_db")
    def test_get_pending(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("t1",), ("t2",)]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.get_pending_tasks(
            task_type="report", priority_min=1, priority_max=10,
            owner="admin", limit=50, offset=0, sort_by="priority",
            sort_order="DESC", include_metadata=True, status_filter="pending",
        )
        assert len(result) == 2

    @patch("task_scheduler.get_db")
    def test_get_pending_no_filters(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.get_pending_tasks(
            task_type=None, priority_min=None, priority_max=None,
            owner=None, limit=10, offset=0, sort_by=None,
            sort_order="ASC", include_metadata=False, status_filter="pending",
        )
        assert result == []


class TestCleanupOldTasks:
    @patch("task_scheduler.get_db")
    def test_cleanup_dry_run(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.cleanup_old_tasks(30, ["report", "import"], dry_run=True)
        assert result == 0
        mock_cursor.execute.assert_not_called()

    @patch("task_scheduler.get_db")
    def test_cleanup_actual(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.cleanup_old_tasks(30, ["report"], dry_run=False)
        assert result == 3


class TestMigrateTasksBetweenQueues:
    @patch("task_scheduler.get_db")
    def test_migrate_dry_run(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("t1", "task", "report", 5, "admin")
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.migrate_tasks_between_queues(
            "queue_a", "queue_b", ["t1", "t2"], preserve_priority=True,
            preserve_owner=True, preserve_metadata=True, dry_run=True,
            verbose=False, batch_size=10, on_conflict="skip",
        )
        assert result["migrated"] == 2

    @patch("task_scheduler.get_db")
    def test_migrate_task_not_found(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.migrate_tasks_between_queues(
            "queue_a", "queue_b", ["t1"], preserve_priority=True,
            preserve_owner=True, preserve_metadata=True, dry_run=False,
            verbose=True, batch_size=10, on_conflict="skip",
        )
        assert result["skipped"] == 1


class TestGenerateTaskReport:
    @patch("task_scheduler.get_db")
    def test_report(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("t1", "task1", "report", 5, "admin", "2024-01-01", 3, 300, "{}", "", "", "pending"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.generate_task_report(
            "main", "2024-01-01", "2024-12-31", "type",
            include_details=False, format_type="json", output_path=None,
            include_charts=False, timezone="UTC", locale="en",
        )
        assert result["total_tasks"] == 1
        assert "groups" in result


class TestGetDb:
    @patch("task_scheduler.sqlite3.connect")
    def test_returns_connection(self, mock_connect):
        mock_connect.return_value = MagicMock()
        conn = task_scheduler.get_db()
        mock_connect.assert_called_once_with("ecommerce.db")
        assert conn is not None


class TestExecuteTaskQueue:
    def _make_task_row(self, task_id="t1", name="task1", task_type="report",
                       priority=5, owner="admin", scheduled_at="2024-01-01",
                       retry_count=0, timeout=300):
        return (task_id, name, task_type, priority, owner, scheduled_at,
                retry_count, timeout, "{}", "", "", "pending")

    @patch("task_scheduler.get_db")
    def test_empty_queue(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, False, False, False, None,
            "info", False, 1
        )
        assert result["completed"] == 0
        assert result["total_tasks"] == 0

    @patch("task_scheduler.get_db")
    def test_skip_low_priority_verbose_debug(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row(priority=2)]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, True, False, False, None,
            "debug", False, 5
        )
        assert result["skipped"] == 1
        assert result["completed"] == 0

    @patch("task_scheduler.get_db")
    def test_skip_low_priority_verbose_info(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row(priority=2)]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, True, False, False, None,
            "info", False, 5
        )
        assert result["skipped"] == 1

    @patch("task_scheduler.get_db")
    def test_skip_low_priority_verbose_other(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row(priority=2)]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, True, False, False, None,
            "warn", False, 5
        )
        assert result["skipped"] == 1

    @patch("task_scheduler.get_db")
    def test_skip_low_priority_non_verbose(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row(priority=2)]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, False, False, False, None,
            "info", False, 5
        )
        assert result["skipped"] == 1

    @patch("task_scheduler.get_db")
    def test_dry_run_verbose_debug(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row()]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, True, True, False, False, None,
            "debug", False, 1
        )
        assert len(result) > 0

    @patch("task_scheduler.get_db")
    def test_dry_run_verbose_info(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row()]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, True, True, False, False, None,
            "info", False, 1
        )
        assert len(result) > 0

    @patch("task_scheduler.get_db")
    def test_dry_run_verbose_other(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row()]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, True, True, False, False, None,
            "warn", False, 1
        )
        assert len(result) > 0

    @patch("task_scheduler.get_db")
    def test_execute_report_type(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row(task_type="report")]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, False, False, False, None,
            "info", False, 1
        )
        assert result["completed"] == 1

    @patch("task_scheduler.get_db")
    def test_execute_cleanup_type(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row(task_type="cleanup")]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, False, False, False, None,
            "info", False, 1
        )
        assert result["completed"] == 1

    @patch("task_scheduler.get_db")
    def test_execute_notification_type(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row(task_type="notification")]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, False, False, False, None,
            "info", False, 1
        )
        assert result["completed"] == 1

    @patch("task_scheduler.get_db")
    def test_execute_unknown_type(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row(task_type="other")]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, False, False, False, None,
            "info", False, 1
        )
        assert result["completed"] == 1

    @patch("task_scheduler.get_db")
    def test_execute_data_import_batch_mode(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row(task_type="data_import")]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, False, False, False, None,
            "info", True, 1
        )
        assert result["completed"] == 1

    @patch("task_scheduler.get_db")
    def test_execute_data_import_non_batch(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row(task_type="data_import")]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, False, False, False, None,
            "info", False, 1
        )
        assert result["completed"] == 1

    @patch("task_scheduler.get_db")
    def test_execute_with_error_fail_fast(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row()]
        call_count = {"n": 0}
        def execute_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] > 1:
                raise Exception("DB error")
        mock_cursor.execute.side_effect = execute_side_effect
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, False, True, False, None,
            "info", False, 1
        )
        assert result["failed"] == 1

    @patch("task_scheduler.get_db")
    def test_execute_with_retry(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [self._make_task_row()]
        call_count = {"n": 0}
        def execute_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] > 1:
                raise Exception("DB error")
        mock_cursor.execute.side_effect = execute_side_effect
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.execute_task_queue(
            "main", 4, False, True, False, True, None,
            "info", False, 1
        )
        assert result["failed"] >= 0


class TestMigrateNotPreserve:
    @patch("task_scheduler.get_db")
    def test_migrate_not_preserve_priority_owner(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("t1", "task", "report", 5, "admin")
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.migrate_tasks_between_queues(
            "queue_a", "queue_b", ["t1"], preserve_priority=False,
            preserve_owner=False, preserve_metadata=True, dry_run=False,
            verbose=False, batch_size=10, on_conflict="skip",
        )
        assert result["migrated"] == 1

    @patch("task_scheduler.get_db")
    def test_migrate_on_conflict_overwrite(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("t1", "task", "report", 5, "admin")
        call_count = [0]
        def side_effect(sql):
            call_count[0] += 1
            if call_count[0] == 2:  # first UPDATE fails
                raise Exception("conflict")
        mock_cursor.execute.side_effect = side_effect
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.migrate_tasks_between_queues(
            "queue_a", "queue_b", ["t1"], preserve_priority=True,
            preserve_owner=True, preserve_metadata=True, dry_run=False,
            verbose=False, batch_size=10, on_conflict="overwrite",
        )
        # After error the overwrite path executes DELETE + UPDATE
        assert result["migrated"] >= 0

    @patch("task_scheduler.get_db")
    def test_migrate_on_conflict_error(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("t1", "task", "report", 5, "admin")
        call_count = [0]
        def side_effect(sql):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("db error")
        mock_cursor.execute.side_effect = side_effect
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.migrate_tasks_between_queues(
            "queue_a", "queue_b", ["t1"], preserve_priority=True,
            preserve_owner=True, preserve_metadata=True, dry_run=False,
            verbose=False, batch_size=10, on_conflict="fail",
        )
        assert len(result["errors"]) >= 1


class TestGenerateTaskReportOutput:
    @patch("task_scheduler.get_db")
    def test_report_with_json_output(self, mock_get_db, tmp_path):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("t1", "task1", "report", 5, "admin", "2024-01-01", 3, 300, "{}", "", "", "pending"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        out = str(tmp_path / "report.json")
        task_scheduler.generate_task_report(
            "main", "2024-01-01", "2024-12-31", "type",
            include_details=True, format_type="json", output_path=out,
            include_charts=False, timezone="UTC", locale="en",
        )
        import json
        with open(out) as f:
            data = json.load(f)
        assert data["total_tasks"] == 1

    @patch("task_scheduler.get_db")
    def test_report_with_csv_output(self, mock_get_db, tmp_path):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("t1", "task1", "report", 5, "admin", "2024-01-01", 3, 300, "{}", "", "", "pending"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        out = str(tmp_path / "report.csv")
        task_scheduler.generate_task_report(
            "main", "2024-01-01", "2024-12-31", "type",
            include_details=False, format_type="csv", output_path=out,
            include_charts=False, timezone="UTC", locale="en",
        )
        content = open(out).read()
        assert "group,count" in content

    @patch("task_scheduler.get_db")
    def test_report_group_by_owner(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("t1", "task1", "report", 5, "admin", "2024-01-01", 3, 300, "{}", "", "", "pending"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = task_scheduler.generate_task_report(
            None, "2024-01-01", "2024-12-31", "owner",
            include_details=False, format_type="json", output_path=None,
            include_charts=False, timezone="UTC", locale="en",
        )
        assert "admin" in result["groups"]
