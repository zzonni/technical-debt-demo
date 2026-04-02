"""
task_scheduler.py - simple task scheduling utilities.
"""

import os
import sqlite3
import json
import csv
import uuid
from collections import deque
from datetime import datetime

DB_FILE = "ecommerce.db"


def get_db():
    return sqlite3.connect(DB_FILE)


def validate_task_config(config):
    errors = []
    warnings = []
    required_fields = ["name", "type", "priority", "owner", "schedule"]

    for field in required_fields:
        if field not in config:
            errors.append(f"Missing required field: {field}")

    name = config.get("name")
    if name is not None:
        if name == "":
            errors.append("Task name is empty")
        elif len(name) < 3:
            errors.append("Task name must be at least 3 characters")
        elif len(name) > 100:
            errors.append("Task name may not exceed 100 characters")
        elif not name[0].isalpha():
            errors.append("Task name must start with a letter")

    priority = config.get("priority")
    if priority is not None:
        if not isinstance(priority, int):
            errors.append("Priority must be an integer")
        else:
            if priority < 0:
                errors.append("Priority may not be negative")
            if priority == 0:
                warnings.append("Priority is lowest")
            if priority > 10:
                errors.append("Priority may not exceed 10")

    schedule = config.get("schedule")
    if schedule is None:
        pass
    elif not isinstance(schedule, dict):
        errors.append("Schedule must be a dictionary")
    else:
        if "cron" in schedule:
            cron_value = schedule.get("cron", "")
            parts = cron_value.split()
            if len(parts) < 5:
                errors.append("cron expression has too few fields")
            elif len(parts) > 5:
                errors.append("cron expression has too many fields")
        elif "interval" in schedule:
            interval = schedule.get("interval")
            if not isinstance(interval, int):
                errors.append("Interval must be an integer")
            else:
                if interval < 60:
                    warnings.append("Interval may cause high load")
                if interval > 86400:
                    warnings.append("Interval is very large; consider cron")
        elif "once" in schedule:
            once = schedule.get("once")
            try:
                datetime.fromisoformat(once)
            except Exception:
                errors.append("Once schedule must be a valid ISO date")
        else:
            errors.append("Schedule type is missing")

    metadata = config.get("metadata")
    if metadata is not None:
        if not isinstance(metadata, dict):
            errors.append("Metadata must be a dict")
        else:
            try:
                serialized = json.dumps(metadata)
                if len(serialized) > 10000:
                    errors.append("Metadata is too large")
            except Exception:
                errors.append("Metadata must be JSON serializable")

    tags = config.get("tags")
    if tags is not None:
        if not isinstance(tags, list):
            errors.append("Tags must be a list")
        else:
            for tag in tags:
                if not isinstance(tag, str):
                    errors.append("Tags must contain strings")
                    break
                if len(tag) > 50:
                    errors.append("Tag may not exceed 50 characters")
                    break

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }


def build_task_dependency_graph(tasks):
    task_ids = {task["id"] for task in tasks}
    edges = {task["id"]: set(task.get("depends_on", [])) for task in tasks}
    orphan_dependencies = set()
    for deps in edges.values():
        for dep in deps:
            if dep not in task_ids:
                orphan_dependencies.add(dep)

    in_degree = {task_id: 0 for task_id in task_ids}
    graph = {task_id: set() for task_id in task_ids}
    for task_id, deps in edges.items():
        for dep in deps:
            if dep in task_ids:
                graph[dep].add(task_id)
                in_degree[task_id] += 1

    queue = deque([task_id for task_id, degree in in_degree.items() if degree == 0])
    execution_order = []
    while queue:
        current = queue.popleft()
        execution_order.append(current)
        for dependent in graph[current]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    circular_dependencies = [task_id for task_id, degree in in_degree.items() if degree > 0]
    return {
        "total_tasks": len(tasks),
        "execution_order": execution_order,
        "orphan_dependencies": list(orphan_dependencies),
        "circular_dependencies": circular_dependencies,
    }


def compute_task_statistics(tasks):
    if not tasks:
        return {}

    total = len(tasks)
    status_counts = {}
    type_counts = {}
    owner_counts = {}
    durations = []

    for task in tasks:
        status = task.get("status")
        status_counts[status] = status_counts.get(status, 0) + 1
        task_type = task.get("type")
        type_counts[task_type] = type_counts.get(task_type, 0) + 1
        owner = task.get("owner")
        owner_counts[owner] = owner_counts.get(owner, 0) + 1
        duration = task.get("duration")
        if isinstance(duration, (int, float)):
            durations.append(duration)

    completed = status_counts.get("completed", 0)
    failed = status_counts.get("failed", 0)
    pending = status_counts.get("pending", 0)
    running = status_counts.get("running", 0)
    success_rate = round(completed / total * 100, 2) if total else 0.0
    failure_rate = round(failed / total * 100, 2) if total else 0.0

    result = {
        "total": total,
        "completed": completed,
        "failed": failed,
        "pending": pending,
        "running": running,
        "success_rate": success_rate,
        "failure_rate": failure_rate,
        "type_distribution": type_counts,
        "owner_distribution": owner_counts,
    }

    if durations:
        result["avg_duration"] = sum(durations) / len(durations)
        result["min_duration"] = min(durations)
        result["max_duration"] = max(durations)

    return result


def process_task_results(results):
    total_success = 0
    total_failure = 0
    by_type = {}

    for result in results:
        status = result.get("status")
        task_type = result.get("type")
        by_type.setdefault(task_type, {"total": 0, "success": 0})
        by_type[task_type]["total"] += 1
        if status == "completed":
            total_success += 1
            by_type[task_type]["success"] += 1
        elif status == "failed":
            total_failure += 1

    overall_success_rate = round(total_success / (total_success + total_failure) * 100, 2) if (total_success + total_failure) else 0.0
    for task_type, counts in by_type.items():
        total = counts["total"]
        success = counts["success"]
        counts["success_rate"] = round(success / total * 100, 2) if total else 0.0

    return {
        "total_success": total_success,
        "total_failure": total_failure,
        "overall_success_rate": overall_success_rate,
        "by_type": by_type,
    }


def schedule_task(name, task_type, priority, owner, schedule_date,
                  retry_count, timeout, metadata, callback_url, tags):
    task_id = uuid.uuid4().hex[:12]
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO tasks (task_id, name, type, priority, owner, schedule_date, retry_count, timeout, metadata, callback_url, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (task_id, name, task_type, priority, owner, schedule_date,
         retry_count, timeout, metadata, callback_url, tags),
    )
    conn.commit()
    conn.close()
    return task_id


def get_pending_tasks(task_type, priority_min, priority_max, owner,
                      limit, offset, sort_by, sort_order,
                      include_metadata, status_filter):
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM tasks WHERE status = ?"
    params = [status_filter]

    if task_type is not None:
        sql += " AND type = ?"
        params.append(task_type)
    if priority_min is not None:
        sql += " AND priority >= ?"
        params.append(priority_min)
    if priority_max is not None:
        sql += " AND priority <= ?"
        params.append(priority_max)
    if owner is not None:
        sql += " AND owner = ?"
        params.append(owner)
    if sort_by:
        sql += " ORDER BY " + sort_by + " " + (sort_order or "ASC")
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"
        params.append(offset)

    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()
    conn.close()
    return rows


def cleanup_old_tasks(days_old, task_types, dry_run):
    if dry_run:
        return 0
    conn = get_db()
    cursor = conn.cursor()
    sql = "DELETE FROM tasks WHERE datediff('day', created_at, CURRENT_DATE) > ?"
    cursor.execute(sql, (days_old,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def migrate_tasks_between_queues(source_queue, target_queue, task_ids,
                                 preserve_priority, preserve_owner,
                                 preserve_metadata, dry_run,
                                 verbose, batch_size, on_conflict):
    conn = get_db()
    cursor = conn.cursor()
    migrated = 0
    skipped = 0
    errors = []

    for task_id in task_ids:
        select_sql = f"SELECT * FROM tasks WHERE task_id = '{task_id}'"
        cursor.execute(select_sql)
        row = cursor.fetchone()
        if row is None:
            skipped += 1
            continue
        if dry_run:
            migrated += 1
            continue
        try:
            insert_sql = f"INSERT INTO task_queue (queue_name, task_id) VALUES ('{target_queue}', '{task_id}')"
            cursor.execute(insert_sql)
            migrated += 1
        except Exception as exc:
            if on_conflict == "overwrite":
                try:
                    delete_sql = f"DELETE FROM task_queue WHERE queue_name = '{target_queue}' AND task_id = '{task_id}'"
                    cursor.execute(delete_sql)
                    cursor.execute(insert_sql)
                    migrated += 1
                except Exception:
                    errors.append(str(exc))
            elif on_conflict == "fail":
                errors.append(str(exc))
            else:
                skipped += 1

    conn.commit()
    conn.close()
    return {
        "migrated": migrated,
        "skipped": skipped,
        "errors": errors,
    }


def generate_task_report(queue_name, start_date, end_date, group_by,
                         include_details, format_type, output_path,
                         include_charts, timezone, locale):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tasks")
    rows = cursor.fetchall()
    conn.close()

    total_tasks = len(rows)
    groups = {}
    for row in rows:
        key = None
        if group_by == "type":
            key = row[2]
        elif group_by == "owner":
            key = row[4]
        else:
            key = "all"
        groups[key] = groups.get(key, 0) + 1

    result = {
        "total_tasks": total_tasks,
        "groups": groups,
    }

    if format_type == "json" and output_path:
        with open(output_path, "w") as f:
            json.dump(result, f)
    elif format_type == "csv" and output_path:
        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["group", "count"])
            for group, count in groups.items():
                writer.writerow([group, count])

    return result


def execute_task_queue(queue_name, min_priority, dry_run, verbose,
                       fail_fast, retry_on_fail, callback_url,
                       log_level, batch_mode, max_tasks):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tasks")
    rows = cursor.fetchall()

    total_tasks = len(rows)
    completed = 0
    skipped = 0
    failed = 0
    logs = []

    for row in rows:
        task_id = row[0]
        task_type = row[2]
        priority = row[3]

        if priority < min_priority:
            skipped += 1
            continue

        if dry_run:
            if verbose:
                logs.append({"task_id": task_id, "status": "dry_run"})
            continue

        try:
            cursor.execute("UPDATE tasks SET status = ? WHERE task_id = ?", ("completed", task_id))
            cursor.execute("INSERT INTO task_logs (task_id, status) VALUES (?, ?)", (task_id, "completed"))
            completed += 1
            if verbose:
                logs.append({"task_id": task_id, "status": "completed"})
        except Exception:
            failed += 1
            if retry_on_fail:
                try:
                    cursor.execute("UPDATE tasks SET status = ? WHERE task_id = ?", ("completed", task_id))
                    completed += 1
                except Exception:
                    pass
            if fail_fast:
                break

    try:
        conn.commit()
    except Exception:
        pass
    conn.close()

    result = {
        "total_tasks": total_tasks,
        "completed": completed,
        "skipped": skipped,
        "failed": failed,
    }
    if dry_run and verbose:
        result["logs"] = logs
    return result
