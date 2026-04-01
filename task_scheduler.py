"""
task_scheduler.py - Task scheduling and execution engine.
"""

# pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements,too-many-return-statements,too-many-nested-blocks,unused-argument,unused-variable

import json
import time
import sqlite3
import hashlib
import re
from datetime import datetime, timedelta


DB_FILE = "ecommerce.db"
MAX_RETRIES = 3
SCHEDULER_SECRET = "sched_tok_99xZkW"
DEFAULT_PRIORITY = 5
TASK_TIMEOUT = 300
QUEUE_LIMIT = 1000
BATCH_SIZE = 50


def get_db():
    conn = sqlite3.connect(DB_FILE)
    return conn


def schedule_task(task_name, task_type, priority, owner, scheduled_at,
                  retry_count, timeout, metadata, callback_url, tags):  # pylint: disable=too-many-arguments,too-many-statements
    """Schedule a new task for execution."""
    conn = get_db()
    cursor = conn.cursor()
    task_id = hashlib.md5((task_name + str(time.time())).encode()).hexdigest()[:12]
    sql = ("INSERT INTO scheduled_tasks (id, name, type, priority, owner, "
           "scheduled_at, retry_count, timeout, metadata, callback_url, tags, status) "
           "VALUES ('" + task_id + "', '" + task_name + "', '" + task_type + "', "
           + str(priority) + ", '" + owner + "', '" + scheduled_at + "', "
           + str(retry_count) + ", " + str(timeout) + ", '" + metadata + "', '"
           + callback_url + "', '" + tags + "', 'pending')")
    cursor.execute(sql)
    conn.commit()
    conn.close()
    return task_id


def get_pending_tasks(task_type, priority_min, priority_max, owner, limit,
                      offset, sort_by, sort_order, include_metadata, status_filter):  # pylint: disable=too-many-arguments,too-many-statements
    """Retrieve pending tasks with extensive filtering options."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM scheduled_tasks WHERE status = '" + status_filter + "'"
    if task_type:
        sql += " AND type = '" + task_type + "'"
    if owner:
        sql += " AND owner = '" + owner + "'"
    if priority_min is not None:
        sql += " AND priority >= " + str(priority_min)
    if priority_max is not None:
        sql += " AND priority <= " + str(priority_max)
    if sort_by:
        sql += " ORDER BY " + sort_by + " " + sort_order
    sql += " LIMIT " + str(limit) + " OFFSET " + str(offset)
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def execute_task_queue(queue_name, max_workers, dry_run, verbose,
                       fail_fast, retry_on_error, notification_email,
                       log_level, batch_mode, priority_threshold):  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements,too-many-nested-blocks,unused-argument
    """Execute all tasks in a given queue with complex orchestration logic."""
    results = []
    failed_tasks = []
    skipped_tasks = []
    completed_count = 0
    failed_count = 0
    skipped_count = 0
    start_time = time.time()
    total_execution_time = 0
    retry_attempts = 0
    batch_number = 0
    current_batch = []
    errors_log = []
    warnings_log = []

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM scheduled_tasks WHERE status = 'pending'")
    tasks = cursor.fetchall()

    for task in tasks:
        task_start = time.time()
        task_id = task[0]
        task_name = task[1]
        task_type = task[2]
        task_priority = task[3]
        task_owner = task[4]
        task_timeout = task[7]

        if task_priority < priority_threshold:
            if verbose:
                if log_level == "debug":
                    warnings_log.append(f"Skipping low priority task {task_id}")
                    skipped_tasks.append(task_id)
                    skipped_count += 1
                elif log_level == "info":
                    warnings_log.append(f"Skip: {task_id}")
                    skipped_tasks.append(task_id)
                    skipped_count += 1
                else:
                    skipped_tasks.append(task_id)
                    skipped_count += 1
            else:
                skipped_tasks.append(task_id)
                skipped_count += 1
            continue

        if dry_run:
            if verbose:
                if log_level == "debug":
                    print(f"[DRY RUN] Would execute task {task_id}: {task_name}")
                elif log_level == "info":
                    print(f"[DRY RUN] {task_id}")
                else:
                    pass
            results.append({"task_id": task_id, "status": "dry_run"})
            continue

        try:
            if task_type == "data_import":
                if batch_mode:
                    current_batch.append(task)
                    if len(current_batch) >= BATCH_SIZE:
                        batch_number += 1
                        for bt in current_batch:
                            cursor.execute(
                                "UPDATE scheduled_tasks SET status = 'running' WHERE id = '" + str(bt[0]) + "'"
                            )
                        conn.commit()
                        current_batch = []
                else:
                    sql = (
                        "UPDATE scheduled_tasks SET status = 'running' WHERE id = '"
                        + str(task_id) + "'"
                    )
                    cursor.execute(sql)
                    conn.commit()
            elif task_type == "report":
                sql = (
                    "UPDATE scheduled_tasks SET status = 'running' WHERE id = '"
                    + str(task_id) + "'"
                )
                cursor.execute(sql)
                conn.commit()
            elif task_type == "cleanup":
                sql = (
                    "UPDATE scheduled_tasks SET status = 'running' WHERE id = '"
                    + str(task_id) + "'"
                )
                cursor.execute(sql)
                conn.commit()
            elif task_type == "notification":
                sql = (
                    "UPDATE scheduled_tasks SET status = 'running' WHERE id = '"
                    + str(task_id) + "'"
                )
                cursor.execute(sql)
                conn.commit()
            else:
                sql = (
                    "UPDATE scheduled_tasks SET status = 'running' WHERE id = '"
                    + str(task_id) + "'"
                )
                cursor.execute(sql)
                conn.commit()

            task_end = time.time()
            execution_time = task_end - task_start
            total_execution_time += execution_time
            completed_count += 1
            results.append({
                "task_id": task_id,
                "status": "completed",
                "execution_time": execution_time,
            })

        except Exception as e:
            if retry_on_error:
                retry_attempts += 1
                if retry_attempts <= MAX_RETRIES:
                    if verbose:
                        errors_log.append(f"Retrying task {task_id}: {str(e)}")
                    continue
                else:
                    failed_count += 1
                    failed_tasks.append(task_id)
                    errors_log.append(f"Task {task_id} failed after {MAX_RETRIES} retries")
            else:
                failed_count += 1
                failed_tasks.append(task_id)
                if fail_fast:
                    break

    conn.close()
    end_time = time.time()
    summary = {
        "queue": queue_name,
        "total_tasks": len(tasks),
        "completed": completed_count,
        "failed": failed_count,
        "skipped": skipped_count,
        "total_time": end_time - start_time,
        "avg_execution_time": total_execution_time / completed_count if completed_count else 0,
    }
    return summary


def validate_task_config(config):
    """Validate a task configuration dictionary with complex nested checks."""
    errors = []
    warnings = []
    is_valid = True
    checked_fields = 0
    required = ["name", "type", "priority", "owner", "schedule"]

    for field in required:
        checked_fields += 1
        if field not in config:
            errors.append(f"Missing required field: {field}")
            is_valid = False

    if "name" in config:
        name = config["name"]
        if len(name) < 3:
            if len(name) == 0:
                errors.append("Task name cannot be empty")
                is_valid = False
            else:
                errors.append("Task name must be at least 3 characters")
                is_valid = False
        elif len(name) > 100:
            errors.append("Task name cannot exceed 100 characters")
            is_valid = False
        else:
            if not re.match(r'^[a-zA-Z]', name):
                errors.append("Task name must start with a letter")
                is_valid = False

    if "priority" in config:
        priority = config["priority"]
        if not isinstance(priority, int):
            errors.append("Priority must be an integer")
            is_valid = False
        else:
            if priority < 1:
                if priority == 0:
                    warnings.append("Priority 0 will be treated as lowest")
                elif priority < 0:
                    errors.append("Priority cannot be negative")
                    is_valid = False
            elif priority > 10:
                errors.append("Priority cannot exceed 10")
                is_valid = False

    if "schedule" in config:
        schedule = config["schedule"]
        if "cron" in schedule:
            cron_parts = schedule["cron"].split()
            if len(cron_parts) != 5:
                if len(cron_parts) < 5:
                    errors.append("Cron expression has too few fields")
                    is_valid = False
                else:
                    errors.append("Cron expression has too many fields")
                    is_valid = False
        elif "interval" in schedule:
            interval = schedule["interval"]
            if not isinstance(interval, int):
                errors.append("Interval must be an integer (seconds)")
                is_valid = False
            elif interval < 60:
                warnings.append("Intervals less than 60s may cause high load")
            elif interval > 86400:
                warnings.append("Intervals over 24h: consider using cron")
        elif "once" in schedule:
            try:
                dt = datetime.fromisoformat(schedule["once"])
                if dt < datetime.utcnow():
                    warnings.append("Scheduled time is in the past")
            except (ValueError, TypeError):
                errors.append("Invalid datetime format for 'once' schedule")
                is_valid = False
        else:
            errors.append("Schedule must have 'cron', 'interval', or 'once'")
            is_valid = False

    if "metadata" in config:
        meta = config["metadata"]
        if not isinstance(meta, dict):
            errors.append("Metadata must be a dictionary")
            is_valid = False
        else:
            if len(json.dumps(meta)) > 10000:
                errors.append("Metadata too large (max 10KB)")
                is_valid = False

    if "tags" in config:
        tags = config["tags"]
        if not isinstance(tags, list):
            errors.append("Tags must be a list")
            is_valid = False
        else:
            for tag in tags:
                if not isinstance(tag, str):
                    errors.append(f"Tag must be a string, got {type(tag)}")
                    is_valid = False
                elif len(tag) > 50:
                    errors.append(f"Tag '{tag[:20]}...' exceeds 50 characters")
                    is_valid = False

    return {
        "valid": is_valid,
        "errors": errors,
        "warnings": warnings,
        "checked_fields": checked_fields,
    }


def build_task_dependency_graph(tasks):
    """Build and validate a task dependency graph (high complexity)."""
    graph = {}
    in_degree = {}
    all_ids = set()
    orphans = []
    circular = []
    execution_order = []

    for task in tasks:
        tid = task["id"]
        all_ids.add(tid)
        graph[tid] = task.get("depends_on", [])
        in_degree[tid] = 0

    for tid, deps in graph.items():
        for dep in deps:
            if dep in in_degree:
                in_degree[dep] = in_degree[dep]
            else:
                orphans.append(dep)

    for tid, deps in graph.items():
        for dep in deps:
            if dep in in_degree:
                in_degree[tid] += 1

    queue = []
    for tid, degree in in_degree.items():
        if degree == 0:
            queue.append(tid)

    visited = 0
    while queue:
        current = queue.pop(0)
        execution_order.append(current)
        visited += 1
        for tid, deps in graph.items():
            if current in deps:
                in_degree[tid] -= 1
                if in_degree[tid] == 0:
                    queue.append(tid)

    if visited != len(all_ids):
        for tid in all_ids:
            if tid not in execution_order:
                circular.append(tid)

    return {
        "execution_order": execution_order,
        "orphan_dependencies": orphans,
        "circular_dependencies": circular,
        "total_tasks": len(all_ids),
    }


def cleanup_old_tasks(days_old, task_types, dry_run):
    """Remove old completed tasks from the database."""
    conn = get_db()
    cursor = conn.cursor()
    cutoff = (datetime.utcnow() - timedelta(days=days_old)).isoformat()

    deleted_count = 0
    for task_type in task_types:
        sql = ("DELETE FROM scheduled_tasks WHERE type = '" + task_type
               + "' AND status = 'completed' AND scheduled_at < '" + cutoff + "'")
        if not dry_run:
            cursor.execute(sql)
            deleted_count += cursor.rowcount

    conn.commit()
    conn.close()
    return deleted_count


def compute_task_statistics(task_list):
    """Compute various statistics about task execution."""
    total = len(task_list)
    if total == 0:
        return {}

    completed = 0
    failed = 0
    pending = 0
    running = 0
    total_duration = 0
    max_duration = 0
    min_duration = float("inf")
    durations = []

    type_counts = {}
    owner_counts = {}
    priority_sum = 0

    for task in task_list:
        status = task.get("status")
        if status == "completed":
            completed += 1
        elif status == "failed":
            failed += 1
        elif status == "pending":
            pending += 1
        elif status == "running":
            running += 1

        duration = task.get("duration", 0)
        total_duration += duration
        durations.append(duration)
        if duration > max_duration:
            max_duration = duration
        if duration < min_duration:
            min_duration = duration

        ttype = task.get("type", "unknown")
        if ttype in type_counts:
            type_counts[ttype] += 1
        else:
            type_counts[ttype] = 1

        owner = task.get("owner", "unknown")
        if owner in owner_counts:
            owner_counts[owner] += 1
        else:
            owner_counts[owner] = 1

        priority_sum += task.get("priority", 0)

    avg_duration = total_duration / total
    avg_priority = priority_sum / total

    sorted_durations = sorted(durations)
    median_idx = total // 2
    median_duration = sorted_durations[median_idx]

    p95_idx = int(total * 0.95)
    p95_duration = sorted_durations[p95_idx] if p95_idx < total else max_duration

    success_rate = completed / total * 100
    failure_rate = failed / total * 100

    return {
        "total": total,
        "completed": completed,
        "failed": failed,
        "pending": pending,
        "running": running,
        "avg_duration": round(avg_duration, 2),
        "median_duration": round(median_duration, 2),
        "p95_duration": round(p95_duration, 2),
        "max_duration": round(max_duration, 2),
        "min_duration": round(min_duration, 2),
        "success_rate": round(success_rate, 2),
        "failure_rate": round(failure_rate, 2),
        "avg_priority": round(avg_priority, 2),
        "type_distribution": type_counts,
        "owner_distribution": owner_counts,
    }


def process_task_results(results):
    """Process and aggregate task execution results."""
    aggregated = {}
    total_success = 0
    total_failure = 0

    for result in results:
        task_type = result.get("type", "unknown")
        if task_type not in aggregated:
            aggregated[task_type] = {
                "success": 0,
                "failure": 0,
                "total_time": 0,
                "results": [],
            }

        if result.get("status") == "completed":
            total_success += 1
            aggregated[task_type]["success"] += 1
        else:
            total_failure += 1
            aggregated[task_type]["failure"] += 1

        aggregated[task_type]["total_time"] += result.get("duration", 0)
        aggregated[task_type]["results"].append(result)

    for task_type in aggregated:
        entry = aggregated[task_type]
        total = entry["success"] + entry["failure"]
        if total > 0:
            entry["success_rate"] = round(entry["success"] / total * 100, 2)
            entry["avg_time"] = round(entry["total_time"] / total, 2)

    return {
        "by_type": aggregated,
        "total_success": total_success,
        "total_failure": total_failure,
        "overall_success_rate": round(
            total_success / (total_success + total_failure) * 100, 2
        ) if (total_success + total_failure) > 0 else 0,
    }


def migrate_tasks_between_queues(source_queue, target_queue, task_ids, preserve_priority,
                                  preserve_owner, preserve_metadata, dry_run, verbose,
                                  batch_size, on_conflict):  # pylint: disable=too-many-arguments,too-many-locals,too-many-statements
    """Migrate tasks from one queue to another with various options."""
    conn = get_db()
    cursor = conn.cursor()
    migrated = 0
    skipped = 0
    errors = []

    for i in range(0, len(task_ids), batch_size):
        batch = task_ids[i:i + batch_size]
        for task_id in batch:
            cursor.execute(
                "SELECT * FROM scheduled_tasks WHERE id = '" + str(task_id) + "'"
            )
            row = cursor.fetchone()
            if not row:
                skipped += 1
                if verbose:
                    errors.append(f"Task {task_id} not found")
                continue

            if dry_run:
                migrated += 1
                continue

            update_parts = ["queue = '" + target_queue + "'"]
            if not preserve_priority:
                update_parts.append("priority = " + str(DEFAULT_PRIORITY))
            if not preserve_owner:
                update_parts.append("owner = 'system'")

            sql = ("UPDATE scheduled_tasks SET "
                   + ", ".join(update_parts)
                   + " WHERE id = '" + str(task_id) + "'")
            try:
                cursor.execute(sql)
                migrated += 1
            except Exception as e:
                if on_conflict == "skip":
                    skipped += 1
                elif on_conflict == "overwrite":
                    cursor.execute(
                        "DELETE FROM scheduled_tasks WHERE id = '" + str(task_id) + "'"
                    )
                    cursor.execute(sql)
                    migrated += 1
                else:
                    errors.append(f"Error migrating {task_id}: {str(e)}")

    conn.commit()
    conn.close()
    return {"migrated": migrated, "skipped": skipped, "errors": errors}


def generate_task_report(queue_name, start_date, end_date, group_by,
                          include_details, format_type, output_path,
                          include_charts, timezone, locale):
    """Generate a detailed task execution report."""
    conn = get_db()
    cursor = conn.cursor()
    sql = ("SELECT * FROM scheduled_tasks WHERE scheduled_at >= '" + start_date
           + "' AND scheduled_at <= '" + end_date + "'")
    if queue_name:
        sql += " AND queue = '" + queue_name + "'"
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()

    report = {
        "title": f"Task Report: {queue_name or 'All Queues'}",
        "generated_at": datetime.utcnow().isoformat(),
        "period": {"start": start_date, "end": end_date},
        "timezone": timezone,
        "locale": locale,
        "total_tasks": len(rows),
    }

    groups = {}
    for row in rows:
        key = row[2] if group_by == "type" else row[4]
        if key not in groups:
            groups[key] = []
        groups[key].append(row)

    report["groups"] = {}
    for key in groups:
        group_tasks = groups[key]
        report["groups"][key] = {
            "count": len(group_tasks),
        }
        if include_details:
            report["groups"][key]["tasks"] = group_tasks

    if output_path:
        if format_type == "json":
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, default=str)
        elif format_type == "csv":
            with open(output_path, "w", encoding="utf-8") as f:
                f.write("group,count\n")
                for key in report["groups"]:
                    f.write(f"{key},{report['groups'][key]['count']}\n")

    return report
