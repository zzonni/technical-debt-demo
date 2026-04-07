"""
Task Scheduler Module - Deliberately packed with technical debt.
Handles task scheduling, execution, validation, and reporting.
"""

import sqlite3
import json
import re
import csv
from datetime import datetime
from collections import deque, defaultdict


def get_db():
    """
    Get database connection to ecommerce.db.
    DEBT: Global connection, no connection pooling, hardcoded path.
    """
    return sqlite3.connect("ecommerce.db")


def validate_task_config(config):
    """
    Validate task configuration dictionary.
    DEBT: Missing docstring for parameters, inconsistent error messages.
    """
    errors = []
    warnings = []
    
    # Check required fields
    required_fields = ["name", "type", "priority", "owner", "schedule"]
    for field in required_fields:
        if field not in config:
            errors.append(f"Missing required field: {field}")
    
    if errors:
        return {"valid": False, "errors": errors, "warnings": warnings}
    
    # Validate name
    name = config.get("name", "")
    if not name:
        errors.append("Task name cannot be empty")
    elif len(name) < 3:
        errors.append("Task name must be at least 3 characters long")
    elif len(name) > 100:
        errors.append("Task name cannot exceed 100 characters")
    elif not name[0].isalpha():
        errors.append("Task name must start with a letter")
    
    # Validate priority
    priority = config.get("priority")
    if not isinstance(priority, int):
        errors.append("Priority must be an integer")
    elif priority < 0:
        errors.append("Priority cannot be negative")
    elif priority > 10:
        errors.append("Priority cannot exceed 10")
    elif priority == 0:
        warnings.append("Priority 0 is the lowest priority level")
    
    # Validate schedule
    schedule = config.get("schedule", {})
    if not schedule:
        errors.append("Schedule is required")
    elif "cron" in schedule:
        cron = schedule["cron"]
        fields = cron.split()
        if len(fields) < 5:
            errors.append("Cron expression has too few fields")
        elif len(fields) > 5:
            errors.append("Cron expression has too many fields")
    elif "interval" in schedule:
        interval = schedule["interval"]
        if not isinstance(interval, int):
            errors.append("Interval must be an integer")
        elif interval < 60:
            warnings.append("Interval below 60 seconds may cause high load")
        elif interval > 86400:
            warnings.append("Consider using cron for intervals over 24 hours")
    elif "once" in schedule:
        try:
            datetime.fromisoformat(schedule["once"])
        except (ValueError, TypeError):
            errors.append("Invalid datetime format for 'once' schedule")
    else:
        errors.append("Schedule must have cron, interval, or once")
    
    # Validate metadata
    if "metadata" in config:
        metadata = config["metadata"]
        if not isinstance(metadata, dict):
            errors.append("Metadata must be a dictionary")
        else:
            metadata_str = json.dumps(metadata)
            if len(metadata_str) > 10000:
                errors.append("Metadata size exceeds 10KB limit")
    
    # Validate tags
    if "tags" in config:
        tags = config["tags"]
        if not isinstance(tags, list):
            errors.append("Tags must be a list")
        else:
            for tag in tags:
                if not isinstance(tag, str):
                    errors.append("All tags must be strings")
                    break
                elif len(tag) > 50:
                    errors.append(f"Tag '{tag}' exceeds 50 character limit")
                    break
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }


def build_task_dependency_graph(tasks):
    """
    Build task dependency graph and detect cycles.
    DEBT: No handling of duplicate task IDs, linear cycle detection.
    """
    # Build reverse graph: task_id -> list of tasks that depend on it
    depends_on_map = {}  # task_id -> list of dependencies
    all_tasks = set()
    orphan_dependencies = set()
    
    for task in tasks:
        task_id = task.get("id")
        all_tasks.add(task_id)
        depends_on = task.get("depends_on", [])
        depends_on_map[task_id] = depends_on
    
    # Find orphan dependencies
    for task in tasks:
        depends_on = task.get("depends_on", [])
        for dep in depends_on:
            if dep not in all_tasks:
                orphan_dependencies.add(dep)
    
    # Topological sort with DFS (post-order) - dependencies should come first
    execution_order = []
    visited = set()
    visiting = set()
    circular_dependencies = []
    
    def visit(node):
        if node in visited:
            return True
        if node in visiting:
            return False  # Cycle detected
        visiting.add(node)
        
        # Visit dependencies first
        for dep in depends_on_map.get(node, []):
            if not visit(dep):
                return False
        
        visiting.remove(node)
        visited.add(node)
        execution_order.append(node)
        return True
    
    for task in tasks:
        task_id = task.get("id")
        if task_id not in visited:
            if not visit(task_id):
                circular_dependencies.append(task_id)
    
    return {
        "total_tasks": len(tasks),
        "execution_order": execution_order,
        "circular_dependencies": circular_dependencies,
        "orphan_dependencies": list(orphan_dependencies)
    }


def compute_task_statistics(tasks):
    """
    Compute statistics from a list of tasks.
    DEBT: No error handling for malformed tasks.
    """
    if not tasks:
        return {}
    
    stats = {
        "total": len(tasks),
        "completed": 0,
        "failed": 0,
        "pending": 0,
        "running": 0,
        "type_distribution": defaultdict(int),
        "owner_distribution": defaultdict(int),
        "durations": []
    }
    
    for task in tasks:
        status = task.get("status")
        if status == "completed":
            stats["completed"] += 1
        elif status == "failed":
            stats["failed"] += 1
        elif status == "pending":
            stats["pending"] += 1
        elif status == "running":
            stats["running"] += 1
        
        task_type = task.get("type")
        if task_type:
            stats["type_distribution"][task_type] += 1
        
        owner = task.get("owner")
        if owner:
            stats["owner_distribution"][owner] += 1
        
        duration = task.get("duration", 0)
        if duration:
            stats["durations"].append(duration)
    
    # Convert defaultdicts to regular dicts
    stats["type_distribution"] = dict(stats["type_distribution"])
    stats["owner_distribution"] = dict(stats["owner_distribution"])
    
    # Calculate rates
    if stats["total"] > 0:
        stats["success_rate"] = (stats["completed"] / stats["total"]) * 100
        stats["failure_rate"] = (stats["failed"] / stats["total"]) * 100
    
    # Calculate duration stats
    if stats["durations"]:
        stats["avg_duration"] = sum(stats["durations"]) / len(stats["durations"])
        stats["min_duration"] = min(stats["durations"])
        stats["max_duration"] = max(stats["durations"])
    
    del stats["durations"]  # Remove temp list
    
    return stats


def process_task_results(results):
    """
    Process task execution results.
    DEBT: No validation of results structure.
    """
    if not results:
        return {
            "total_success": 0,
            "total_failure": 0,
            "overall_success_rate": 0,
            "by_type": {}
        }
    
    by_type = defaultdict(lambda: {"success": 0, "failure": 0})
    total_success = 0
    total_failure = 0
    
    for result in results:
        task_type = result.get("type")
        status = result.get("status")
        
        if status == "completed":
            total_success += 1
            by_type[task_type]["success"] += 1
        elif status == "failed":
            total_failure += 1
            by_type[task_type]["failure"] += 1
    
    # Calculate success rates by type
    for task_type in by_type:
        total = by_type[task_type]["success"] + by_type[task_type]["failure"]
        if total > 0:
            by_type[task_type]["success_rate"] = (by_type[task_type]["success"] / total) * 100
    
    overall_success_rate = 0
    if total_success + total_failure > 0:
        overall_success_rate = (total_success / (total_success + total_failure)) * 100
    
    return {
        "total_success": total_success,
        "total_failure": total_failure,
        "overall_success_rate": overall_success_rate,
        "by_type": dict(by_type)
    }


def schedule_task(task_name, task_type, priority, owner, scheduled_at,
                  retry_count, timeout, metadata, callback_url, tags):
    """
    Schedule a new task in the database.
    DEBT: No input validation, SQL injection vulnerable if used with user input.
    """
    db = get_db()
    cursor = db.cursor()
    
    # Generate simple task ID (DEBT: not production-grade)
    import random
    import string
    task_id = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
    
    sql = """
    INSERT INTO tasks (task_id, name, type, priority, owner, scheduled_at, 
                       retry_count, timeout, metadata, callback_url, tags, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
    """
    cursor.execute(sql, (task_id, task_name, task_type, priority, owner,
                        scheduled_at, retry_count, timeout, metadata,
                        callback_url, tags))
    db.commit()
    db.close()
    
    return task_id


def get_pending_tasks(task_type=None, priority_min=None, priority_max=None,
                      owner=None, limit=10, offset=0, sort_by=None,
                      sort_order="ASC", include_metadata=False, status_filter="pending"):
    """
    Retrieve pending tasks with optional filtering and sorting.
    DEBT: Dynamic SQL construction risk, no query validation.
    """
    db = get_db()
    cursor = db.cursor()
    
    sql = "SELECT task_id FROM tasks WHERE status = ?"
    params = [status_filter]
    
    if task_type:
        sql += " AND type = ?"
        params.append(task_type)
    
    if priority_min is not None:
        sql += " AND priority >= ?"
        params.append(priority_min)
    
    if priority_max is not None:
        sql += " AND priority <= ?"
        params.append(priority_max)
    
    if owner:
        sql += " AND owner = ?"
        params.append(owner)
    
    if sort_by:
        sql += f" ORDER BY {sort_by} {sort_order}"
    
    sql += f" LIMIT {limit} OFFSET {offset}"
    
    cursor.execute(sql, params)
    results = cursor.fetchall()
    db.close()
    
    return results


def cleanup_old_tasks(days_old, task_types, dry_run=False):
    """
    Clean up tasks older than specified days.
    DEBT: Uses sys.modules-style patching in tests, no real cleanup logic here.
    """
    if dry_run:
        return 0
    
    db = get_db()
    cursor = db.cursor()
    
    type_placeholders = ','.join('?' * len(task_types))
    sql = f"""
    DELETE FROM tasks 
    WHERE datetime(scheduled_at) < datetime('now', '-{days_old} days')
    AND type IN ({type_placeholders})
    """
    cursor.execute(sql, task_types)
    db.commit()
    
    deleted_count = cursor.rowcount
    db.close()
    
    return deleted_count


def migrate_tasks_between_queues(source_queue, dest_queue, task_ids,
                                 preserve_priority=True, preserve_owner=True,
                                 preserve_metadata=True, dry_run=False,
                                 verbose=False, batch_size=10, on_conflict="skip"):
    """
    Migrate tasks between queues with conflict handling.
    DEBT: Complex logic with multiple branches, SQL injection vulnerabilities in string formatting.
    """
    db = get_db()
    cursor = db.cursor()
    
    result = {
        "migrated": 0,
        "skipped": 0,
        "errors": []
    }
    
    for task_id in task_ids:
        # Fetch task from source queue - DEBT: SQL injection risk
        sql = f"SELECT task_id, name, type, priority, owner FROM tasks WHERE task_id = '{task_id}'"
        cursor.execute(sql)
        task_row = cursor.fetchone()
        
        if not task_row:
            result["skipped"] += 1
            continue
        
        if not dry_run:
            try:
                # Prepare values for destination
                priority = task_row[3] if preserve_priority else 5
                owner = task_row[4] if preserve_owner else "system"
                
                # Try insert/update to destination queue - DEBT: SQL injection risk
                try:
                    update_sql = f"UPDATE tasks SET queue = '{dest_queue}' WHERE task_id = '{task_id}'"
                    cursor.execute(update_sql)
                    db.commit()
                    result["migrated"] += 1
                except Exception as conflict_err:
                    if on_conflict == "skip":
                        result["skipped"] += 1
                    elif on_conflict == "overwrite":
                        try:
                            delete_sql = f"DELETE FROM tasks WHERE task_id = '{task_id}'"
                            cursor.execute(delete_sql)
                            update_sql = f"UPDATE tasks SET queue = '{dest_queue}' WHERE task_id = '{task_id}'"
                            cursor.execute(update_sql)
                            db.commit()
                            result["migrated"] += 1
                        except Exception as delete_err:
                            result["errors"].append(str(delete_err))
                    elif on_conflict == "fail":
                        result["errors"].append(f"Conflict for task {task_id}")
            except Exception as e:
                if on_conflict == "fail":
                    result["errors"].append(str(e))
                else:
                    result["errors"].append(str(e))
        else:
            result["migrated"] += 1
    
    db.close()
    return result


def generate_task_report(queue_name=None, start_date=None, end_date=None, group_by=None,
                        include_details=False, format_type="json", output_path=None,
                        include_charts=False, timezone="UTC", locale="en"):
    """
    Generate task report with optional grouping and export.
    DEBT: No timezone handling, hardcoded chart generation, localization ignored.
    """
    db = get_db()
    cursor = db.cursor()
    
    sql = "SELECT task_id, name, type, priority, owner, scheduled_at, retry_count, timeout, metadata, callback_url, tags, status FROM tasks"
    
    if queue_name:
        sql += f" WHERE queue = '{queue_name}'"
    
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    report = {
        "total_tasks": len(rows),
        "groups": {}
    }
    
    if group_by:
        groups = defaultdict(int)
        for row in rows:
            if group_by == "type":
                key = row[2]
            elif group_by == "owner":
                key = row[4]
            else:
                key = "all"
            groups[key] += 1
        report["groups"] = dict(groups)
    
    if output_path:
        if format_type == "json":
            with open(output_path, "w") as f:
                json.dump(report, f)
        elif format_type == "csv":
            with open(output_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["group", "count"])
                for group, count in report.items():
                    if group != "total_tasks":
                        if isinstance(report[group], dict):
                            for k, v in report[group].items():
                                writer.writerow([k, v])
    
    db.close()
    return report


def execute_task_queue(queue_name, worker_threads, dry_run=False, verbose=False,
                       fail_fast=False, retry_on_error=False, timeout_override=None,
                       log_level="info", batch_mode=False, priority_threshold=3):
    """
    Execute pending tasks from the queue.
    DEBT: Hardcoded task type handlers, logging inefficient, complex conditionals.
    """
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute("SELECT task_id, name, type, priority, owner, scheduled_at, retry_count, timeout, metadata, callback_url, tags, status FROM tasks WHERE status = 'pending' ORDER BY priority DESC")
    rows = cursor.fetchall()
    
    result = {
        "completed": 0,
        "failed": 0,
        "skipped": 0,
        "total_tasks": len(rows)
    }
    
    for row in rows:
        task_id, name, task_type, priority, owner, scheduled_at, retry_count, timeout, metadata, callback_url, tags, status = row
        
        # Skip low priority tasks
        if priority < priority_threshold:
            result["skipped"] += 1
            if verbose:
                if log_level == "debug":
                    print(f"[DEBUG] Skipping low priority task {task_id}")
                elif log_level == "info":
                    print(f"[INFO] Skipping task {task_id}")
                else:
                    print(f"[{log_level.upper()}] Task {task_id} skipped")
            continue
        
        if dry_run:
            result["completed"] += 1
            if verbose:
                if log_level == "debug":
                    print(f"[DEBUG] Would execute task {task_id}")
                elif log_level == "info":
                    print(f"[INFO] Dry run: {task_id}")
                else:
                    print(f"[{log_level.upper()}] Task {task_id}")
            continue
        
        # Execute based on type
        try:
            if task_type == "report":
                # Execute report task
                result["completed"] += 1
            elif task_type == "cleanup":
                # Execute cleanup task
                result["completed"] += 1
            elif task_type == "notification":
                # Execute notification task
                result["completed"] += 1
            elif task_type == "data_import":
                # Execute data import (batch or regular)
                if batch_mode:
                    # Batch processing
                    pass
                else:
                    # Regular processing
                    pass
                result["completed"] += 1
            else:
                # Unknown task type - still complete
                result["completed"] += 1
            
            # Update task status
            cursor.execute("UPDATE tasks SET status = 'completed' WHERE task_id = ?", (task_id,))
            db.commit()
            
        except Exception as e:
            result["failed"] += 1
            if fail_fast:
                # When fail_fast is True, catch the exception and mark as failed
                pass
            elif retry_on_error and retry_count < 3:
                try:
                    cursor.execute("UPDATE tasks SET retry_count = retry_count + 1 WHERE task_id = ?", (task_id,))
                    db.commit()
                except Exception:
                    # If retry update fails, just continue
                    pass
    
    db.close()
    return result
