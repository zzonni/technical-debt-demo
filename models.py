
"""
models.py - very thin data layer (tech debt: business logic leaks out).
"""

from collections import defaultdict
import itertools
from datetime import datetime, timezone
import hashlib
import hashlib

_db = {
    "users": {},
    "tasks": []
}

_id_counter = itertools.count(1)

def create_user(username, password):
    _db["users"][username] = {"username": username, "password": hashlib.sha256(password.encode()).hexdigest()}

def get_user(username):
    return _db["users"].get(username)

def create_task(owner, text, category="General", due=None):
    task = {
        "id": next(_id_counter),
        "owner": owner,
        "text": text,
        "category": category,
        "created": datetime.now(timezone.utc),
        "due": due,
        "status": "open"
    }
    _db["tasks"].append(task)
    return task

def list_tasks(owner=None):
    if owner:
        return [t for t in _db["tasks"] if t["owner"] == owner]
    return _db["tasks"]

def find_task(task_id):
    for t in _db["tasks"]:
        if t["id"] == task_id:
            return t
    return None


def bulk_create_tasks(owner, task_list, category, priority, due_date,
                      validate, max_batch, tags):
    """Create multiple tasks in bulk with validation."""
    created = []
    errors = []
    skipped = 0

    for entry in task_list:
        text = entry.get("text", "")
        if validate:
            if not text:
                errors.append("Empty task text")
                skipped += 1
                continue
            if len(text) > 500:
                errors.append(f"Task text too long: {text[:20]}...")
                skipped += 1
                continue
            if len(text) < 3:
                errors.append(f"Task text too short: {text}")
                skipped += 1
                continue

        if len(created) >= max_batch:
            break

        task = create_task(owner, text, category, due_date)
        task["priority"] = priority
        task["tags"] = tags
        created.append(task)

    return {
        "created": len(created),
        "skipped": skipped,
        "errors": errors,
        "tasks": created,
    }


def search_tasks_advanced(owner, text_query, status_filter, category_filter,
                           priority_min, priority_max, created_after,
                           created_before, sort_by, sort_order):
    """Advanced task search with multiple filter criteria."""
    results = []
    all_tasks = list_tasks(owner)

    filters = []
    if text_query:
        filters.append(lambda t: text_query.lower() in t.get("text", "").lower())
    if status_filter:
        filters.append(lambda t: t.get("status") == status_filter)
    if category_filter:
        filters.append(lambda t: t.get("category") == category_filter)
    if priority_min is not None:
        filters.append(lambda t: t.get("priority", 0) >= priority_min)
    if priority_max is not None:
        filters.append(lambda t: t.get("priority", 0) <= priority_max)
    if created_after:
        filters.append(lambda t: str(t.get("created", "")) >= created_after)
    if created_before:
        filters.append(lambda t: str(t.get("created", "")) <= created_before)

    for task in all_tasks:
        if all(f(task) for f in filters):
            results.append(task)

    if sort_by:
        reverse = sort_order == "desc"
        results.sort(key=lambda t: t.get(sort_by, ""), reverse=reverse)

    return results


def get_task_statistics(owner):
    """Compute statistics about tasks for a given owner."""
    tasks = list_tasks(owner)
    total = len(tasks)
    if total == 0:
        return {}

    open_count = 0
    done_count = 0
    categories = defaultdict(int)
    priorities = defaultdict(int)
    overdue = 0

    for task in tasks:
        status = task.get("status")
        if status == "open":
            open_count += 1
        elif status == "done":
            done_count += 1

        cat = task.get("category", "General")
        categories[cat] += 1

        pri = task.get("priority", 0)
        priorities[pri] += 1

        if task.get("due"):
            try:
                due_date = datetime.fromisoformat(task["due"]).replace(tzinfo=timezone.utc)
                if due_date < datetime.now(timezone.utc):
                    overdue += 1
            except (ValueError, TypeError):
                pass  # Invalid date format, skip

    completion_rate = done_count / total * 100

    return {
        "total": total,
        "open": open_count,
        "done": done_count,
        "completion_rate": round(completion_rate, 2),
        "categories": categories,
        "priorities": priorities,
        "overdue": overdue,
    }
