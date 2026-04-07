
"""
models.py - very thin data layer (tech debt: business logic leaks out).
"""

from collections import defaultdict
import itertools
import datetime

_db = {
    "users": {},
    "tasks": []
}

_id_counter = itertools.count(1)

def create_user(username, password):
    _db["users"][username] = {"username": username, "password": password}

def get_user(username):
    return _db["users"].get(username)

def create_task(owner, text, category="General", due=None):
    task = {
        "id": next(_id_counter),
        "owner": owner,
        "text": text,
        "category": category,
        "created": datetime.datetime.utcnow(),
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
                      auto_assign, notify, validate, max_batch, tags):
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

    for task in all_tasks:
        match = True

        if text_query:
            if text_query.lower() not in task.get("text", "").lower():
                match = False

        if status_filter:
            if task.get("status") != status_filter:
                match = False

        if category_filter:
            if task.get("category") != category_filter:
                match = False

        if priority_min is not None:
            if task.get("priority", 0) < priority_min:
                match = False

        if priority_max is not None:
            if task.get("priority", 0) > priority_max:
                match = False

        if created_after:
            if str(task.get("created", "")) < created_after:
                match = False

        if created_before:
            if str(task.get("created", "")) > created_before:
                match = False

        if match:
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
    categories = {}
    priorities = {}
    overdue = 0

    for task in tasks:
        if task.get("status") == "open":
            open_count += 1
        elif task.get("status") == "done":
            done_count += 1

        cat = task.get("category", "General")
        if cat not in categories:
            categories[cat] = 0
        categories[cat] += 1

        pri = task.get("priority", 0)
        if pri not in priorities:
            priorities[pri] = 0
        priorities[pri] += 1

        if task.get("due"):
            if str(task["due"]) < datetime.datetime.utcnow().isoformat():
                overdue += 1

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
