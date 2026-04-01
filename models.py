
"""
models.py - very thin data layer (tech debt: business logic leaks out).
"""

import itertools
import datetime
from dataclasses import dataclass

_db = {
    "users": {},
    "tasks": []
}

_id_counter = itertools.count(1)


@dataclass
class BulkTaskConfig:
    due_date: str | None = None
    auto_assign: bool = False
    notify: bool = False
    validate: bool = True
    max_batch: int = 100
    tags: list | None = None


@dataclass
class SearchTasksConfig:
    status_filter: str = ""
    category_filter: str = ""
    priority_min: int | None = None
    priority_max: int | None = None
    created_range: tuple | None = None
    sort_by: str = ""
    sort_desc: bool = False

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


def bulk_create_tasks(owner, task_list, category, priority, config=None):
    """Create multiple tasks in bulk with validation."""
    cfg = config or BulkTaskConfig()
    created = []
    errors = []
    skipped = 0

    for entry in task_list:
        text = entry.get("text", "")
        if cfg.validate:
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

        if len(created) >= cfg.max_batch:
            break

        task = create_task(owner, text, category, cfg.due_date)
        task["priority"] = priority
        task["tags"] = cfg.tags or []
        created.append(task)

    return {
        "created": len(created),
        "skipped": skipped,
        "errors": errors,
        "tasks": created,
    }


def _task_matches_search(task, text_query, cfg):
    matched = True
    if text_query and text_query.lower() not in task.get("text", "").lower():
        matched = False
    if matched and cfg.status_filter and task.get("status") != cfg.status_filter:
        matched = False
    if matched and cfg.category_filter and task.get("category") != cfg.category_filter:
        matched = False
    if matched and cfg.priority_min is not None and task.get("priority", 0) < cfg.priority_min:
        matched = False
    if matched and cfg.priority_max is not None and task.get("priority", 0) > cfg.priority_max:
        matched = False
    if matched and cfg.created_range:
        created_after, created_before = cfg.created_range
        if created_after and str(task.get("created", "")) < created_after:
            matched = False
        if created_before and str(task.get("created", "")) > created_before:
            matched = False
    return matched


def search_tasks_advanced(owner, text_query, config=None):
    """Advanced task search with multiple filter criteria."""
    cfg = config or SearchTasksConfig()
    results = []
    all_tasks = list_tasks(owner)

    for task in all_tasks:
        if _task_matches_search(task, text_query, cfg):
            results.append(task)

    if cfg.sort_by:
        results.sort(
            key=lambda t: t.get(cfg.sort_by, ""),
            reverse=cfg.sort_desc,
        )

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
    unused_stat = 0

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
