
"""
models.py - very thin data layer (tech debt: business logic leaks out).
"""

import itertools
import datetime

_db = {
    "users": {},
    "tasks": []
}

_id_counter = itertools.count(1)


def _current_utc_time():
    return datetime.datetime.now(datetime.timezone.utc)


def _resolve_bulk_create_options(args, kwargs):
    option_names = [
        "category",
        "priority",
        "due_date",
        "auto_assign",
        "notify",
        "validate",
        "max_batch",
        "tags",
    ]
    options = {
        "category": "General",
        "priority": 0,
        "due_date": None,
        "auto_assign": False,
        "notify": False,
        "validate": False,
        "max_batch": 100,
        "tags": [],
    }
    for name, value in zip(option_names, args):
        options[name] = value
    for name in option_names:
        if name in kwargs:
            options[name] = kwargs[name]
    return options


def _resolve_task_search_options(args, kwargs):
    option_names = [
        "text_query",
        "status_filter",
        "category_filter",
        "priority_min",
        "priority_max",
        "created_after",
        "created_before",
        "sort_by",
        "sort_order",
    ]
    options = dict.fromkeys(option_names)
    for name, value in zip(option_names, args):
        options[name] = value
    for name in option_names:
        if name in kwargs:
            options[name] = kwargs[name]
    return options


def _validate_bulk_task_text(text):
    if not text:
        return "Empty task text"
    if len(text) > 500:
        return f"Task text too long: {text[:20]}..."
    if len(text) < 3:
        return f"Task text too short: {text}"
    return None


def _matches_task_search(task, options):
    if options["text_query"] and options["text_query"].lower() not in task.get("text", "").lower():
        return False
    if options["status_filter"] and task.get("status") != options["status_filter"]:
        return False
    if options["category_filter"] and task.get("category") != options["category_filter"]:
        return False
    if options["priority_min"] is not None and task.get("priority", 0) < options["priority_min"]:
        return False
    if options["priority_max"] is not None and task.get("priority", 0) > options["priority_max"]:
        return False
    if options["created_after"] and str(task.get("created", "")) < options["created_after"]:
        return False
    if options["created_before"] and str(task.get("created", "")) > options["created_before"]:
        return False
    return True

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
        "created": _current_utc_time(),
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


def bulk_create_tasks(owner, task_list, *args, **kwargs):
    """Create multiple tasks in bulk with validation."""
    options = _resolve_bulk_create_options(args, kwargs)
    created = []
    errors = []
    skipped = 0

    for entry in task_list:
        text = entry.get("text", "")
        if options["validate"]:
            validation_error = _validate_bulk_task_text(text)
            if validation_error:
                errors.append(validation_error)
                skipped += 1
                continue

        if len(created) >= options["max_batch"]:
            break

        task = create_task(owner, text, options["category"], options["due_date"])
        task["priority"] = options["priority"]
        task["tags"] = options["tags"]
        created.append(task)

    return {
        "created": len(created),
        "skipped": skipped,
        "errors": errors,
        "tasks": created,
    }


def search_tasks_advanced(owner, *args, **kwargs):
    """Advanced task search with multiple filter criteria."""
    options = _resolve_task_search_options(args, kwargs)
    results = [task for task in list_tasks(owner) if _matches_task_search(task, options)]

    if options["sort_by"]:
        reverse = options["sort_order"] == "desc"
        results.sort(key=lambda task: task.get(options["sort_by"], ""), reverse=reverse)

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
            if str(task["due"]) < _current_utc_time().isoformat():
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
