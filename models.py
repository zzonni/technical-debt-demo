
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
_UNSET = object()

_BULK_CREATE_KEYS = [
    "category",
    "priority",
    "due_date",
    "auto_assign",
    "notify",
    "validate",
    "max_batch",
    "tags",
]

_SEARCH_FILTER_KEYS = [
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


def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


def _resolve_options(option_values, keys, args, kwargs):
    options = {}
    if option_values is _UNSET:
        positional_values = list(args)
    elif isinstance(option_values, dict):
        options.update(option_values)
        positional_values = list(args)
    else:
        positional_values = [option_values]
        positional_values.extend(args)

    for key, value in zip(keys, positional_values):
        options[key] = value

    for key in keys:
        if key in kwargs:
            options[key] = kwargs[key]

    return options


def _task_text_is_valid(text, errors):
    if not text:
        errors.append("Empty task text")
        return False
    if len(text) > 500:
        errors.append(f"Task text too long: {text[:20]}...")
        return False
    if len(text) < 3:
        errors.append(f"Task text too short: {text}")
        return False
    return True


def _task_matches_filters(task, filters):
    text_query = filters.get("text_query")
    if text_query and text_query.lower() not in task.get("text", "").lower():
        return False

    status_filter = filters.get("status_filter")
    if status_filter and task.get("status") != status_filter:
        return False

    category_filter = filters.get("category_filter")
    if category_filter and task.get("category") != category_filter:
        return False

    priority_min = filters.get("priority_min")
    if priority_min is not None and task.get("priority", 0) < priority_min:
        return False

    priority_max = filters.get("priority_max")
    if priority_max is not None and task.get("priority", 0) > priority_max:
        return False

    created_after = filters.get("created_after")
    created_value = str(task.get("created", ""))
    if created_after and created_value < created_after:
        return False

    created_before = filters.get("created_before")
    if created_before and created_value > created_before:
        return False

    return True


def _sort_tasks(tasks, sort_by, sort_order):
    if not sort_by:
        return tasks

    reverse = sort_order == "desc"
    tasks.sort(key=lambda task: task.get(sort_by, ""), reverse=reverse)
    return tasks

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
        "created": _utc_now(),
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


def bulk_create_tasks(owner, task_list, options=_UNSET, *args, **kwargs):
    """Create multiple tasks in bulk with validation."""
    resolved = _resolve_options(options, _BULK_CREATE_KEYS, args, kwargs)
    category = resolved.get("category", "General")
    priority = resolved.get("priority", 0)
    due_date = resolved.get("due_date")
    validate = resolved.get("validate", False)
    max_batch = resolved.get("max_batch", len(task_list))
    tags = resolved.get("tags", [])

    created = []
    errors = []
    skipped = 0

    for entry in task_list:
        text = entry.get("text", "")
        if validate and not _task_text_is_valid(text, errors):
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


def search_tasks_advanced(owner, filters=_UNSET, *args, **kwargs):
    """Advanced task search with multiple filter criteria."""
    resolved = _resolve_options(filters, _SEARCH_FILTER_KEYS, args, kwargs)
    results = [task for task in list_tasks(owner) if _task_matches_filters(task, resolved)]
    return _sort_tasks(results, resolved.get("sort_by"), resolved.get("sort_order"))


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

        if task.get("due") and str(task["due"]) < _utc_now().isoformat():
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
