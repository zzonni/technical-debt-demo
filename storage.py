import json
import os
from datetime import datetime, timezone

DATA_FILE = "todos.json"

_BULK_ADD_KEYS = [
    "category",
    "priority",
    "due_date",
    "owner",
    "validate",
    "skip_duplicates",
    "max_batch",
    "notify",
    "tags",
]

_SEARCH_KEYS = [
    "query",
    "status_filter",
    "category_filter",
    "owner_filter",
    "priority_min",
    "priority_max",
    "created_after",
    "created_before",
    "sort_by",
    "sort_order",
]

_EXPORT_KEYS = [
    "status_filter",
    "owner_filter",
    "include_metadata",
    "sort_by",
    "sort_order",
    "date_format",
    "encoding",
    "delimiter",
]

_UNSET = object()


def _utc_now():
    return datetime.now(timezone.utc)


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


def _is_valid_task_name(name, errors):
    if not name or not name.strip():
        errors.append("Empty task name")
        return False
    if len(name) > 200:
        errors.append(f"Name too long: {name[:20]}...")
        return False
    if len(name) < 2:
        errors.append(f"Name too short: {name}")
        return False
    return True


def _create_item_record(next_id, name, options):
    return {
        "id": next_id,
        "text": name,
        "status": "open",
        "created_at": _utc_now().isoformat(),
        "category": options.get("category"),
        "priority": options.get("priority"),
        "due_date": options.get("due_date"),
        "owner": options.get("owner"),
        "tags": options.get("tags", []),
    }


def _matches_expected(actual, expected):
    return expected is None or actual == expected


def _within_minimum(actual, minimum):
    return minimum is None or actual >= minimum


def _within_maximum(actual, maximum):
    return maximum is None or actual <= maximum


def _within_created_range(created_at, filters):
    created_after = filters.get("created_after")
    created_before = filters.get("created_before")
    if created_after and created_at < created_after:
        return False
    if created_before and created_at > created_before:
        return False
    return True


def _item_matches_filters(item, filters):
    query = filters.get("query")
    if query and query.lower() not in item.get("text", "").lower():
        return False

    created_at = item.get("created_at", "")
    return all(
        (
            _matches_expected(item.get("status"), filters.get("status_filter")),
            _matches_expected(item.get("category"), filters.get("category_filter")),
            _matches_expected(item.get("owner"), filters.get("owner_filter")),
            _within_minimum(item.get("priority", 0), filters.get("priority_min")),
            _within_maximum(item.get("priority", 0), filters.get("priority_max")),
            _within_created_range(created_at, filters),
        )
    )


def _sort_items(items, sort_by, sort_order):
    if not sort_by:
        return items

    reverse = sort_order == "desc"
    items.sort(key=lambda item: item.get(sort_by, ""), reverse=reverse)
    return items


def _filter_export_items(items, resolved):
    filtered = []
    status_filter = resolved.get("status_filter")
    owner_filter = resolved.get("owner_filter")
    for item in items:
        if status_filter and item.get("status") != status_filter:
            continue
        if owner_filter and item.get("owner") != owner_filter:
            continue
        filtered.append(item)
    return filtered


def _write_export_file(output_path, format_type, items, encoding, delimiter):
    if format_type == "json":
        with open(output_path, "w", encoding=encoding) as f:
            json.dump(items, f, indent=2)
        return

    if format_type == "csv":
        with open(output_path, "w", encoding=encoding) as f:
            if items:
                headers = list(items[0].keys())
                f.write(delimiter.join(headers) + "\n")
                for item in items:
                    values = [str(item.get(header, "")) for header in headers]
                    f.write(delimiter.join(values) + "\n")
        return

    if format_type == "txt":
        with open(output_path, "w", encoding=encoding) as f:
            for item in items:
                f.write(f"{item.get('id', '')} | {item.get('text', '')} | {item.get('status', '')}\n")


def _ensure_file():
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)


def _normalize_record(record):
    if "text" not in record and "title" in record:
        record["text"] = record["title"]
    if "status" not in record:
        record["status"] = "done" if record.get("done") else "open"
    if "created_at" not in record:
        record["created_at"] = _utc_now().isoformat()
    return record


def load_items():
    _ensure_file()
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [_normalize_record(x) for x in data]


def save_items(items):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)


def add_item(task_name):
    items = load_items()
    next_id = 1
    if items:
        next_id = max(x["id"] for x in items) + 1
    item = {
        "id": next_id,
        "text": task_name,
        "status": "open",
        "created_at": _utc_now().isoformat(),
    }
    items.append(item)
    save_items(items)
    return item


def delete_item(item_id):
    items = load_items()
    items = [x for x in items if x["id"] != item_id]
    save_items(items)


def toggle_item(todo_id):
    items = load_items()
    for item in items:
        if item["id"] == todo_id:
            if item.get("status") == "done":
                item["status"] = "open"
            else:
                item["status"] = "done"
    save_items(items)


def clear_done_items():
    items = load_items()
    kept = []
    for item in items:
        if item.get("status") != "done":
            kept.append(item)
    save_items(kept)


def bulk_add_items(task_names, options=_UNSET, *args, **kwargs):
    """Add multiple items in bulk with validation and dedup."""
    resolved = _resolve_options(options, _BULK_ADD_KEYS, args, kwargs)
    items = load_items()
    added = []
    skipped = 0
    errors = []
    existing_texts = {item.get("text", "").lower() for item in items} if resolved.get("skip_duplicates") else set()
    next_id = max((item.get("id", 0) for item in items), default=0) + 1

    for name in task_names:
        if resolved.get("validate") and not _is_valid_task_name(name, errors):
            skipped += 1
            continue

        if resolved.get("skip_duplicates") and name.lower() in existing_texts:
            skipped += 1
            continue

        if len(added) >= resolved.get("max_batch", len(task_names)):
            break

        item = _create_item_record(next_id, name, resolved)
        next_id += 1
        items.append(item)
        existing_texts.add(name.lower())
        added.append(item)

    save_items(items)

    return {
        "added": len(added),
        "skipped": skipped,
        "errors": errors,
        "items": added,
    }


def search_items_advanced(filters=_UNSET, *args, **kwargs):
    """Search items with multiple filter criteria."""
    resolved = _resolve_options(filters, _SEARCH_KEYS, args, kwargs)
    results = [item for item in load_items() if _item_matches_filters(item, resolved)]
    return _sort_items(results, resolved.get("sort_by"), resolved.get("sort_order"))


def get_storage_statistics():
    """Compute statistics about stored items."""
    items = load_items()
    total = len(items)
    if total == 0:
        return {}

    open_count = 0
    done_count = 0
    categories = {}
    owners = {}
    for item in items:
        if item.get("status") == "open":
            open_count += 1
        elif item.get("status") == "done":
            done_count += 1

        cat = item.get("category", "uncategorized")
        if cat not in categories:
            categories[cat] = 0
        categories[cat] += 1

        owner = item.get("owner", "unassigned")
        if owner not in owners:
            owners[owner] = 0
        owners[owner] += 1

    completion_rate = done_count / total * 100

    return {
        "total": total,
        "open": open_count,
        "done": done_count,
        "completion_rate": round(completion_rate, 2),
        "categories": categories,
        "owners": owners,
    }


def export_items_to_file(output_path, format_type, options=_UNSET, *args, **kwargs):
    """Export items to a file with various format options."""
    resolved = _resolve_options(options, _EXPORT_KEYS, args, kwargs)
    filtered = _filter_export_items(load_items(), resolved)
    _sort_items(filtered, resolved.get("sort_by"), resolved.get("sort_order"))

    encoding = resolved.get("encoding", "utf-8")
    delimiter = resolved.get("delimiter", ",")
    _write_export_file(output_path, format_type, filtered, encoding, delimiter)

    return {"exported": len(filtered), "output_path": output_path}
