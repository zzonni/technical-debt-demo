import json
import os
from datetime import datetime
from datetime import timezone

DATA_FILE = "todos.json"


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
        record["created_at"] = datetime.now(timezone.utc).isoformat()
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
        "created_at": datetime.now(timezone.utc).isoformat(),
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


def _get_bulk_add_options(args, kwargs):
    option_names = [
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
    options = dict(zip(option_names, args))
    options.update(kwargs)
    options.setdefault("category", "General")
    options.setdefault("priority", 0)
    options.setdefault("due_date", None)
    options.setdefault("owner", None)
    options.setdefault("validate", False)
    options.setdefault("skip_duplicates", False)
    options.setdefault("max_batch", 0)
    options.setdefault("tags", [])
    return options


def _validate_task_name(name):
    if not name or not name.strip():
        return "Empty task name"
    if len(name) > 200:
        return f"Name too long: {name[:20]}..."
    if len(name) < 2:
        return f"Name too short: {name}"
    return None


def bulk_add_items(task_names, *args, **kwargs):
    """Add multiple items in bulk with validation and dedup."""
    options = _get_bulk_add_options(args, kwargs)
    items = load_items()
    added = []
    skipped = 0
    errors = []
    existing_texts = set()

    if options["skip_duplicates"]:
        for item in items:
            existing_texts.add(item.get("text", "").lower())

    for name in task_names:
        if options["validate"]:
            validation_error = _validate_task_name(name)
            if validation_error:
                errors.append(validation_error)
                skipped += 1
                continue

        if options["skip_duplicates"] and name.lower() in existing_texts:
            skipped += 1
            continue

        if options["max_batch"] and len(added) >= options["max_batch"]:
            break

        item = add_item(name)
        item["category"] = options["category"]
        item["priority"] = options["priority"]
        item["due_date"] = options["due_date"]
        item["owner"] = options["owner"]
        item["tags"] = options["tags"]
        added.append(item)

    return {
        "added": len(added),
        "skipped": skipped,
        "errors": errors,
        "items": added,
    }


def _get_search_options(args, kwargs):
    option_names = [
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
    options = dict(zip(option_names, args))
    options.update(kwargs)
    return options


def _matches_item_search(item, options):
    checks = (
        options.get("query") and options["query"].lower() not in item.get("text", "").lower(),
        options.get("status_filter") and item.get("status") != options["status_filter"],
        options.get("category_filter") and item.get("category") != options["category_filter"],
        options.get("owner_filter") and item.get("owner") != options["owner_filter"],
        options.get("priority_min") is not None and item.get("priority", 0) < options["priority_min"],
        options.get("priority_max") is not None and item.get("priority", 0) > options["priority_max"],
        options.get("created_after") and item.get("created_at", "") < options["created_after"],
        options.get("created_before") and item.get("created_at", "") > options["created_before"],
    )
    if any(checks):
        return False
    return True


def search_items_advanced(*args, **kwargs):
    """Search items with multiple filter criteria."""
    options = _get_search_options(args, kwargs)
    items = load_items()
    results = [item for item in items if _matches_item_search(item, options)]

    if options.get("sort_by"):
        reverse = options.get("sort_order") == "desc"
        results.sort(key=lambda item: item.get(options["sort_by"], ""), reverse=reverse)

    return results


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


def _get_export_options(args, kwargs):
    option_names = [
        "output_path",
        "format_type",
        "status_filter",
        "owner_filter",
        "include_metadata",
        "sort_by",
        "sort_order",
        "date_format",
        "encoding",
        "delimiter",
    ]
    options = dict(zip(option_names, args))
    options.update(kwargs)
    options.setdefault("encoding", "utf-8")
    options.setdefault("delimiter", ",")
    return options


def _write_export_file(filtered, options):
    if options["format_type"] == "json":
        with open(options["output_path"], "w", encoding=options["encoding"]) as file_handle:
            json.dump(filtered, file_handle, indent=2)
        return

    if options["format_type"] == "csv":
        with open(options["output_path"], "w", encoding=options["encoding"]) as file_handle:
            if filtered:
                headers = list(filtered[0].keys())
                file_handle.write(options["delimiter"].join(headers) + "\n")
                for item in filtered:
                    values = [str(item.get(header, "")) for header in headers]
                    file_handle.write(options["delimiter"].join(values) + "\n")
        return

    if options["format_type"] == "txt":
        with open(options["output_path"], "w", encoding=options["encoding"]) as file_handle:
            for item in filtered:
                file_handle.write(f"{item.get('id', '')} | {item.get('text', '')} | {item.get('status', '')}\n")


def export_items_to_file(*args, **kwargs):
    """Export items to a file with various format options."""
    options = _get_export_options(args, kwargs)
    items = load_items()
    filtered = []

    for item in items:
        if options.get("status_filter") and item.get("status") != options["status_filter"]:
            continue
        if options.get("owner_filter") and item.get("owner") != options["owner_filter"]:
            continue
        filtered.append(item)

    if options.get("sort_by"):
        reverse = options.get("sort_order") == "desc"
        filtered.sort(key=lambda item: item.get(options["sort_by"], ""), reverse=reverse)

    _write_export_file(filtered, options)

    return {"exported": len(filtered), "output_path": options["output_path"]}
