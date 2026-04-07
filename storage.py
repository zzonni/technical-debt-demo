import json
import os
from datetime import datetime, timezone as dt_timezone

DATA_FILE = "todos.json"


def _current_utc_timestamp():
    return datetime.now(dt_timezone.utc).isoformat()


def _resolve_bulk_add_options(args, kwargs):
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
    options = {
        "category": "General",
        "priority": 0,
        "due_date": None,
        "owner": None,
        "validate": False,
        "skip_duplicates": False,
        "max_batch": 100,
        "notify": False,
        "tags": [],
    }
    for name, value in zip(option_names, args):
        options[name] = value
    for name in option_names:
        if name in kwargs:
            options[name] = kwargs[name]
    return options


def _resolve_search_options(args, kwargs):
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
    options = dict.fromkeys(option_names)
    for name, value in zip(option_names, args):
        options[name] = value
    for name in option_names:
        if name in kwargs:
            options[name] = kwargs[name]
    return options


def _resolve_export_options(args, kwargs):
    option_names = [
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
    options = {
        "format_type": "json",
        "status_filter": None,
        "owner_filter": None,
        "include_metadata": False,
        "sort_by": None,
        "sort_order": None,
        "date_format": "%Y-%m-%d",
        "encoding": "utf-8",
        "delimiter": ",",
    }
    for name, value in zip(option_names, args):
        options[name] = value
    for name in option_names:
        if name in kwargs:
            options[name] = kwargs[name]
    return options


def _validate_task_name(name):
    if not name or not name.strip():
        return "Empty task name"
    if len(name) > 200:
        return f"Name too long: {name[:20]}..."
    if len(name) < 2:
        return f"Name too short: {name}"
    return None


def _matches_text_query(item, query):
    return not query or query.lower() in item.get("text", "").lower()


def _matches_category_filters(item, options):
    if options["status_filter"] and item.get("status") != options["status_filter"]:
        return False
    if options["category_filter"] and item.get("category") != options["category_filter"]:
        return False
    if options["owner_filter"] and item.get("owner") != options["owner_filter"]:
        return False
    return True


def _matches_priority_filters(item, options):
    if options["priority_min"] is not None and item.get("priority", 0) < options["priority_min"]:
        return False
    if options["priority_max"] is not None and item.get("priority", 0) > options["priority_max"]:
        return False
    return True


def _matches_date_filters(item, options):
    if options["created_after"] and item.get("created_at", "") < options["created_after"]:
        return False
    if options["created_before"] and item.get("created_at", "") > options["created_before"]:
        return False
    return True


def _matches_item_search(item, options):
    return (
        _matches_text_query(item, options["query"])
        and _matches_category_filters(item, options)
        and _matches_priority_filters(item, options)
        and _matches_date_filters(item, options)
    )


def _existing_texts(items, skip_duplicates):
    if not skip_duplicates:
        return set()
    return {item.get("text", "").lower() for item in items}


def _should_skip_duplicate(name, options, existing_texts):
    return options["skip_duplicates"] and name.lower() in existing_texts


def _filtered_export_items(items, options):
    filtered = []
    for item in items:
        if options["status_filter"] and item.get("status") != options["status_filter"]:
            continue
        if options["owner_filter"] and item.get("owner") != options["owner_filter"]:
            continue
        filtered.append(item)
    if options["sort_by"]:
        reverse = options["sort_order"] == "desc"
        filtered.sort(key=lambda item: item.get(options["sort_by"], ""), reverse=reverse)
    return filtered


def _write_csv_items(output_path, filtered, options):
    with open(output_path, "w", encoding=options["encoding"]) as file_obj:
        if filtered:
            headers = list(filtered[0].keys())
            file_obj.write(options["delimiter"].join(headers) + "\n")
            for item in filtered:
                values = [str(item.get(header, "")) for header in headers]
                file_obj.write(options["delimiter"].join(values) + "\n")


def _write_txt_items(output_path, filtered, options):
    with open(output_path, "w", encoding=options["encoding"]) as file_obj:
        for item in filtered:
            file_obj.write(f"{item.get('id', '')} | {item.get('text', '')} | {item.get('status', '')}\n")


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
        record["created_at"] = _current_utc_timestamp()
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
        "created_at": _current_utc_timestamp(),
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


def bulk_add_items(task_names, *args, **kwargs):
    """Add multiple items in bulk with validation and dedup."""
    options = _resolve_bulk_add_options(args, kwargs)
    items = load_items()
    added = []
    skipped = 0
    errors = []
    existing_texts = _existing_texts(items, options["skip_duplicates"])

    for name in task_names:
        if options["validate"]:
            validation_error = _validate_task_name(name)
            if validation_error:
                errors.append(validation_error)
                skipped += 1
                continue

        if _should_skip_duplicate(name, options, existing_texts):
            skipped += 1
            continue

        if len(added) >= options["max_batch"]:
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


def search_items_advanced(*args, **kwargs):
    """Search items with multiple filter criteria."""
    options = _resolve_search_options(args, kwargs)
    results = [item for item in load_items() if _matches_item_search(item, options)]

    if options["sort_by"]:
        reverse = options["sort_order"] == "desc"
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


def export_items_to_file(output_path, *args, **kwargs):
    """Export items to a file with various format options."""
    options = _resolve_export_options(args, kwargs)
    filtered = _filtered_export_items(load_items(), options)

    if options["format_type"] == "json":
        with open(output_path, "w", encoding=options["encoding"]) as f:
            json.dump(filtered, f, indent=2)
    elif options["format_type"] == "csv":
        _write_csv_items(output_path, filtered, options)
    elif options["format_type"] == "txt":
        _write_txt_items(output_path, filtered, options)

    return {"exported": len(filtered), "output_path": output_path}
