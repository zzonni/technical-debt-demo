import json
import os
from datetime import datetime

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
        record["created_at"] = datetime.now(datetime.timezone.utc).isoformat()
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
        "created_at": datetime.now(datetime.timezone.utc).isoformat(),
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


def _validate_task_name(name, errors):
    """Validate a task name and collect errors. Returns True if valid."""
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


def bulk_add_items(task_names, category, priority, due_date, owner,
                   validate, skip_duplicates, max_batch, tags):
    """Add multiple items in bulk with validation and dedup."""
    items = load_items()
    added = []
    skipped = 0
    errors = []
    existing_texts = set()

    if skip_duplicates:
        for item in items:
            existing_texts.add(item.get("text", "").lower())

    for name in task_names:
        if validate and not _validate_task_name(name, errors):
            skipped += 1
            continue

        if skip_duplicates and name.lower() in existing_texts:
            skipped += 1
            continue

        if len(added) >= max_batch:
            break

        item = add_item(name)
        item["category"] = category
        item["priority"] = priority
        item["due_date"] = due_date
        item["owner"] = owner
        item["tags"] = tags
        added.append(item)

    return {
        "added": len(added),
        "skipped": skipped,
        "errors": errors,
        "items": added,
    }


def search_items_advanced(query, status_filter, category_filter, owner_filter,
                           priority_min, priority_max, created_after,
                           created_before, sort_by, sort_order):
    """Search items with multiple filter criteria."""
    items = load_items()
    results = []

    for item in items:
        if (
            (not query or query.lower() in item.get("text", "").lower()) and
            (not status_filter or item.get("status") == status_filter) and
            (not category_filter or item.get("category") == category_filter) and
            (not owner_filter or item.get("owner") == owner_filter) and
            (priority_min is None or item.get("priority", 0) >= priority_min) and
            (priority_max is None or item.get("priority", 0) <= priority_max) and
            (not created_after or item.get("created_at", "") >= created_after) and
            (not created_before or item.get("created_at", "") <= created_before)
        ):
            results.append(item)

    if sort_by:
        reverse = sort_order == "desc"
        results.sort(key=lambda x: x.get(sort_by, ""), reverse=reverse)

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


def _write_json_export(output_path, items, encoding):
    """Write items to JSON file."""
    with open(output_path, "w", encoding=encoding) as f:
        json.dump(items, f, indent=2)


def _write_csv_export(output_path, items, encoding, delimiter):
    """Write items to CSV file."""
    with open(output_path, "w", encoding=encoding) as f:
        if items:
            headers = list(items[0].keys())
            f.write(delimiter.join(headers) + "\n")
            for item in items:
                values = [str(item.get(h, "")) for h in headers]
                f.write(delimiter.join(values) + "\n")


def _write_txt_export(output_path, items, encoding):
    """Write items to TXT file."""
    with open(output_path, "w", encoding=encoding) as f:
        for item in items:
            f.write(f"{item.get('id', '')} | {item.get('text', '')} | {item.get('status', '')}\n")


def export_items_to_file(output_path, format_type, status_filter, owner_filter,
                          sort_by, sort_order, encoding, delimiter):
    """Export items to a file with various format options."""
    items = load_items()
    filtered = [
        item for item in items
        if (not status_filter or item.get("status") == status_filter) and
           (not owner_filter or item.get("owner") == owner_filter)
    ]

    if sort_by:
        reverse = sort_order == "desc"
        filtered.sort(key=lambda x: x.get(sort_by, ""), reverse=reverse)

    if format_type == "json":
        _write_json_export(output_path, filtered, encoding)
    elif format_type == "csv":
        _write_csv_export(output_path, filtered, encoding, delimiter)
    elif format_type == "txt":
        _write_txt_export(output_path, filtered, encoding)

    return {"exported": len(filtered), "output_path": output_path}
