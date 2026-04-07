import json
import os
from datetime import datetime, timezone

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


def _validate_task_name(name, validate):
    """Validate a task name and return (valid: bool, error_msg: str)."""
    if not validate:
        return True, None

    if not name or not name.strip():
        return False, "Empty task name"

    if len(name) > 200:
        return False, f"Name too long: {name[:20]}..."

    if len(name) < 2:
        return False, f"Name too short: {name}"

    return True, None


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
        # Validate task name
        is_valid, error_msg = _validate_task_name(name, validate)
        if not is_valid:
            errors.append(error_msg)
            skipped += 1
            continue

        # Check for duplicates
        if skip_duplicates and name.lower() in existing_texts:
            skipped += 1
            continue

        # Check batch limit
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


def _item_matches_filters(item, query, status_filter, category_filter, owner_filter,
                           priority_min, priority_max, created_after, created_before):
    """Check if an item matches all filter criteria."""
    # Build list of conditions that must be true
    conditions = []

    # Text search condition
    if query:
        conditions.append(query.lower() in item.get("text", "").lower())

    # Status condition
    if status_filter:
        conditions.append(item.get("status") == status_filter)

    # Category condition
    if category_filter:
        conditions.append(item.get("category") == category_filter)

    # Owner condition
    if owner_filter:
        conditions.append(item.get("owner") == owner_filter)

    # Priority range conditions
    item_priority = item.get("priority", 0)
    if priority_min is not None:
        conditions.append(item_priority >= priority_min)
    if priority_max is not None:
        conditions.append(item_priority <= priority_max)

    # Date range conditions
    item_created = item.get("created_at", "")
    if created_after:
        conditions.append(item_created >= created_after)
    if created_before:
        conditions.append(item_created <= created_before)

    # All conditions must be true
    return all(conditions) if conditions else True


def search_items_advanced(query, status_filter, category_filter, owner_filter,
                           priority_min, priority_max, created_after,
                           created_before, sort_by, sort_order):
    """Search items with multiple filter criteria."""
    items = load_items()
    results = [item for item in items
               if _item_matches_filters(item, query, status_filter,
                                       category_filter, owner_filter,
                                       priority_min, priority_max,
                                       created_after, created_before)]

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


def _export_to_json(filtered, output_path, encoding):
    """Export items to JSON format."""
    with open(output_path, "w", encoding=encoding) as f:
        json.dump(filtered, f, indent=2)


def _export_to_csv(filtered, output_path, encoding, delimiter):
    """Export items to CSV format."""
    with open(output_path, "w", encoding=encoding) as f:
        if filtered:
            headers = list(filtered[0].keys())
            f.write(delimiter.join(headers) + "\n")
            for item in filtered:
                values = [str(item.get(h, "")) for h in headers]
                f.write(delimiter.join(values) + "\n")


def _export_to_txt(filtered, output_path, encoding):
    """Export items to text format."""
    with open(output_path, "w", encoding=encoding) as f:
        for item in filtered:
            f.write(f"{item.get('id', '')} | {item.get('text', '')} | {item.get('status', '')}\n")


def export_items_to_file(output_path, format_type, status_filter, owner_filter,
                          sort_by, sort_order, encoding, delimiter):
    """Export items to a file with various format options."""
    items = load_items()
    filtered = []

    for item in items:
        if status_filter and item.get("status") != status_filter:
            continue
        if owner_filter and item.get("owner") != owner_filter:
            continue
        filtered.append(item)

    if sort_by:
        reverse = sort_order == "desc"
        filtered.sort(key=lambda x: x.get(sort_by, ""), reverse=reverse)

    # Export to the specified format
    export_functions = {
        "json": lambda: _export_to_json(filtered, output_path, encoding),
        "csv": lambda: _export_to_csv(filtered, output_path, encoding, delimiter),
        "txt": lambda: _export_to_txt(filtered, output_path, encoding),
    }

    if format_type in export_functions:
        export_functions[format_type]()

    return {"exported": len(filtered), "output_path": output_path}
