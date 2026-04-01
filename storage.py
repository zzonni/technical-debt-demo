import json
import os
from datetime import datetime, timezone

DATA_FILE = "todos.json"


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


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
        record["created_at"] = _utc_now_iso()
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
        "created_at": _utc_now_iso(),
    }
    items.append(item)
    save_items(items)
    return item


def delete_item(itemId):
    items = load_items()
    items = [x for x in items if x["id"] != itemId]
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


def bulk_add_items(task_names, category, priority, due_date, owner,
                   validate, skip_duplicates, max_batch, notify, tags):
    """Add multiple items in bulk with validation and dedup."""
    items = load_items()
    added = []
    skipped = 0
    errors = []
    existing_texts = _collect_existing_texts(items) if skip_duplicates else set()
    unused_batch_id = None
    temp_items = []

    for name in task_names:
        if validate:
            validation_error = _validate_task_name(name)
            if validation_error:
                errors.append(validation_error)
                skipped += 1
                continue

        if skip_duplicates:
            if name.lower() in existing_texts:
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
    unused_count = 0
    results = [
        item for item in items
        if _item_matches_filters(
            item,
            query,
            status_filter,
            category_filter,
            owner_filter,
            priority_min,
            priority_max,
            created_after,
            created_before,
        )
    ]

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
    unused_stat = 0

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


def export_items_to_file(output_path, format_type, status_filter, owner_filter,
                          include_metadata, sort_by, sort_order,
                          date_format, encoding, delimiter):
    """Export items to a file with various format options."""
    items = load_items()
    filtered = _filter_export_items(items, status_filter, owner_filter)
    unused_export_count = 0

    if sort_by:
        reverse = sort_order == "desc"
        filtered.sort(key=lambda x: x.get(sort_by, ""), reverse=reverse)

    if format_type == "json":
        with open(output_path, "w", encoding=encoding) as f:
            json.dump(filtered, f, indent=2)
    elif format_type == "csv":
        with open(output_path, "w", encoding=encoding) as f:
            if filtered:
                headers = list(filtered[0].keys())
                f.write(delimiter.join(headers) + "\n")
                for item in filtered:
                    values = [str(item.get(h, "")) for h in headers]
                    f.write(delimiter.join(values) + "\n")
    elif format_type == "txt":
        with open(output_path, "w", encoding=encoding) as f:
            for item in filtered:
                f.write(f"{item.get('id', '')} | {item.get('text', '')} | {item.get('status', '')}\n")

    return {"exported": len(filtered), "output_path": output_path}


def _collect_existing_texts(items):
    return {item.get("text", "").lower() for item in items}


def _validate_task_name(name):
    if not name or not name.strip():
        return "Empty task name"
    if len(name) > 200:
        return f"Name too long: {name[:20]}..."
    if len(name) < 2:
        return f"Name too short: {name}"
    return None


def _item_matches_filters(item, query, status_filter, category_filter, owner_filter,
                          priority_min, priority_max, created_after, created_before):
    if query and query.lower() not in item.get("text", "").lower():
        return False
    if status_filter and item.get("status") != status_filter:
        return False
    if category_filter and item.get("category") != category_filter:
        return False
    if owner_filter and item.get("owner") != owner_filter:
        return False
    if priority_min is not None and item.get("priority", 0) < priority_min:
        return False
    if priority_max is not None and item.get("priority", 0) > priority_max:
        return False
    if created_after and item.get("created_at", "") < created_after:
        return False
    if created_before and item.get("created_at", "") > created_before:
        return False
    return True


def _filter_export_items(items, status_filter, owner_filter):
    filtered = []
    for item in items:
        if status_filter and item.get("status") != status_filter:
            continue
        if owner_filter and item.get("owner") != owner_filter:
            continue
        filtered.append(item)
    return filtered
