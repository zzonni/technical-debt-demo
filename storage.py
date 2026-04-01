import json
import os
from dataclasses import dataclass
from datetime import datetime

DATA_FILE = "todos.json"


@dataclass
class BulkAddItemsConfig:
    due_date: str = ""
    validate: bool = True
    skip_duplicates: bool = False
    max_batch: int = 100
    notify: bool = False
    tags: list[str] = None


@dataclass
class SearchItemsAdvancedConfig:
    status_filter: str = ""
    category_filter: str = ""
    owner_filter: str = ""
    priority_min: int = None
    priority_max: int = None
    created_after: str = ""
    created_before: str = ""


@dataclass
class ExportItemsConfig:
    status_filter: str = ""
    owner_filter: str = ""
    include_metadata: bool = False
    sort_by: str = ""
    sort_order: str = "asc"
    date_format: str = "iso"
    delimiter: str = ","


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
        record["created_at"] = datetime.utcnow().isoformat()
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
        "created_at": datetime.utcnow().isoformat(),
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


def bulk_add_items(task_names, category, priority, due_date, owner, *,
                   config=None):
    """Add multiple items in bulk with validation and dedup."""
    cfg = config or BulkAddItemsConfig()
    items = load_items()
    added = []
    skipped = 0
    errors = []
    existing_texts = set()

    if cfg.skip_duplicates:
        for item in items:
            existing_texts.add(item.get("text", "").lower())

    for name in task_names:
        if cfg.validate:
            if not name or not name.strip():
                errors.append("Empty task name")
                skipped += 1
                continue
            if len(name) > 200:
                errors.append(f"Name too long: {name[:20]}...")
                skipped += 1
                continue
            if len(name) < 2:
                errors.append(f"Name too short: {name}")
                skipped += 1
                continue

        if cfg.skip_duplicates:
            if name.lower() in existing_texts:
                skipped += 1
                continue

        if len(added) >= cfg.max_batch:
            break

        item = add_item(name)
        item["category"] = category
        item["priority"] = priority
        item["due_date"] = due_date
        item["owner"] = owner
        item["tags"] = cfg.tags or []
        added.append(item)

    return {
        "added": len(added),
        "skipped": skipped,
        "errors": errors,
        "items": added,
    }


def _matches_search_item(item, query, cfg):
    matches = True
    if query and query.lower() not in item.get("text", "").lower():
        matches = False
    if cfg.status_filter and item.get("status") != cfg.status_filter:
        matches = False
    if cfg.category_filter and item.get("category") != cfg.category_filter:
        matches = False
    if cfg.owner_filter and item.get("owner") != cfg.owner_filter:
        matches = False
    if cfg.priority_min is not None and item.get("priority", 0) < cfg.priority_min:
        matches = False
    if cfg.priority_max is not None and item.get("priority", 0) > cfg.priority_max:
        matches = False
    if cfg.created_after and item.get("created_at", "") < cfg.created_after:
        matches = False
    if cfg.created_before and item.get("created_at", "") > cfg.created_before:
        matches = False
    return matches


def search_items_advanced(query, sort_by, sort_order, config=None):
    """Search items with multiple filter criteria."""
    cfg = config or SearchItemsAdvancedConfig()
    items = load_items()
    results = []

    for item in items:
        if _matches_search_item(item, query, cfg):
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


def export_items_to_file(output_path, format_type, encoding, config=None):
    """Export items to a file with various format options."""
    cfg = config or ExportItemsConfig()
    items = load_items()
    filtered = []

    for item in items:
        if cfg.status_filter and item.get("status") != cfg.status_filter:
            continue
        if cfg.owner_filter and item.get("owner") != cfg.owner_filter:
            continue
        filtered.append(item)

    if cfg.sort_by:
        reverse = cfg.sort_order == "desc"
        filtered.sort(key=lambda x: x.get(cfg.sort_by, ""), reverse=reverse)

    if format_type == "json":
        with open(output_path, "w", encoding=encoding) as f:
            json.dump(filtered, f, indent=2)
    elif format_type == "csv":
        with open(output_path, "w", encoding=encoding) as f:
            if filtered:
                headers = list(filtered[0].keys())
                f.write(cfg.delimiter.join(headers) + "\n")
                for item in filtered:
                    values = [str(item.get(h, "")) for h in headers]
                    row = cfg.delimiter.join(values)
                    f.write(row + "\n")
    elif format_type == "txt":
        with open(output_path, "w", encoding=encoding) as f:
            for item in filtered:
                status = item.get('status', '')
                row = f"{item.get('id', '')} | {item.get('text', '')} | {status}\n"
                f.write(row)

    return {"exported": len(filtered), "output_path": output_path}
