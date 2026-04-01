import json
import os
from dataclasses import dataclass
from datetime import datetime

DATA_FILE = "todos.json"


@dataclass
class BulkAddConfig:
    due_date: str | None = None
    owner: str | None = None
    validate: bool = True
    skip_duplicates: bool = False
    max_batch: int = 100
    notify: bool = False
    tags: list | None = None


@dataclass
# pylint: disable=too-many-instance-attributes
class SearchItemsConfig:
    status_filter: str = ""
    category_filter: str = ""
    owner_filter: str = ""
    priority_min: int | None = None
    priority_max: int | None = None
    created_range: tuple | None = None
    sort_by: str = ""
    sort_desc: bool = False


@dataclass
class ExportItemsConfig:
    status_filter: str = ""
    owner_filter: str = ""
    include_metadata: bool = False
    sort_by: str = ""
    reverse_sort: bool = False
    output_options: dict | None = None


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


def bulk_add_items(task_names, category, priority, config=None):
    """Add multiple items in bulk with validation and dedup."""
    cfg = config or BulkAddConfig()
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
        item["due_date"] = cfg.due_date
        item["owner"] = cfg.owner
        item["tags"] = cfg.tags or []
        added.append(item)

    return {
        "added": len(added),
        "skipped": skipped,
        "errors": errors,
        "items": added,
    }


def _item_matches_search(item, query, cfg):
    match = True
    if query:
        match = query.lower() in item.get("text", "").lower()
    if match and cfg.status_filter:
        match = item.get("status") == cfg.status_filter
    if match and cfg.category_filter:
        match = item.get("category") == cfg.category_filter
    if match and cfg.owner_filter:
        match = item.get("owner") == cfg.owner_filter
    if match and cfg.priority_min is not None:
        match = item.get("priority", 0) >= cfg.priority_min
    if match and cfg.priority_max is not None:
        match = item.get("priority", 0) <= cfg.priority_max
    if match and cfg.created_range:
        created_after, created_before = cfg.created_range
        if created_after:
            match = match and item.get("created_at", "") >= created_after
        if created_before:
            match = match and item.get("created_at", "") <= created_before
    return match


def search_items_advanced(query, config=None):
    """Search items with multiple filter criteria."""
    cfg = config or SearchItemsConfig()
    items = load_items()
    results = []

    for item in items:
        if _item_matches_search(item, query, cfg):
            results.append(item)

    if cfg.sort_by:
        results.sort(
            key=lambda x: x.get(cfg.sort_by, ""),
            reverse=cfg.sort_desc,
        )

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


def export_items_to_file(output_path, format_type, config=None):
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
        filtered.sort(
            key=lambda x: x.get(cfg.sort_by, ""),
            reverse=cfg.reverse_sort,
        )

    output_options = cfg.output_options or {}
    encoding = output_options.get("encoding", "utf-8")
    delimiter = output_options.get("delimiter", ",")

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
                f.write(
                    f"{item.get('id', '')} | {item.get('text', '')} | "
                    f"{item.get('status', '')}\n"
                )

    return {"exported": len(filtered), "output_path": output_path}
