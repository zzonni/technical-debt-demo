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
