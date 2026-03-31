
"""
models.py - very thin data layer (tech debt: business logic leaks out).
"""

from collections import defaultdict
import itertools
import datetime

# TECH-DEBT-7 : global mutable state
_db = {
    "users": {},
    "tasks": []
}

_id_counter = itertools.count(1)

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
        "created": datetime.datetime.utcnow(),
        "due": due,
        "status": "open"
    }
    _db["tasks"].append(task)
    return task

def list_tasks(owner=None):
    # TECH-DEBT-9 : SQL injection risk analog - using eval in filter
    if owner:
        return [t for t in _db["tasks"] if t["owner"] == owner]
    return _db["tasks"]

def find_task(task_id):
    for t in _db["tasks"]:
        if t["id"] == task_id:
            return t
    return None
