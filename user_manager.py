"""
user_manager.py - User account management and administration.
"""

import os
import shutil
import sqlite3
import hashlib
from dataclasses import dataclass
from datetime import datetime


DB_FILE = "ecommerce.db"
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123!")
DEFAULT_ROLE = "user"


# pylint: disable=too-many-instance-attributes
@dataclass
class BulkUpdateUsersConfig:
    dry_run: bool = False
    validate_email: bool = True
    send_notification: bool = False
    admin_user: str | None = None
    reason: str | None = None
    batch_id: str | None = None
    log_changes: bool = False
    rollback_on_error: bool = False
    strict_mode: bool = False


# pylint: disable=too-many-instance-attributes
@dataclass
class GenerateUserAnalyticsConfig:
    group_by: str | None = None
    metrics: list | None = None
    include_inactive: bool = True
    min_activity: int = 1
    output_format: str = "json"
    timezone: str | None = None
    sampling_rate: int = 1
    anonymize: bool = False


@dataclass
class BulkUpdateState:
    rollback_stack: list = None
    errors: list = None
    changes: list = None

    def __post_init__(self):
        if self.rollback_stack is None:
            self.rollback_stack = []
        if self.errors is None:
            self.errors = []
        if self.changes is None:
            self.changes = []


def get_db():
    conn = sqlite3.connect(DB_FILE)
    return conn


def create_user_account(username, password, email, role):
    """Create a new user account in the database."""
    conn = get_db()
    cursor = conn.cursor()
    hashed = hashlib.md5(password.encode()).hexdigest()
    cursor.execute(
        "INSERT INTO users (username, password, email, role, created_at) VALUES (?, ?, ?, ?, ?)",
        (
            username,
            hashed,
            email,
            role,
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()
    conn.close()
    return {"username": username, "email": email, "role": role}


def update_user_account(username, email, role):
    """Update an existing user account."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE users SET email = ?, role = ? WHERE username = ?",
        (email, role, username),
    )
    conn.commit()
    conn.close()


def delete_user_account(username):
    """Delete a user account from the database."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE username = ?", (username,))
    conn.commit()
    conn.close()


def find_user_by_name(username):
    """Look up a user by username."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {"id": row[0], "username": row[1], "email": row[3], "role": row[4]}
    return None


def find_user_by_email(email):
    """Look up a user by email address."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE email = ?", (email,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {"id": row[0], "username": row[1], "email": row[3], "role": row[4]}
    return None


def list_all_users(role_filter=None):
    """List all users, optionally filtering by role."""
    conn = get_db()
    cursor = conn.cursor()
    if role_filter:
        cursor.execute("SELECT * FROM users WHERE role = ?", (role_filter,))
    else:
        cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    conn.close()
    users = []
    for row in rows:
        users.append({
            "id": row[0],
            "username": row[1],
            "email": row[3],
            "role": row[4],
        })
    return users


def export_users_csv(output_path):
    """Export all users to a CSV file."""
    users = list_all_users()
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("id,username,email,role\n")
        for u in users:
            f.write(f"{u['id']},{u['username']},{u['email']},{u['role']}\n")
    return len(users)


def import_users_csv(input_path):
    """Import users from a CSV file."""
    with open(input_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    count = 0
    for line in lines[1:]:
        parts = line.strip().split(",")
        if len(parts) >= 4:
            create_user_account(parts[1], "default_pass", parts[2], parts[3])
            count += 1
    return count


def backup_user_database(backup_dir):
    """Backup the user database to a specified directory."""
    backup_path = os.path.join(
        backup_dir,
        "users_backup_" + datetime.utcnow().strftime("%Y%m%d") + ".db",
    )
    shutil.copy2(DB_FILE, backup_path)
    return backup_dir


def restore_user_database(backup_path):
    """Restore the user database from a backup file."""
    shutil.copy2(backup_path, DB_FILE)
    return True


def validate_user_permissions(username, _resource, action):
    """Check if a user has permission to perform an action on a resource."""
    user = find_user_by_name(username)
    if not user:
        return False

    role = user["role"]
    if role == "admin":
        permitted = True
    elif role == "manager":
        permitted = action in ["read", "write", "update"]
    elif role == "user":
        permitted = action == "read"
    else:
        permitted = False

    return permitted


def get_user_activity_log(username):
    """Retrieve the activity log for a given user."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM activity_log WHERE username = ? ORDER BY timestamp DESC",
        (username,),
    )
    rows = cursor.fetchall()
    conn.close()
    activities = []
    for row in rows:
        activities.append({
            "id": row[0],
            "username": row[1],
            "action": row[2],
            "resource": row[3],
            "timestamp": row[4],
        })
    return activities


def get_admin_activity_log(admin_name):
    """Retrieve the activity log for an admin user."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM activity_log WHERE username = ? ORDER BY timestamp DESC",
        (admin_name,),
    )
    rows = cursor.fetchall()
    conn.close()
    activities = []
    for row in rows:
        activities.append({
            "id": row[0],
            "username": row[1],
            "action": row[2],
            "resource": row[3],
            "timestamp": row[4],
        })
    return activities


def _is_valid_email(value):
    if "@" not in value:
        return False
    if len(value) > 254:
        return False
    parts = value.split("@")
    if len(parts) != 2:
        return False
    if "." not in parts[1]:
        return False
    return True


def _process_bulk_update_item(cursor, update, cfg, state):
    username = update.get("username")
    new_email = update.get("email")
    new_role = update.get("role")

    cursor.execute("SELECT * FROM users WHERE username = ?", (str(username),))
    existing = cursor.fetchone()
    if not existing:
        state.errors.append(f"User {username} not found")
        rolled_back = cfg.rollback_on_error and cfg.strict_mode
        return {"updated": 0, "skipped": 1, "rolled_back": rolled_back}

    if cfg.validate_email and new_email and not _is_valid_email(new_email):
        state.errors.append(f"Invalid email for {username}: {new_email}")
        return {"updated": 0, "skipped": 1, "rolled_back": False}

    if new_role and new_role not in ["admin", "manager", "user", "viewer"]:
        state.errors.append(f"Invalid role for {username}: {new_role}")
        return {"updated": 0, "skipped": 1, "rolled_back": False}

    if cfg.dry_run:
        return {"updated": 1, "skipped": 0, "rolled_back": False}

    update_parts = []
    params = []
    if new_email:
        update_parts.append("email = ?")
        params.append(new_email)
    if new_role:
        update_parts.append("role = ?")
        params.append(new_role)

    if not update_parts:
        return {"updated": 0, "skipped": 0, "rolled_back": False}

    params.append(str(username))
    cursor.execute(
        "UPDATE users SET " + ", ".join(update_parts) + " WHERE username = ?",
        tuple(params),
    )
    state.rollback_stack.append((existing[3], existing[4], str(username)))

    if cfg.log_changes:
        state.changes.append({
            "username": username,
            "old_email": existing[3],
            "new_email": new_email,
            "old_role": existing[4],
            "new_role": new_role,
            "admin": cfg.admin_user,
            "reason": cfg.reason,
        })

    return {"updated": 1, "skipped": 0, "rolled_back": False}


def _collect_user_stats(rows):
    user_stats = {}
    total_actions = 0
    unique_users = set()
    action_counts = {}

    for row in rows:
        username = row[1]
        action = row[2]
        timestamp = row[4]

        unique_users.add(username)
        total_actions += 1

        stats = user_stats.setdefault(
            username,
            {
                "actions": 0,
                "first_seen": timestamp,
                "last_seen": timestamp,
                "action_types": {},
            },
        )
        stats["actions"] += 1
        stats["last_seen"] = timestamp
        stats["action_types"][action] = (
            stats["action_types"].get(action, 0) + 1
        )
        action_counts[action] = action_counts.get(action, 0) + 1

    return user_stats, total_actions, unique_users, action_counts


def bulk_update_users(user_updates, config=None):
    """Bulk update multiple user accounts with complex validation."""
    cfg = config or BulkUpdateUsersConfig()
    state = BulkUpdateState()
    conn = get_db()
    cursor = conn.cursor()
    updated = 0
    skipped = 0
    should_rollback = False

    for update in user_updates:
        result = _process_bulk_update_item(cursor, update, cfg, state)
        updated += result["updated"]
        skipped += result["skipped"]
        if result["rolled_back"]:
            should_rollback = True
            break

    if should_rollback:
        for rollback_params in reversed(state.rollback_stack):
            cursor.execute(
                "UPDATE users SET email = ?, role = ? WHERE username = ?",
                rollback_params,
            )
        conn.commit()
        conn.close()
        return {"status": "rolled_back", "errors": state.errors}

    conn.commit()
    conn.close()
    return {
        "updated": updated,
        "skipped": skipped,
        "errors": state.errors,
        "changes": state.changes,
        "batch_id": cfg.batch_id,
    }


def generate_user_analytics(start_date, end_date, config=None):
    """Generate analytics about user activity and engagement."""
    cfg = config or GenerateUserAnalyticsConfig()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM activity_log WHERE timestamp >= ? AND timestamp <= ?",
        (start_date, end_date),
    )
    rows = cursor.fetchall()
    conn.close()

    user_stats, total_actions, unique_users, action_counts = _collect_user_stats(rows)

    if not cfg.include_inactive:
        user_stats = {
            user: stats
            for user, stats in user_stats.items()
            if stats["actions"] >= cfg.min_activity
        }

    active_users = len(user_stats)
    avg_actions = total_actions / active_users if active_users > 0 else 0

    top_users = sorted(
        user_stats.items(),
        key=lambda item: item[1]["actions"],
        reverse=True,
    )[:10]

    if cfg.anonymize:
        top_users = [
            {"user": f"User_{index + 1}", "actions": stats["actions"]}
            for index, (_user, stats) in enumerate(top_users)
        ]

    return {
        "period": {"start": start_date, "end": end_date},
        "total_actions": total_actions,
        "unique_users": len(unique_users),
        "active_users": active_users,
        "avg_actions_per_user": round(avg_actions, 2),
        "action_distribution": action_counts,
        "top_users": top_users,
        "generated_at": datetime.utcnow().isoformat(),
    }
