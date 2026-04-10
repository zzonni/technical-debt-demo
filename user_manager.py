"""
user_manager.py - User account management and administration.
"""

import os
import shutil
import sqlite3
import hashlib
from datetime import datetime, timezone as dt_timezone


DB_FILE = "ecommerce.db"
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD")
DEFAULT_ROLE = "user"

_BULK_UPDATE_KEYS = [
    "dry_run",
    "validate_email",
    "send_notification",
    "admin_user",
    "reason",
    "batch_id",
    "log_changes",
    "rollback_on_error",
    "strict_mode",
]

_ANALYTICS_KEYS = [
    "group_by",
    "metrics",
    "include_inactive",
    "min_activity",
    "output_format",
    "timezone",
    "sampling_rate",
    "anonymize",
]

_UNSET = object()

_PERMISSIONS = {
    "admin": {"read", "write", "update", "delete"},
    "manager": {"read", "write", "update"},
    "user": {"read"},
}


def _utc_now():
    return datetime.now(dt_timezone.utc)


def _resolve_options(option_values, keys, args, kwargs):
    options = {}
    if option_values is _UNSET:
        positional_values = list(args)
    elif isinstance(option_values, dict):
        options.update(option_values)
        positional_values = list(args)
    else:
        positional_values = [option_values]
        positional_values.extend(args)

    for key, value in zip(keys, positional_values):
        options[key] = value

    for key in keys:
        if key in kwargs:
            options[key] = kwargs[key]

    return options


def _read_activity_log(username):
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


def _is_valid_email_address(new_email):
    if "@" not in new_email:
        return False, f"Invalid email: {new_email}"
    if len(new_email) > 254:
        return False, "Email too long"
    parts = new_email.split("@")
    if len(parts) != 2:
        return False, f"Malformed email: {new_email}"
    if "." not in parts[1]:
        return False, f"Invalid domain in email: {new_email}"
    return True, None


def _append_email_error(errors, username, new_email, error_message):
    if error_message.startswith("Email too long"):
        errors.append(f"Email too long for {username}")
    elif error_message.startswith("Invalid domain"):
        errors.append(f"Invalid domain in email for {username}")
    elif error_message.startswith("Malformed email"):
        errors.append(f"Malformed email for {username}")
    else:
        errors.append(f"Invalid email for {username}: {new_email}")


def _rollback_updates(cursor, rollback_stack):
    for rollback_values in reversed(rollback_stack):
        cursor.execute(
            "UPDATE users SET email = ?, role = ? WHERE username = ?",
            rollback_values,
        )


def _fetch_existing_user(cursor, username):
    cursor.execute(
        "SELECT * FROM users WHERE username = ?",
        (str(username),),
    )
    return cursor.fetchone()


def _is_valid_role(new_role):
    return new_role in ["admin", "manager", "user", "viewer"]


def _update_user_record(cursor, username, new_email, new_role):
    update_values = []
    update_parts = []
    if new_email:
        update_parts.append("email = ?")
        update_values.append(new_email)
    if new_role:
        update_parts.append("role = ?")
        update_values.append(new_role)
    if not update_parts:
        return False

    cursor.execute(
        f"UPDATE users SET {', '.join(update_parts)} WHERE username = ?",
        (*update_values, str(username)),
    )
    return True


def _record_change(changes, username, existing, new_email, new_role, resolved):
    changes.append({
        "username": username,
        "old_email": existing[3],
        "new_email": new_email,
        "old_role": existing[4],
        "new_role": new_role,
        "admin": resolved.get("admin_user"),
        "reason": resolved.get("reason"),
    })


def _process_user_update(cursor, update, resolved, rollback_stack, errors, changes):
    username = update.get("username")
    new_email = update.get("email")
    new_role = update.get("role")
    existing = _fetch_existing_user(cursor, username)

    if not existing:
        errors.append(f"User {username} not found")
        return {"updated": 0, "skipped": 1, "rolled_back": False}

    if resolved.get("validate_email") and new_email:
        is_valid, error_message = _is_valid_email_address(new_email)
        if not is_valid:
            _append_email_error(errors, username, new_email, error_message)
            return {"updated": 0, "skipped": 1, "rolled_back": False}

    if new_role and not _is_valid_role(new_role):
        errors.append(f"Invalid role for {username}: {new_role}")
        return {"updated": 0, "skipped": 1, "rolled_back": False}

    if resolved.get("dry_run"):
        return {"updated": 1, "skipped": 0, "rolled_back": False}

    if _update_user_record(cursor, username, new_email, new_role):
        rollback_stack.append((str(existing[3]), str(existing[4]), str(username)))
        if resolved.get("log_changes"):
            _record_change(changes, username, existing, new_email, new_role, resolved)
        return {"updated": 1, "skipped": 0, "rolled_back": False}

    return {"updated": 0, "skipped": 0, "rolled_back": False}


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

        stats = user_stats.setdefault(username, {
            "actions": 0,
            "first_seen": timestamp,
            "last_seen": timestamp,
            "action_types": {},
        })
        stats["actions"] += 1
        stats["last_seen"] = timestamp
        stats["action_types"][action] = stats["action_types"].get(action, 0) + 1
        action_counts[action] = action_counts.get(action, 0) + 1

    return user_stats, total_actions, unique_users, action_counts


def _filter_active_users(user_stats, include_inactive, min_activity):
    if include_inactive:
        return user_stats

    return {
        user: stats
        for user, stats in user_stats.items()
        if stats["actions"] >= min_activity
    }


def _build_top_users(user_stats, anonymize):
    top_users = sorted(
        user_stats.items(),
        key=lambda item: item[1]["actions"],
        reverse=True,
    )[:10]

    if not anonymize:
        return top_users

    return [
        {"user": f"User_{index + 1}", "actions": stats["actions"]}
        for index, (_, stats) in enumerate(top_users)
    ]


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
        (username, hashed, email, role, _utc_now().isoformat()),
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


def find_user_by_email(email):
    """Look up a user by email address."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE email = ?", (email,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {"id": row[0], "username": row[1], "email": row[3], "role": row[4]}


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
    with open(output_path, "w") as f:
        f.write("id,username,email,role\n")
        for u in users:
            f.write(f"{u['id']},{u['username']},{u['email']},{u['role']}\n")
    return len(users)


def import_users_csv(input_path):
    """Import users from a CSV file."""
    with open(input_path, "r") as f:
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
    backup_path = os.path.join(backup_dir, f"users_backup_{_utc_now().strftime('%Y%m%d')}.db")
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
    return action in _PERMISSIONS.get(user["role"], set())


def get_user_activity_log(username):
    """Retrieve the activity log for a given user."""
    return _read_activity_log(username)


def get_admin_activity_log(admin_name):
    """Retrieve the activity log for an admin user."""
    return _read_activity_log(admin_name)


def bulk_update_users(user_updates, options=_UNSET, *args, **kwargs):
    """Bulk update multiple user accounts with complex validation."""
    resolved = _resolve_options(options, _BULK_UPDATE_KEYS, args, kwargs)
    conn = get_db()
    cursor = conn.cursor()
    updated = 0
    skipped = 0
    errors = []
    changes = []
    rollback_stack = []

    for update in user_updates:
        result = _process_user_update(cursor, update, resolved, rollback_stack, errors, changes)
        updated += result["updated"]
        skipped += result["skipped"]

        if result["skipped"] and resolved.get("rollback_on_error") and resolved.get("strict_mode"):
                _rollback_updates(cursor, rollback_stack)
                conn.commit()
                conn.close()
                return {"status": "rolled_back", "errors": errors}

    conn.commit()
    conn.close()
    return {
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "changes": changes,
        "batch_id": resolved.get("batch_id"),
    }


def generate_user_analytics(start_date, end_date, options=_UNSET, *args, **kwargs):
    """Generate analytics about user activity and engagement."""
    resolved = _resolve_options(options, _ANALYTICS_KEYS, args, kwargs)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM activity_log WHERE timestamp >= ? AND timestamp <= ?",
        (start_date, end_date),
    )
    rows = cursor.fetchall()
    conn.close()

    user_stats, total_actions, unique_users, action_counts = _collect_user_stats(rows)
    user_stats = _filter_active_users(
        user_stats,
        resolved.get("include_inactive", False),
        resolved.get("min_activity", 0),
    )

    active_users = len(user_stats)
    avg_actions = total_actions / active_users if active_users > 0 else 0
    top_users = _build_top_users(user_stats, resolved.get("anonymize", False))

    return {
        "period": {"start": start_date, "end": end_date},
        "total_actions": total_actions,
        "unique_users": len(unique_users),
        "active_users": active_users,
        "avg_actions_per_user": round(avg_actions, 2),
        "action_distribution": action_counts,
        "top_users": top_users,
        "generated_at": _utc_now().isoformat(),
    }
