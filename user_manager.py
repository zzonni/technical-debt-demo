"""
user_manager.py - User account management and administration.
"""

import os
import sqlite3
import hashlib
import shutil
from datetime import datetime, timezone


DB_FILE = "ecommerce.db"
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD")
DEFAULT_ROLE = "user"


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
        (username, hashed, email, role, datetime.now(timezone.utc).isoformat()),
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
    os.makedirs(backup_dir, exist_ok=True)
    backup_path = os.path.join(
        backup_dir,
        f"users_backup_{datetime.now(timezone.utc).strftime('%Y%m%d')}.db",
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
    role_permissions = {
        "admin": {"read", "write", "update", "delete"},
        "manager": {"read", "write", "update"},
        "user": {"read"},
    }
    return action in role_permissions.get(user["role"], set())


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


def _get_bulk_update_options(args, kwargs):
    option_names = [
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
    options = dict(zip(option_names, args))
    options.update(kwargs)
    options.setdefault("dry_run", False)
    options.setdefault("validate_email", False)
    options.setdefault("admin_user", None)
    options.setdefault("reason", None)
    options.setdefault("batch_id", None)
    options.setdefault("log_changes", False)
    options.setdefault("rollback_on_error", False)
    options.setdefault("strict_mode", False)
    return options


def _validate_email_address(username, new_email, errors):
    if not new_email:
        return True
    if "@" not in new_email:
        errors.append(f"Invalid email for {username}: {new_email}")
        return False
    if len(new_email) > 254:
        errors.append(f"Email too long for {username}")
        return False
    parts = new_email.split("@")
    if len(parts) != 2:
        errors.append(f"Malformed email for {username}")
        return False
    if "." not in parts[1]:
        errors.append(f"Invalid domain in email for {username}")
        return False
    return True


def _rollback_user_updates(cursor, rollback_stack):
    for username, old_email, old_role in reversed(rollback_stack):
        cursor.execute(
            "UPDATE users SET email = ?, role = ? WHERE username = ?",
            (old_email, old_role, username),
        )


def _process_user_update(cursor, update, options, errors, rollback_stack, changes):
    username = update.get("username")
    new_email = update.get("email")
    new_role = update.get("role")

    cursor.execute("SELECT * FROM users WHERE username = ?", (str(username),))
    existing = cursor.fetchone()
    if not existing:
        errors.append(f"User {username} not found")
        return "missing", None

    if options["validate_email"] and not _validate_email_address(username, new_email, errors):
        return "invalid", None

    if new_role and new_role not in ["admin", "manager", "user", "viewer"]:
        errors.append(f"Invalid role for {username}: {new_role}")
        return "invalid", None

    if options["dry_run"]:
        return "updated", None

    update_parts = []
    values = []
    if new_email:
        update_parts.append("email = ?")
        values.append(new_email)
    if new_role:
        update_parts.append("role = ?")
        values.append(new_role)
    if not update_parts:
        return "skipped", None

    values.append(str(username))
    cursor.execute(
        f"UPDATE users SET {', '.join(update_parts)} WHERE username = ?",
        tuple(values),
    )
    rollback_stack.append((str(username), existing[3], existing[4]))

    if options["log_changes"]:
        changes.append({
            "username": username,
            "old_email": existing[3],
            "new_email": new_email,
            "old_role": existing[4],
            "new_role": new_role,
            "admin": options["admin_user"],
            "reason": options["reason"],
        })
    return "updated", existing


def bulk_update_users(user_updates, *args, **kwargs):
    """Bulk update multiple user accounts with complex validation."""
    options = _get_bulk_update_options(args, kwargs)
    conn = get_db()
    cursor = conn.cursor()
    updated = 0
    skipped = 0
    errors = []
    changes = []
    rollback_stack = []

    for update in user_updates:
        result, _ = _process_user_update(cursor, update, options, errors, rollback_stack, changes)
        if result == "missing":
            skipped += 1
            if options["rollback_on_error"] and options["strict_mode"]:
                _rollback_user_updates(cursor, rollback_stack)
                conn.commit()
                conn.close()
                return {"status": "rolled_back", "errors": errors}
            continue

        if result == "invalid":
            skipped += 1
            continue

        if result == "updated":
            updated += 1

    conn.commit()
    conn.close()
    return {
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "changes": changes,
        "batch_id": options["batch_id"],
    }


def _get_analytics_options(args, kwargs):
    option_names = [
        "end_date",
        "group_by",
        "metrics",
        "include_inactive",
        "min_activity",
        "output_format",
        "timezone",
        "sampling_rate",
        "anonymize",
    ]
    options = dict(zip(option_names, args))
    options.update(kwargs)
    options.setdefault("include_inactive", True)
    options.setdefault("min_activity", 0)
    options.setdefault("anonymize", False)
    return options


def _build_user_stats(rows):
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
            {"actions": 0, "first_seen": timestamp, "last_seen": timestamp, "action_types": {}},
        )
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


def _top_users(user_stats, anonymize):
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


def generate_user_analytics(start_date, *args, **kwargs):
    """Generate analytics about user activity and engagement."""
    options = _get_analytics_options(args, kwargs)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM activity_log WHERE timestamp >= ? AND timestamp <= ?",
        (start_date, options["end_date"]),
    )
    rows = cursor.fetchall()
    conn.close()

    user_stats, total_actions, unique_users, action_counts = _build_user_stats(rows)
    user_stats = _filter_active_users(
        user_stats,
        options["include_inactive"],
        options["min_activity"],
    )

    active_users = len(user_stats)
    avg_actions = total_actions / active_users if active_users > 0 else 0
    top_users = _top_users(user_stats, options["anonymize"])

    return {
        "period": {"start": start_date, "end": options["end_date"]},
        "total_actions": total_actions,
        "unique_users": len(unique_users),
        "active_users": active_users,
        "avg_actions_per_user": round(avg_actions, 2),
        "action_distribution": action_counts,
        "top_users": top_users,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
