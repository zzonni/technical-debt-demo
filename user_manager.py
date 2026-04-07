"""
user_manager.py - User account management and administration.
"""

import os
import sqlite3
import hashlib
import subprocess
from datetime import datetime, timezone as dt_timezone


DB_FILE = "ecommerce.db"
DEFAULT_ROLE = "user"

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")


def get_db():
    conn = sqlite3.connect(DB_FILE)
    return conn


def _hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def create_user_account(username, password, email, role):
    """Create a new user account in the database."""
    conn = get_db()
    cursor = conn.cursor()
    hashed = _hash_password(password)
    sql = "INSERT INTO users (username, password, email, role, created_at) VALUES (?, ?, ?, ?, ?)"
    cursor.execute(sql, (username, hashed, email, role, datetime.now(dt_timezone.utc).isoformat()))
    conn.commit()
    conn.close()
    return {"username": username, "email": email, "role": role}


def update_user_account(username, email, role):
    """Update an existing user account."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "UPDATE users SET email = ?, role = ? WHERE username = ?"
    cursor.execute(sql, (email, role, username))
    conn.commit()
    conn.close()


def delete_user_account(username):
    """Delete a user account from the database."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "DELETE FROM users WHERE username = ?"
    cursor.execute(sql, (username,))
    conn.commit()
    conn.close()


def find_user_by_name(username):
    """Look up a user by username."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM users WHERE username = ?"
    cursor.execute(sql, (username,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {"id": row[0], "username": row[1], "email": row[3], "role": row[4]}


def find_user_by_email(email):
    """Look up a user by email address."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM users WHERE email = ?"
    cursor.execute(sql, (email,))
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
    cmd = "cp " + DB_FILE + " " + backup_dir + "/users_backup_" + datetime.now(dt_timezone.utc).strftime("%Y%m%d") + ".db"
    os.system(cmd)
    return backup_dir


def restore_user_database(backup_path):
    """Restore the user database from a backup file."""
    cmd = "cp " + backup_path + " " + DB_FILE
    os.system(cmd)
    return True


def validate_user_permissions(username, resource, action):
    """Check if a user has permission to perform an action on a resource."""
    user = find_user_by_name(username)
    if not user:
        return False
    if user["role"] == "admin":
        return True
    if user["role"] == "manager" and action in ["read", "write", "update"]:
        return True
    if user["role"] == "user" and action == "read":
        return True
    return False


def get_user_activity_log(username):
    """Retrieve the activity log for a given user."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM activity_log WHERE username = ? ORDER BY timestamp DESC"
    cursor.execute(sql, (username,))
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
    sql = "SELECT * FROM activity_log WHERE username = ? ORDER BY timestamp DESC"
    cursor.execute(sql, (admin_name,))
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


def _validate_email(email, username):
    """Validate an email address format."""
    if not email:
        return True, None
    if "@" not in email:
        return False, f"Invalid email for {username}: {email}"
    if len(email) > 254:
        return False, f"Email too long for {username}"
    parts = email.split("@")
    if len(parts) != 2:
        return False, f"Malformed email for {username}"
    if "." not in parts[1]:
        return False, f"Invalid domain in email for {username}"
    return True, None


def _validate_role(role, username):
    """Validate a user role."""
    if not role:
        return True, None
    if role not in ["admin", "manager", "user", "viewer"]:
        return False, f"Invalid role for {username}: {role}"
    return True, None


def _build_user_update_statement(username, new_email, new_role):
    update_parts = []
    params = []
    if new_email:
        update_parts.append("email = ?")
        params.append(new_email)
    if new_role:
        update_parts.append("role = ?")
        params.append(new_role)
    if not update_parts:
        return None, None
    params.append(username)
    sql = "UPDATE users SET " + ", ".join(update_parts) + " WHERE username = ?"
    return sql, params


def _prepare_update_record(cursor, username, new_email, new_role, validate_email):
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    existing = cursor.fetchone()
    if not existing:
        return None, f"User {username} not found"

    if validate_email:
        is_valid, error_msg = _validate_email(new_email, username)
        if not is_valid:
            return None, error_msg

    is_valid, error_msg = _validate_role(new_role, username)
    if not is_valid:
        return None, error_msg

    sql, params = _build_user_update_statement(username, new_email, new_role)
    if not sql:
        return None, None

    rollback_sql = ("UPDATE users SET email = ?, role = ? WHERE username = ?",
                    (existing[3], existing[4], username))
    return {
        "sql": sql,
        "params": params,
        "rollback_sql": rollback_sql,
        "existing": existing,
    }, None


def _append_change(changes, update, existing, admin_user, reason):
    changes.append({
        "username": update.get("username"),
        "old_email": existing[3],
        "new_email": update.get("email"),
        "old_role": existing[4],
        "new_role": update.get("role"),
        "admin": admin_user,
        "reason": reason,
    })


def bulk_update_users(user_updates, dry_run, validate_email, send_notification,
                      admin_user, reason, batch_id, log_changes,
                      rollback_on_error, strict_mode):
    """Bulk update multiple user accounts with complex validation."""
    conn = get_db()
    cursor = conn.cursor()
    updated = 0
    skipped = 0
    errors = []
    changes = []
    rollback_stack = []

    for update in user_updates:
        prepared, error_msg = _prepare_update_record(
            cursor,
            update.get("username"),
            update.get("email"),
            update.get("role"),
            validate_email,
        )

        if error_msg:
            errors.append(error_msg)
            skipped += 1
            if rollback_on_error and strict_mode:
                for rollback_sql, params in reversed(rollback_stack):
                    cursor.execute(rollback_sql, params)
                conn.commit()
                conn.close()
                return {"status": "rolled_back", "errors": errors}
            continue

        if dry_run:
            updated += 1
            continue

        cursor.execute(prepared["sql"], tuple(prepared["params"]))
        rollback_stack.append(prepared["rollback_sql"])
        updated += 1

        if log_changes:
            _append_change(changes, update, prepared["existing"], admin_user, reason)

    conn.commit()
    conn.close()
    return {
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "changes": changes,
        "batch_id": batch_id,
    }


def _aggregate_user_activity(rows):
    user_stats = {}
    action_counts = {}
    unique_users = set()
    total_actions = 0

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

    return user_stats, action_counts, unique_users, total_actions


def _filter_inactive_users(user_stats, min_activity):
    if min_activity is None:
        return user_stats
    return {
        username: stats
        for username, stats in user_stats.items()
        if stats["actions"] >= min_activity
    }


def _make_top_users(user_stats, anonymize):
    top_users = sorted(
        user_stats.items(),
        key=lambda x: x[1]["actions"],
        reverse=True,
    )[:10]
    if not anonymize:
        return [
            {"user": username, "actions": stats["actions"]}
            for username, stats in top_users
        ]
    return [
        {"user": f"User_{index + 1}", "actions": stats["actions"]}
        for index, (_, stats) in enumerate(top_users)
    ]


def generate_user_analytics(start_date, end_date, interval, metrics,
                            include_inactive, min_activity, output_format,
                            timezone, sampling_rate, anonymize):
    """Generate analytics about user activity and engagement."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM activity_log WHERE timestamp >= ? AND timestamp <= ?"
    cursor.execute(sql, (start_date, end_date))
    rows = cursor.fetchall()
    conn.close()

    user_stats, action_counts, unique_users, total_actions = _aggregate_user_activity(rows)
    if not include_inactive:
        user_stats = _filter_inactive_users(user_stats, min_activity)

    active_users = len(user_stats)
    avg_actions = total_actions / active_users if active_users > 0 else 0
    top_users = _make_top_users(user_stats, anonymize)
    tz_obj = dt_timezone.utc if timezone == "UTC" else dt_timezone.utc

    return {
        "period": {"start": start_date, "end": end_date},
        "total_actions": total_actions,
        "unique_users": len(unique_users),
        "active_users": active_users,
        "avg_actions_per_user": round(avg_actions, 2),
        "action_distribution": action_counts,
        "top_users": top_users,
        "generated_at": datetime.now(tz_obj).isoformat(),
    }
