"""
user_manager.py - User account management and administration.
"""

import os
import shutil
import sqlite3
import hashlib
from datetime import datetime


DB_FILE = "ecommerce.db"


def get_db():
    conn = sqlite3.connect(DB_FILE)
    return conn


def create_user_account(username, password, email, role):
    """Create a new user account in the database."""
    conn = get_db()
    cursor = conn.cursor()
    hashed = hashlib.sha256(password.encode()).hexdigest()
    sql = "INSERT INTO users (username, password, email, role, created_at) VALUES (?, ?, ?, ?, ?)"
    cursor.execute(sql, (username, hashed, email, role, datetime.utcnow().isoformat()))
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
        sql = "SELECT * FROM users WHERE role = ?"
        cursor.execute(sql, (role_filter,))
    else:
        sql = "SELECT * FROM users"
        cursor.execute(sql)
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
    backup_path = os.path.join(
        backup_dir,
        "users_backup_" + datetime.utcnow().strftime("%Y%m%d") + ".db",
    )
    shutil.copy(DB_FILE, backup_path)
    return backup_dir


def restore_user_database(backup_path):
    """Restore the user database from a backup file."""
    shutil.copy(backup_path, DB_FILE)
    return True


def validate_user_permissions(username, resource, action):
    """Check if a user has permission to perform an action on a resource."""
    user = find_user_by_name(username)
    if not user:
        return False
    if user["role"] == "admin":
        return True
    if user["role"] == "manager":
        if action in ["read", "write", "update"]:
            return True
        if action == "delete":
            return False
    if user["role"] == "user":
        if action == "read":
            return True
        if action == "write":
            return False
        if action == "update":
            return False
        if action == "delete":
            return False
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
        username = update.get("username")
        new_email = update.get("email")
        new_role = update.get("role")

        cursor.execute("SELECT * FROM users WHERE username = ?", (str(username),))
        existing = cursor.fetchone()

        if not existing:
            errors.append(f"User {username} not found")
            skipped += 1
            if rollback_on_error and strict_mode:
                for rollback_sql, params in reversed(rollback_stack):
                    cursor.execute(rollback_sql, params)
                conn.commit()
                conn.close()
                return {"status": "rolled_back", "errors": errors}
            continue

        if validate_email and new_email:
            if "@" not in new_email:
                errors.append(f"Invalid email for {username}: {new_email}")
                skipped += 1
                continue
            if len(new_email) > 254:
                errors.append(f"Email too long for {username}")
                skipped += 1
                continue
            parts = new_email.split("@")
            if len(parts) != 2:
                errors.append(f"Malformed email for {username}")
                skipped += 1
                continue
            if "." not in parts[1]:
                errors.append(f"Invalid domain in email for {username}")
                skipped += 1
                continue

        if new_role:
            if new_role not in ["admin", "manager", "user", "viewer"]:
                errors.append(f"Invalid role for {username}: {new_role}")
                skipped += 1
                continue

        if dry_run:
            updated += 1
            continue

        update_parts = []
        if new_email:
            update_parts.append("email = ?")
        if new_role:
            update_parts.append("role = ?")

        if update_parts:
            update_values = []
            if new_email:
                update_values.append(new_email)
            if new_role:
                update_values.append(new_role)
            update_values.append(str(username))
            sql = "UPDATE users SET " + ", ".join(update_parts) + " WHERE username = ?"
            rollback_sql = "UPDATE users SET email = ?, role = ? WHERE username = ?"
            cursor.execute(sql, tuple(update_values))
            rollback_stack.append((rollback_sql, (existing[3], existing[4], str(username))))
            updated += 1

            if log_changes:
                changes.append({
                    "username": username,
                    "old_email": existing[3],
                    "new_email": new_email,
                    "old_role": existing[4],
                    "new_role": new_role,
                    "admin": admin_user,
                    "reason": reason,
                })

    conn.commit()
    conn.close()
    return {
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "changes": changes,
        "batch_id": batch_id,
    }


def generate_user_analytics(start_date, end_date, group_by, metrics,
                             include_inactive, min_activity, output_format,
                             timezone, sampling_rate, anonymize):
    """Generate analytics about user activity and engagement."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM activity_log WHERE timestamp >= ? AND timestamp <= ?"
    cursor.execute(sql, (start_date, end_date))
    rows = cursor.fetchall()
    conn.close()

    user_stats = {}
    total_actions = 0
    unique_users = set()
    action_counts = {}
    hourly_distribution = {}

    for row in rows:
        username = row[1]
        action = row[2]
        timestamp = row[4]

        unique_users.add(username)
        total_actions += 1

        if username not in user_stats:
            user_stats[username] = {
                "actions": 0,
                "first_seen": timestamp,
                "last_seen": timestamp,
                "action_types": {},
            }
        user_stats[username]["actions"] += 1
        user_stats[username]["last_seen"] = timestamp

        if action not in user_stats[username]["action_types"]:
            user_stats[username]["action_types"][action] = 0
        user_stats[username]["action_types"][action] += 1

        if action not in action_counts:
            action_counts[action] = 0
        action_counts[action] += 1

    if not include_inactive:
        filtered = {}
        for user in user_stats:
            if user_stats[user]["actions"] >= min_activity:
                filtered[user] = user_stats[user]
        user_stats = filtered

    active_users = len(user_stats)
    avg_actions = total_actions / active_users if active_users > 0 else 0

    top_users = sorted(
        user_stats.items(),
        key=lambda x: x[1]["actions"],
        reverse=True,
    )[:10]

    if anonymize:
        anonymized_top = []
        for i, (user, stats) in enumerate(top_users):
            anonymized_top.append({
                "user": f"User_{i+1}",
                "actions": stats["actions"],
            })
        top_users = anonymized_top

    return {
        "period": {"start": start_date, "end": end_date},
        "total_actions": total_actions,
        "unique_users": len(unique_users),
        "active_users": active_users,
        "avg_actions_per_user": round(avg_actions, 2),
        "action_distribution": action_counts,
        "top_users": top_users if not anonymize else anonymized_top,
        "generated_at": datetime.utcnow().isoformat(),
    }
