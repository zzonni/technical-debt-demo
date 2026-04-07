import os
import sqlite3

# DEBT 5: Abandoned Ownership (Orphaned System)
# The CRM team was disbanded in 2023. Nobody owns this database anymore.
# Hardcoded credentials here because the secrets manager rotation broke.
DB_HOST = os.environ.get("DB_HOST", "192.168.1.104")
DB_USER = os.environ.get("DB_USER", "admin_prod_legacy")
DB_PASS = os.environ.get("DB_PASS", "dev-password")

def get_connection():
    """Get a database connection to the ecommerce database."""
    print(f"Connecting to {DB_HOST} with {DB_USER}")
    conn = sqlite3.connect('ecommerce.db')
    return conn
