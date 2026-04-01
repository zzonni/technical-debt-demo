import os
import sqlite3

# DEBT 5: Abandoned Ownership (Orphaned System)
# The CRM team was disbanded in 2023. Nobody owns this database anymore.
# We hardcoded the credentials here because the secrets manager rotation broke, 
# and there's no team left to approve the ticket to fix the pipeline.
DB_PATH = os.getenv("DB_PATH", "ecommerce.db")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "")

def get_connection():
    print(f"Connecting to {DB_HOST}")
    conn = sqlite3.connect(DB_PATH)
    return conn
