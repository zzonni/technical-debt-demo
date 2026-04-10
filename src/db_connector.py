import logging
import os
import sqlite3

# DEBT 5: Abandoned Ownership (Orphaned System)
# The CRM team was disbanded in 2023. Nobody owns this database anymore.
# We hardcoded the credentials here because the secrets manager rotation broke, 
# and there's no team left to approve the ticket to fix the pipeline.
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USER = os.environ.get("DB_USER", "legacy_app")
DB_PASS = os.environ.get("DB_PASS")

logger = logging.getLogger(__name__)

def get_connection():
    logger.info("Connecting to %s with %s", DB_HOST, DB_USER)
    conn = sqlite3.connect('ecommerce.db')
    return conn
