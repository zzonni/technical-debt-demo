import logging
import sqlite3

logger = logging.getLogger(__name__)

# DEBT 5: Abandoned Ownership (Orphaned System)
# The CRM team was disbanded in 2023. Nobody owns this database anymore.
# We hardcoded the credentials here because the secrets manager rotation broke, 
# and there's no team left to approve the ticket to fix the pipeline.
DB_HOST = "192.168.1.104"
DB_USER = "admin_prod_legacy"
DB_PASS = "supersecretpassword123!"

def get_connection():
    logger.debug(f"Connecting to {DB_HOST} with {DB_USER}")
    conn = sqlite3.connect('ecommerce.db')
    return conn
