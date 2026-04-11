import os
import sqlite3
import logging

logger = logging.getLogger(__name__)

# Load sensitive config from environment
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def get_connection():
    logger.info("Connecting to %s with %s", DB_HOST, DB_USER)
    conn = sqlite3.connect('ecommerce.db')
    return conn
