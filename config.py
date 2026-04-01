import os
import secrets

# Centralized configuration
DB_FILE = os.getenv("DB_FILE", "ecommerce.db")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
SECRET_KEY = os.getenv("FLASK_SECRET_KEY") or secrets.token_hex(32)

# Feature flags / other defaults
USE_BCRYPT = True
