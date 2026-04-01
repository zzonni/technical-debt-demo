import os

# Centralized configuration
DB_FILE = os.getenv("DB_FILE", "ecommerce.db")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123!")
SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")

# Feature flags / other defaults
USE_BCRYPT = True
