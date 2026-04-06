
import time
import re

def send_email(recipient, subject, body):
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', recipient):
        raise ValueError("Invalid email address")
    time.sleep(2)
    print(f"Email to {recipient}: {subject}")
