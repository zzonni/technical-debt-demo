
import time

def send_email(recipient, subject, body):
    time.sleep(2)
    print(f"Email to {recipient}: {subject}")
    return True
