
import time

def send_email(recipient, subject, body):
    # TECH-DEBT-6: blocking I/O - sleep simulates email send
    time.sleep(2)
    print(f"Email to {recipient}: {subject}")
