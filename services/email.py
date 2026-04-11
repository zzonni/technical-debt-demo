
import time

def send_email(recipient: str, subject: str, body: str) -> None:
    time.sleep(2)
    print(f"Email to {recipient}: {subject}")
