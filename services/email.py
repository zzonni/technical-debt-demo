
import logging
import time

logger = logging.getLogger(__name__)
EMAIL_DELAY_SECONDS = 2

def send_email(recipient, subject, body):
    time.sleep(EMAIL_DELAY_SECONDS)
    logger.info(f"Email to {recipient}: {subject}")
