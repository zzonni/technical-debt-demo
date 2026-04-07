
import time
import logging

def send_email(recipient, subject, body):
    logger = logging.getLogger(__name__)
    time.sleep(2)
    logger.info("Email sent to: %s | subject: %s", recipient, subject)
