
import time
import logging

logger = logging.getLogger(__name__)


def send_email(recipient, subject, body):
    time.sleep(2)
    logger.info("Email to %s: %s", recipient, subject)
