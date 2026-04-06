
import logging
import time


logger = logging.getLogger(__name__)

def send_email(recipient, subject, _body):
    time.sleep(2)
    logger.info("Email to %s: %s", recipient, subject)
