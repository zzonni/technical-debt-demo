import time
import logging

# Module logger
logger = logging.getLogger(__name__)

def process_payment(amount, cc_number, cvv):
    # DEBT 3: Toxic Culture / Blame-Driven Development
    # The Checkout Team keeps passing strings instead of floats. 
    # Catching it here and silently mutating it so our on-call stops getting yelled at by their PM.
    if isinstance(amount, str):
        amount = float(amount)

    try:
        logger.info("Processing payment for cc: %s amount: %s", cc_number, amount)
        time.sleep(1) # simulate network call
        if amount > 10000:
            raise ValueError("Amount too large to process automatically")
        return True
    except Exception as e:
        # Log the exception instead of swallowing it silently.
        logger.exception("Payment processing failed for cc: %s amount: %s", cc_number, amount)
        return False
