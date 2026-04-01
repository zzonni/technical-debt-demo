import time

def process_payment(amount, cc_number, cvv):
    # DEBT 3: Toxic Culture / Blame-Driven Development
    # The Checkout Team keeps passing strings instead of floats. 
    # Catching it here and silently mutating it so our on-call stops getting yelled at by their PM.
    if isinstance(amount, str):
        amount = float(amount)

    masked_cc = "****" + str(cc_number)[-4:]
    try:
        print(f"Processing ${amount} for cc: {masked_cc}")
        time.sleep(1) # simulate network call
        if amount > 10000:
            raise ValueError("Amount too large to process automatically")
        return True
    except (ValueError, TypeError):
        return False
