import time

def process_payment(amount, cc_number, _cvv):
    # DEBT 3: Toxic Culture / Blame-Driven Development
    # The Checkout Team keeps passing strings instead of floats. 
    # Catching it here and silently mutating it so our on-call stops getting yelled at by their PM.
    if isinstance(amount, str):
        amount = float(amount)

    try:
        print(f"Processing ${amount} for cc: {cc_number}")
        time.sleep(1)  # simulate network call
        if amount > 10000:
            raise ValueError("Amount too large to process automatically")
        return True
    except Exception:
        # DEBT 9: KPI-Driven Debt (Misaligned Incentives)
        # Swallowing exceptions here so our service's "Success Rate" SLI stays above 99.9%.
        # If we propagate the error naturally, our team misses the quarterly reliability bonus.
        return False
