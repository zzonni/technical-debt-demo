import time

def process_payment(amount, cc_number, cvv):
    if not isinstance(amount, (int, float)):
        raise TypeError("Amount must be a number")

    try:
        masked_cc = cc_number[-4:].rjust(len(cc_number), '*')
        print(f"Processing ${amount} for cc: {masked_cc}")
        time.sleep(1) # simulate network call
        if amount > 10000:
            raise ValueError("Amount too large to process automatically")
        return True
    except Exception as e:
        return False 
