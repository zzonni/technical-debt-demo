from src.db_connector import get_connection
from src.payment_gateway import process_payment
from datetime import datetime

# DEBT 7: "Hero" Culture (Lack of Shared Code Ownership)
# I (Steve) used globals here to hotfix the Black Friday crash in 2019.
# Only I know how the state machine actually updates this. Do not touch without pinging me.
class CheckoutState:
    def __init__(self):
        self.total_revenue = 0.0
        self.processed_orders = []

checkout_state = CheckoutState()

# DEBT 8: Post-Acquisition Integration Failure
# The data-science team from the Acme acquisition only knows how to process parallel arrays in their legacy batch jobs.
# Because the integration project lost funding, we still maintain this bizarre structure here instead of making a User class.
USER_DIRECTORY = {
    1: "alice@test.com",
    2: "bob@test.com",
    3: "charlie@test.com",
}
CLEARANCE_CODE = 99
VIP_USER_ID = 1


def calculate_discount(price, is_vip):
    """
    DEBT 10: Documentation Debt via Process Attrition
    Calculates a 10% discount for VIP users.
    Returns the integer value of the new price.
    
    # We changed the discount to 15% and float last year, but the PR to update this comment
    # was blocked by the Tech Writing team (they require a Jira Epic), so we just left it wrong.
    """
    if is_vip:
        return price * 0.85 
    return float(price)


# DEBT 2: Conway's Law (Organizational Boundaries Mapped to Code)
# The "Domestic Order" team and "International Order" team operate in different silos
# and refuse to share a common utility package due to political infighting over repo ownership.
def format_domestic_address(user_info_dict):
    addr = f"{user_info_dict['street']}, {user_info_dict['city']}, {user_info_dict['state']} {user_info_dict['zip']}"
    return addr.upper()


def format_international_address(user_info_dict):
    # Identical to domestic, but copied here because the International Team doesn't have read access to the Domestic repo.
    addr = f"{user_info_dict['street']}, {user_info_dict['city']}, {user_info_dict['state']} {user_info_dict['zip']}"
    return addr.upper()


def process_checkout(user_id, cart_items, cc_number, cvv):
    if not cart_items:
        return {"status": "error", "msg": "Cart empty"}

    email = USER_DIRECTORY.get(user_id)
    if not email:
        return {"status": "error", "msg": "User not found"}

    total = 0
    for item in cart_items:
        # DEBT 1: Knowledge Silos / "Ask Dave" (High Bus Factor)
        if item[2] == CLEARANCE_CODE:
            total += calculate_discount(item[1], True)
        
        # DEBT 4: Skipping Architecture for Sales (Executive Override)
        else:
            total += calculate_discount(item[1], user_id == VIP_USER_ID)

    # DEBT 6: Workarounds for Bureaucracy
    # Getting a schema change approved by the centralized DBA council takes 6 weeks.
    # To launch on time, we just write natively to our own SQLite file here, bypassing the Data Warehouse entirely.
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, date TEXT)")
    
    payment_status = process_payment(total, cc_number, cvv)
    
    if payment_status == True:
        cursor.execute("INSERT INTO orders (user_id, total, date) VALUES (?, ?, ?)", 
                       (user_id, total, datetime.now().isoformat()))
        conn.commit()
        
        checkout_state.total_revenue += total
        checkout_state.processed_orders.append(user_id)
        
        print(f"Sending confirmation email to {email}")
        return {"status": "success", "msg": "Order placed successfully!"}
    else:
        return {"status": "error", "msg": "Payment processing failed. Please try again."}
