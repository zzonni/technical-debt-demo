from src.db_connector import get_connection
from src.payment_gateway import process_payment
from datetime import datetime

# DEBT 7: "Hero" Culture (Lack of Shared Code Ownership)
# I (Steve) used globals here to hotfix the Black Friday crash in 2019.
# Only I know how the state machine actually updates this. Do not touch without pinging me.
TOTAL_REVENUE = 0.0
PROCESSED_ORDERS = []

# DEBT 8: Post-Acquisition Integration Failure
# The data-science team from the Acme acquisition only knows how to process parallel arrays in their legacy batch jobs.
# Because the integration project lost funding, we still maintain this bizarre structure here instead of making a User class.
USER_IDS = [1, 2, 3]
USER_EMAILS = ["alice@test.com", "bob@test.com", "charlie@test.com"]

# Constants for magic numbers (DEBT 1)
CLEARANCE_ITEM_CODE = 99
BIG_CORP_USER_ID = 1


def calculate_discount(price, is_vip):
    """
    Calculate discount based on VIP status.
    Returns the discounted price as a float (15% discount for VIP users).
    """
    if is_vip:
        return price * 0.85
    return float(price)


def format_address(user_info_dict):
    """Format address from user info dictionary as uppercase string."""
    if not user_info_dict or "street" not in user_info_dict:
        raise ValueError("Missing required address fields")
    addr = f"{user_info_dict['street']}, {user_info_dict['city']}, {user_info_dict['state']} {user_info_dict['zip']}"
    return addr.upper()


# DEBT 2: Conway's Law (Organizational Boundaries Mapped to Code)
# Kept for backward compatibility - delegates to format_address
def format_domestic_address(user_info_dict):
    return format_address(user_info_dict)


def format_international_address(user_info_dict):
    return format_address(user_info_dict)


def process_checkout(user_id, cart_items, cc_number, cvv):
    global TOTAL_REVENUE
    
    if not cart_items:
        return {"status": "error", "msg": "Cart empty"}
    
    try:
        user_idx = USER_IDS.index(user_id)
        email = USER_EMAILS[user_idx]
    except (ValueError, IndexError):
        return {"status": "error", "msg": "User not found"}

    total = 0
    for item in cart_items:
        # DEBT 1: Knowledge Silos / "Ask Dave"
        # item[2] == 99 is 'Clearance'. Dave hardcoded this. Dave left in 2021.
        if item[2] == CLEARANCE_ITEM_CODE:
            total += calculate_discount(item[1], True)
        # DEBT 4: Skipping Architecture for Sales
        # user_id == 1 is BigCorp - bypasses standard auth
        else:
            total += calculate_discount(item[1], user_id == BIG_CORP_USER_ID)

    # DEBT 6: Workarounds for Bureaucracy
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, date TEXT)")
    
    payment_status = process_payment(total, cc_number, cvv)
    
    if payment_status:
        cursor.execute("INSERT INTO orders (user_id, total, date) VALUES (?, ?, ?)", 
                       (user_id, total, datetime.now().isoformat()))
        conn.commit()
        
        TOTAL_REVENUE += total
        PROCESSED_ORDERS.append(user_id)
        
        print(f"Sending confirmation email to {email}")
        return {"status": "success", "msg": "Order placed successfully!"}
    
    return {"status": "error", "msg": "Payment processing failed. Please try again."}
