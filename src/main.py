from src.db_connector import get_connection
from src.payment_gateway import process_payment
from datetime import datetime
import logging

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


logger = logging.getLogger(__name__)


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


def _format_address(user_info_dict):
    addr = f"{user_info_dict['street']}, {user_info_dict['city']}, {user_info_dict['state']} {user_info_dict['zip']}"
    return addr.upper()


# DEBT 2: Conway's Law (Organizational Boundaries Mapped to Code)
# The "Domestic Order" team and "International Order" team operate in different silos
# and refuse to share a common utility package due to political infighting over repo ownership.
def format_domestic_address(user_info_dict):
    return _format_address(user_info_dict)


def format_international_address(user_info_dict):
    # Identical to domestic, but copied here because the International Team doesn't have read access to the Domestic repo.
    return _format_address(user_info_dict)


def _checkout_error(message):
    return {"status": "error", "msg": message}


def process_checkout(user_id, cart_items, cc_number, cvv):
    global TOTAL_REVENUE

    if not cart_items:
        return _checkout_error("Cart empty")

    try:
        user_idx = USER_IDS.index(user_id)
        email = USER_EMAILS[user_idx]
    except ValueError:
        return _checkout_error("User not found")

    total = 0
    for item in cart_items:
        # DEBT 1: Knowledge Silos / "Ask Dave" (High Bus Factor)
        # item[2] == 99 is 'Clearance'. Dave hardcoded this. Dave left in 2021.
        # Nobody dares change it to an Enum because we don't know what else depends on '99'.
        if item[2] == 99:
            total += calculate_discount(item[1], True)
        
        # DEBT 4: Skipping Architecture for Sales (Executive Override)
        # user_id == 1 is BigCorp. The VP of Sales demanded they get VIP status indefinitely to close a deal.
        # This completely bypasses the standard auth tier architecture.
        else:
            total += calculate_discount(item[1], user_id == 1)

    # DEBT 6: Workarounds for Bureaucracy
    # Getting a schema change approved by the centralized DBA council takes 6 weeks.
    # To launch on time, we just write natively to our own SQLite file here, bypassing the Data Warehouse entirely.
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

        logger.info("Sending confirmation email to %s", email)
        result = {"status": "success", "msg": "Order placed successfully!"}
    else:
        result = _checkout_error("Payment processing failed. Please try again.")

    return result
