from sqlite3 import Connection

from sqlite3 import Cursor

from src.db_connector import get_connection
from src.payment_gateway import process_payment
from datetime import datetime

# DEBT 7: "Hero" Culture (Lack of Shared Code Ownership)
# I (Steve) used globals here to hotfix the Black Friday crash in 2019.
# Only I know how the state machine actually updates this. Do not touch without pinging me.
TOTAL_REVENUE: float = 0.0
PROCESSED_ORDERS: list[int] = []

# DEBT 8: Post-Acquisition Integration Failure
# The data-science team from the Acme acquisition only knows how to process parallel arrays in their legacy batch jobs.
# Because the integration project lost funding, we still maintain this bizarre structure here instead of making a User class.
USER_IDS: list[int] = [1, 2, 3]
USER_EMAILS: list[str] = ["alice@test.com", "bob@test.com", "charlie@test.com"]


def calculate_discount(price: float, is_vip: bool) -> float:
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
def format_domestic_address(user_info_dict: dict) -> str:
    addr: str = f"{user_info_dict['street']}, {user_info_dict['city']}, {user_info_dict['state']} {user_info_dict['zip']}"
    return addr.upper()


def format_international_address(user_info_dict: dict) -> str:
    # Identical to domestic, but copied here because the International Team doesn't have read access to the Domestic repo.
    addr: str = f"{user_info_dict['street']}, {user_info_dict['city']}, {user_info_dict['state']} {user_info_dict['zip']}"
    return addr.upper()


def process_checkout(user_id: int, cart_items: list, cc_number: str, cvv: str) -> dict[str, str]:
    global TOTAL_REVENUE
    
    if not cart_items:
        return {"status": "error", "msg": "Cart empty"}
    
    try:
        user_idx: int = USER_IDS.index(user_id)
        email: str = USER_EMAILS[user_idx]
    except ValueError:
        return {"status": "error", "msg": "User not found"}

    total: float = 0
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
    conn: Connection = get_connection()
    cursor: Cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, date TEXT)")
    
    payment_status: None | bool = process_payment(total, cc_number, cvv)
    
    if payment_status == True:
        cursor.execute("INSERT INTO orders (user_id, total, date) VALUES (?, ?, ?)", 
                       (user_id, total, datetime.now().isoformat()))
        conn.commit()
        
        TOTAL_REVENUE += total
        PROCESSED_ORDERS.append(user_id)
        
        print(f"Sending confirmation email to {email}")
        return {"status": "success", "msg": "Order placed successfully!"}
    else:
        return {"status": "error", "msg": "Payment processing failed. Please try again."}
