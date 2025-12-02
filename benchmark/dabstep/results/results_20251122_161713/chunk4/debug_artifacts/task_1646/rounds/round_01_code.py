# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1646
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 3490 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

# Helper functions for robust data processing
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v)
    return float(value)

def safe_get(df, column, default=None):
    """Safely get column from DataFrame, return default if not exists."""
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

def is_not_empty(array):
    """Check if array/list is not empty. Handles numpy arrays safely."""
    if array is None:
        return False
    if hasattr(array, 'size'):  # numpy array
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default


import json

def calculate_nexpay_f_average_fee():
    fees_file_path = '/output/chunk4/data/context/fees.json'
    
    try:
        with open(fees_file_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {fees_file_path}")
        return

    target_scheme = 'NexPay'
    target_account_type = 'F'
    transaction_value = 4321
    
    calculated_fees = []

    for rule in fees_data:
        # 1. Filter by Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # 2. Filter by Account Type
        # Logic: If the rule's account_type list is empty or None, it applies to ALL (including F).
        # If it is not empty, 'F' must be in the list.
        rule_account_types = rule.get('account_type')
        
        # If rule_account_types has values (is not None/Empty) AND 'F' is not in it, skip.
        if rule_account_types and target_account_type not in rule_account_types:
            continue
            
        # 3. Calculate Fee for this rule
        # fee = fixed_amount + (rate / 10000 * transaction_value)
        fixed_amount = rule.get('fixed_amount', 0.0)
        rate = rule.get('rate', 0)
        
        # Ensure values are not None before calculation (though schema implies they are present)
        if fixed_amount is None: fixed_amount = 0.0
        if rate is None: rate = 0
        
        fee = fixed_amount + (rate / 10000 * transaction_value)
        calculated_fees.append(fee)

    # 4. Calculate Average
    if calculated_fees:
        average_fee = sum(calculated_fees) / len(calculated_fees)
        print(f"{average_fee:.6f}")
    else:
        print("No matching fee rules found for NexPay and Account Type F.")

if __name__ == "__main__":
    calculate_nexpay_f_average_fee()
