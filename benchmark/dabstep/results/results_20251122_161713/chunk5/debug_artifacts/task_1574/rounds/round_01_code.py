# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1574
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 3753 characters (FULL CODE)
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

def main():
    # Path to the fees configuration file
    fees_file_path = '/output/chunk5/data/context/fees.json'
    
    # Load the fees data
    try:
        with open(fees_file_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {fees_file_path}")
        return

    # Define the target parameters from the question
    target_scheme = 'NexPay'
    target_account_type = 'D'
    transaction_value = 5000.0
    
    matching_fees = []
    
    # Iterate through each fee rule to find matches
    for rule in fees_data:
        # 1. Filter by Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # 2. Filter by Account Type
        # The rule applies if the account_type list is empty (wildcard) 
        # OR if it explicitly contains the target account type 'D'.
        account_types = rule.get('account_type')
        
        # Handle case where account_type might be None (treat as wildcard/empty)
        if account_types is None:
            account_types = []
            
        # If list is not empty and does not contain 'D', it's a mismatch
        if len(account_types) > 0 and target_account_type not in account_types:
            continue
            
        # 3. Calculate Fee for this rule
        # Formula: fee = fixed_amount + (rate * transaction_value / 10000)
        fixed_amount = rule.get('fixed_amount', 0.0)
        rate = rule.get('rate', 0.0)
        
        # Ensure values are numeric (handle None if present)
        if fixed_amount is None: fixed_amount = 0.0
        if rate is None: rate = 0.0
        
        fee = fixed_amount + (rate * transaction_value / 10000.0)
        matching_fees.append(fee)
        
    # Calculate and print the average fee
    if matching_fees:
        average_fee = sum(matching_fees) / len(matching_fees)
        # Output formatted to 6 decimals as requested
        print(f"{average_fee:.6f}")
    else:
        print("No matching fee rules found.")

if __name__ == "__main__":
    main()
