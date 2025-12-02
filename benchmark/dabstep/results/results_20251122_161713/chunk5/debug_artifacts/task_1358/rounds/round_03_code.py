# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1358
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 3710 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

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

def main():
    # 1. Get MCC for the specific description
    mcc_path = '/output/chunk5/data/context/merchant_category_codes.csv'
    try:
        df_mcc = pd.read_csv(mcc_path)
    except FileNotFoundError:
        print(f"Error: File not found at {mcc_path}")
        return

    target_description = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"
    
    # Filter for the exact description
    match = df_mcc[df_mcc['description'] == target_description]
    
    if match.empty:
        print(f"Error: Description not found: {target_description}")
        return

    target_mcc = int(match.iloc[0]['mcc'])
    # print(f"Found MCC: {target_mcc}") # Debug

    # 2. Load Fees
    fees_path = '/output/chunk5/data/context/fees.json'
    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {fees_path}")
        return

    # 3. Filter Rules and Calculate Fees
    target_scheme = 'NexPay'
    target_account_type = 'H'
    transaction_value = 5000.0
    
    matching_fees = []
    
    for rule in fees_data:
        # Filter by Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # Filter by Account Type
        # Logic: If list is empty or None, it applies to ALL (so it matches).
        # If list is not empty, 'H' must be in it.
        rule_account_types = rule.get('account_type')
        if is_not_empty(rule_account_types):
            if target_account_type not in rule_account_types:
                continue
        
        # Filter by MCC
        # Logic: If list is empty or None, it applies to ALL (so it matches).
        # If list is not empty, target_mcc must be in it.
        rule_mccs = rule.get('merchant_category_code')
        if is_not_empty(rule_mccs):
            if target_mcc not in rule_mccs:
                continue
        
        # Calculate Fee
        # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
        fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        fee = fixed_amount + (rate * transaction_value / 10000.0)
        matching_fees.append(fee)
    
    # 4. Compute Average
    if not matching_fees:
        print("No matching fee rules found.")
    else:
        average_fee = sum(matching_fees) / len(matching_fees)
        # Print result in EUR with 6 decimals
        print(f"{average_fee:.6f}")

if __name__ == "__main__":
    main()
