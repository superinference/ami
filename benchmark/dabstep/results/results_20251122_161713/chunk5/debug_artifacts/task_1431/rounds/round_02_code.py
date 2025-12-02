# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1431
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3486 characters (FULL CODE)
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
    # 1. Identify the MCC for "Taxicabs and Limousines"
    df_mcc = pd.read_csv('/output/chunk5/data/context/merchant_category_codes.csv')
    target_description = "Taxicabs and Limousines"
    
    # Filter for the exact description
    match = df_mcc[df_mcc['description'] == target_description]
    
    if match.empty:
        print(f"Error: Description '{target_description}' not found in MCC file.")
        return

    # Extract MCC (ensure it's an integer for comparison with JSON data)
    mcc = int(match.iloc[0]['mcc'])
    
    # 2. Load Fee Rules
    with open('/output/chunk5/data/context/fees.json', 'r') as f:
        fees_data = json.load(f)

    # 3. Filter Rules and Calculate Fees
    # Criteria from question
    target_scheme = 'SwiftCharge'
    target_account_type = 'H'
    transaction_value = 4321.0
    
    matching_fees = []
    
    for rule in fees_data:
        # Filter 1: Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # Filter 2: Account Type
        # Logic: If list is empty [], it's a wildcard (matches all). 
        # If list is not empty, 'H' must be present.
        rule_account_types = rule.get('account_type')
        if is_not_empty(rule_account_types):
            if target_account_type not in rule_account_types:
                continue
        
        # Filter 3: Merchant Category Code (MCC)
        # Logic: If list is empty [], it's a wildcard (matches all).
        # If list is not empty, our specific MCC (4121) must be present.
        rule_mccs = rule.get('merchant_category_code')
        if is_not_empty(rule_mccs):
            if mcc not in rule_mccs:
                continue
        
        # If we reach here, the rule applies.
        # Calculate Fee: fee = fixed_amount + (rate * transaction_value / 10000)
        fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        fee = fixed_amount + (rate * transaction_value / 10000)
        matching_fees.append(fee)

    # 4. Compute Average and Output
    if not matching_fees:
        print("No matching fee rules found.")
    else:
        average_fee = sum(matching_fees) / len(matching_fees)
        # Output formatted to 6 decimals as requested
        print(f"{average_fee:.6f}")

if __name__ == "__main__":
    main()
