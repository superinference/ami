# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1417
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3599 characters (FULL CODE)
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
    # 1. Get MCC for "Taxicabs and Limousines"
    try:
        mcc_df = pd.read_csv('/output/chunk4/data/context/merchant_category_codes.csv')
        target_description = "Taxicabs and Limousines"
        matching_row = mcc_df[mcc_df['description'] == target_description]

        if matching_row.empty:
            print(f"Error: MCC for '{target_description}' not found.")
            return

        target_mcc = int(matching_row.iloc[0]['mcc'])
        # print(f"Target MCC: {target_mcc}") # Debug
    except Exception as e:
        print(f"Error reading MCC file: {e}")
        return

    # 2. Load fees and find matching rules
    try:
        with open('/output/chunk4/data/context/fees.json', 'r') as f:
            fees_data = json.load(f)
    except Exception as e:
        print(f"Error reading fees file: {e}")
        return

    # Define target parameters
    target_scheme = 'GlobalCard'
    target_account_type = 'H'
    transaction_amount = 1000.0

    matching_fees = []

    for rule in fees_data:
        # Check Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
        
        # Check Account Type (Wildcard: empty list or None means ALL)
        rule_account_types = rule.get('account_type')
        if is_not_empty(rule_account_types):
            # If list is not empty, target must be in it
            if target_account_type not in rule_account_types:
                continue
        # If empty/None, it matches 'H' implicitly
        
        # Check MCC (Wildcard: empty list or None means ALL)
        rule_mccs = rule.get('merchant_category_code')
        if is_not_empty(rule_mccs):
            # If list is not empty, target must be in it
            if target_mcc not in rule_mccs:
                continue
        # If empty/None, it matches 4121 implicitly
        
        # Calculate Fee
        # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
        fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        fee = fixed_amount + (rate * transaction_amount / 10000.0)
        matching_fees.append(fee)

    # 3. Calculate Average and Print Result
    if not matching_fees:
        print("No matching fees found")
    else:
        average_fee = sum(matching_fees) / len(matching_fees)
        # Print result in EUR with 6 decimals
        print(f"{average_fee:.6f}")

if __name__ == "__main__":
    main()
