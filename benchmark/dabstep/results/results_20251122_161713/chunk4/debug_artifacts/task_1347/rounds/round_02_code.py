# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1347
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3264 characters (FULL CODE)
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

# 1. Get the MCC for the specified description
df_mcc = pd.read_csv('/output/chunk4/data/context/merchant_category_codes.csv')
target_description = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"

# Filter for the exact description
matching_mcc_row = df_mcc[df_mcc['description'] == target_description]

if matching_mcc_row.empty:
    print("Error: MCC Description not found.")
else:
    target_mcc = int(matching_mcc_row.iloc[0]['mcc'])
    
    # 2. Load Fees Data
    with open('/output/chunk4/data/context/fees.json', 'r') as f:
        fees_data = json.load(f)

    matching_fees = []
    transaction_value = 100.0
    target_scheme = 'SwiftCharge'
    target_account_type = 'H'

    # 3. Iterate through rules to find matches
    for rule in fees_data:
        # Check Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue

        # Check Account Type
        # Logic: If field is null/empty, it applies to ALL (Wildcard). 
        # If not empty, it must contain our target 'H'.
        rule_account_types = rule.get('account_type')
        if is_not_empty(rule_account_types):
            if target_account_type not in rule_account_types:
                continue
        
        # Check Merchant Category Code
        # Logic: If field is null/empty, it applies to ALL (Wildcard).
        # If not empty, it must contain our target MCC.
        rule_mccs = rule.get('merchant_category_code')
        if is_not_empty(rule_mccs):
            if target_mcc not in rule_mccs:
                continue

        # 4. Calculate Fee for matching rule
        # Formula: fee = fixed_amount + rate * transaction_value / 10000
        fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        fee = fixed_amount + (rate * transaction_value / 10000)
        matching_fees.append(fee)

    # 5. Calculate Average and Print Result
    if matching_fees:
        average_fee = sum(matching_fees) / len(matching_fees)
        print(f"{average_fee:.6f}")
    else:
        print("No matching fee rules found.")
