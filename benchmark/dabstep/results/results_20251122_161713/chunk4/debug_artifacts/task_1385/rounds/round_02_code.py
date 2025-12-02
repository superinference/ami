# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1385
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3135 characters (FULL CODE)
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

def is_applicable(rule_value, target_value):
    """
    Checks if a rule value (list, None, or specific value) applies to the target value.
    Rule value None or [] means 'applies to all'.
    """
    if rule_value is None:
        return True
    if isinstance(rule_value, list):
        if len(rule_value) == 0:
            return True
        return target_value in rule_value
    # If it's a scalar (though schema says arrays for these fields)
    return rule_value == target_value

# 1. Get MCC for "Fast Food Restaurants"
mcc_file_path = '/output/chunk4/data/context/merchant_category_codes.csv'
df_mcc = pd.read_csv(mcc_file_path)

target_description = "Fast Food Restaurants"
mcc_row = df_mcc[df_mcc['description'] == target_description]

if not mcc_row.empty:
    mcc_code = int(mcc_row.iloc[0]['mcc'])
else:
    # Fallback: try case-insensitive search
    mcc_row = df_mcc[df_mcc['description'].str.lower() == target_description.lower()]
    if not mcc_row.empty:
        mcc_code = int(mcc_row.iloc[0]['mcc'])
    else:
        # Fallback based on context if not found
        mcc_code = 5814 

# 2. Load Fees Data
fees_file_path = '/output/chunk4/data/context/fees.json'
with open(fees_file_path, 'r') as f:
    fees_data = json.load(f)

# 3. Filter Rules and Calculate Fees
matching_fees = []
transaction_amount = 1000.0
target_scheme = 'GlobalCard'
target_account_type = 'H'

for rule in fees_data:
    # Check Card Scheme
    if rule.get('card_scheme') != target_scheme:
        continue
        
    # Check Account Type (Wildcard logic: None/Empty matches all)
    if not is_applicable(rule.get('account_type'), target_account_type):
        continue
        
    # Check MCC (Wildcard logic: None/Empty matches all)
    if not is_applicable(rule.get('merchant_category_code'), mcc_code):
        continue
        
    # Calculate Fee for this rule
    # Formula: fee = fixed_amount + rate * transaction_value / 10000
    fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    
    fee = fixed_amount + (rate * transaction_amount / 10000.0)
    matching_fees.append(fee)

# 4. Compute Average and Print
if matching_fees:
    average_fee = sum(matching_fees) / len(matching_fees)
    print(f"{average_fee:.6f}")
else:
    print("No matching fee rules found")
