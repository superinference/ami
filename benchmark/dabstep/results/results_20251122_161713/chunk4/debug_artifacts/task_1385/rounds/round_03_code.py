# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1385
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 3524 characters (FULL CODE)
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

def calculate_fee(amount, rule):
    """
    Calculates fee based on fixed_amount and rate.
    Formula: fee = fixed_amount + rate * amount / 10000
    """
    fixed = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000.0)

# 1. Get MCC for "Fast Food Restaurants"
mcc_file_path = '/output/chunk4/data/context/merchant_category_codes.csv'
try:
    df_mcc = pd.read_csv(mcc_file_path)
    
    # Exact match first
    target_description = "Fast Food Restaurants"
    mcc_row = df_mcc[df_mcc['description'] == target_description]
    
    if mcc_row.empty:
        # Case-insensitive match
        mcc_row = df_mcc[df_mcc['description'].str.lower() == target_description.lower()]
    
    if not mcc_row.empty:
        mcc_code = int(mcc_row.iloc[0]['mcc'])
        # print(f"Found MCC for '{target_description}': {mcc_code}")
    else:
        # Fallback based on common knowledge/context if file lookup fails
        # print(f"Warning: MCC for '{target_description}' not found in CSV. Using fallback 5814.")
        mcc_code = 5814
except Exception as e:
    # print(f"Error reading MCC file: {e}")
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
    # Check Card Scheme (Exact match required)
    if rule.get('card_scheme') != target_scheme:
        continue
        
    # Check Account Type (Wildcard logic: None/Empty matches all)
    if not is_applicable(rule.get('account_type'), target_account_type):
        continue
        
    # Check MCC (Wildcard logic: None/Empty matches all)
    if not is_applicable(rule.get('merchant_category_code'), mcc_code):
        continue
        
    # Calculate Fee for this rule
    fee = calculate_fee(transaction_amount, rule)
    matching_fees.append(fee)

# 4. Compute Average and Print
if matching_fees:
    average_fee = sum(matching_fees) / len(matching_fees)
    print(f"{average_fee:.6f}")
else:
    print("0.000000")
