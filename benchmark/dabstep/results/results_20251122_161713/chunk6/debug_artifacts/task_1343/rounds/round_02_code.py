# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1343
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3582 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# Helper functions for robust data processing
# ═══════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════
# Main Analysis Script
# ═══════════════════════════════════════════════════════════

# 1. Find the MCC for the specific description
mcc_df = pd.read_csv('/output/chunk6/data/context/merchant_category_codes.csv')
target_description = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"

# Try exact match first
match = mcc_df[mcc_df['description'] == target_description]

if match.empty:
    # Fallback to partial match if exact string differs slightly
    match = mcc_df[mcc_df['description'].str.contains("Drinking Places", case=False, na=False)]
    match = match[match['description'].str.contains("Bars, Taverns", case=False, na=False)]

if match.empty:
    print("Error: MCC not found.")
    exit()

target_mcc = int(match.iloc[0]['mcc'])
print(f"Identified MCC: {target_mcc} for '{target_description}'")

# 2. Load fees and filter for applicable rules
with open('/output/chunk6/data/context/fees.json', 'r') as f:
    fees_data = json.load(f)

target_scheme = 'SwiftCharge'
target_account_type = 'H'
transaction_amount = 50.0

matching_fees = []

for rule in fees_data:
    # Check Card Scheme
    if rule.get('card_scheme') != target_scheme:
        continue
        
    # Check Account Type (Wildcard [] or None matches ALL, otherwise must contain target)
    rule_account_types = rule.get('account_type')
    if is_not_empty(rule_account_types):
        if target_account_type not in rule_account_types:
            continue
            
    # Check MCC (Wildcard [] or None matches ALL, otherwise must contain target)
    rule_mccs = rule.get('merchant_category_code')
    if is_not_empty(rule_mccs):
        if target_mcc not in rule_mccs:
            continue
            
    # If we reached here, the rule applies
    # Calculate fee: fixed_amount + (rate * amount / 10000)
    fixed = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    
    fee = fixed + (rate * transaction_amount / 10000)
    matching_fees.append(fee)

# 3. Calculate Average
if not matching_fees:
    print("No matching fee rules found.")
else:
    average_fee = sum(matching_fees) / len(matching_fees)
    print(f"Found {len(matching_fees)} matching rules.")
    print(f"{average_fee:.6f}")
