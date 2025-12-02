# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1420
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3882 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
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

def is_wildcard_or_match(rule_value, target_value):
    """
    Checks if the rule_value acts as a wildcard or contains the target_value.
    rule_value: list or None from the fee rule (e.g., account_type list)
    target_value: the specific value we are looking for (e.g., 'H')
    """
    # If rule_value is None or empty list, it's a wildcard (applies to all)
    if rule_value is None:
        return True
    if isinstance(rule_value, list):
        if len(rule_value) == 0:
            return True
        return target_value in rule_value
    # Fallback if it's a scalar (though schema says lists)
    return rule_value == target_value

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

# 1. Find the MCC for "Taxicabs and Limousines"
mcc_df = pd.read_csv('/output/chunk3/data/context/merchant_category_codes.csv')
target_description = "Taxicabs and Limousines"

# Filter for the exact description
match = mcc_df[mcc_df['description'] == target_description]

if match.empty:
    # Fallback: try case-insensitive search if exact match fails
    match = mcc_df[mcc_df['description'].str.lower() == target_description.lower()]
    if match.empty:
        raise ValueError(f"Could not find MCC for description: {target_description}")

target_mcc = int(match.iloc[0]['mcc'])
print(f"DEBUG: Found MCC for '{target_description}': {target_mcc}")

# 2. Load Fees Data
with open('/output/chunk3/data/context/fees.json', 'r') as f:
    fees_data = json.load(f)

# 3. Filter Rules and Calculate Fees
# Criteria:
# - Card Scheme: TransactPlus
# - Account Type: H (or wildcard)
# - MCC: 4121 (or wildcard)
# - Transaction Value: 1000 EUR

target_scheme = 'TransactPlus'
target_account_type = 'H'
transaction_amount = 1000.0

matching_fees = []

for rule in fees_data:
    # Check Card Scheme (Exact match required)
    if rule.get('card_scheme') != target_scheme:
        continue
        
    # Check Account Type (List contains 'H' or is empty/wildcard)
    if not is_wildcard_or_match(rule.get('account_type'), target_account_type):
        continue
        
    # Check Merchant Category Code (List contains 4121 or is empty/wildcard)
    if not is_wildcard_or_match(rule.get('merchant_category_code'), target_mcc):
        continue
        
    # If criteria match, calculate the fee for this rule
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    
    fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    
    fee = fixed_amount + (rate * transaction_amount / 10000.0)
    matching_fees.append(fee)

# 4. Calculate Average and Output
if not matching_fees:
    print("No matching fee rules found.")
else:
    average_fee = sum(matching_fees) / len(matching_fees)
    print(f"DEBUG: Found {len(matching_fees)} matching rules.")
    # Output formatted to 6 decimals as requested
    print(f"{average_fee:.6f}")
