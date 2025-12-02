# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1679
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3415 characters (FULL CODE)
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


import pandas as pd
import json

# Define file paths
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

# Load fees.json
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
df_fees = pd.DataFrame(fees_data)

# Load merchant_data.json
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
df_merchants = pd.DataFrame(merchant_data)

# 1. Inspect Fee Rules for SwiftCharge and Account Type 'S'
# Filter for card_scheme = 'SwiftCharge'
swift_fees = df_fees[df_fees['card_scheme'] == 'SwiftCharge'].copy()

# Filter for account_type = 'S' (or empty list which acts as wildcard)
def is_applicable_to_s(acc_type_list):
    if not acc_type_list:  # Empty list matches all
        return True
    return 'S' in acc_type_list

swift_s_fees = swift_fees[swift_fees['account_type'].apply(is_applicable_to_s)].copy()

print(f"Total SwiftCharge rules found: {len(swift_fees)}")
print(f"Rules applicable to Account Type 'S': {len(swift_s_fees)}")
print("\n--- Applicable Fee Rules (First 10) ---")
print(swift_s_fees[['ID', 'account_type', 'merchant_category_code', 'fixed_amount', 'rate']].head(10).to_string())

# 2. Inspect Merchants with Account Type 'S'
# This is crucial because fee rules often depend on Merchant Category Code (MCC)
s_merchants = df_merchants[df_merchants['account_type'] == 'S'].copy()

print("\n--- Merchants with Account Type 'S' ---")
print(s_merchants[['merchant', 'account_type', 'merchant_category_code']].to_string())

# 3. Check for MCC overlap
# We need to see if the fee rules are specific to the MCCs of our S-type merchants
s_mccs = s_merchants['merchant_category_code'].unique().tolist()
print(f"\nUnique MCCs for Account Type 'S': {s_mccs}")
