# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1531
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3019 characters (FULL CODE)
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

# Path to the fees file
fees_file_path = '/output/chunk5/data/context/fees.json'

# Load the fees data
with open(fees_file_path, 'r') as f:
    fees_data = json.load(f)

# Create a DataFrame
df_fees = pd.DataFrame(fees_data)

# Define the filtering criteria
target_scheme = 'SwiftCharge'
target_account_type = 'R'

# Define a helper function to check the account_type condition
# Rule: account_type matches if it contains 'R' OR if it is an empty list (wildcard)
def check_account_match(account_type_list):
    # Check for wildcard (empty list or None)
    if not account_type_list:
        return True
    # Check if target 'R' is in the specific list
    if target_account_type in account_type_list:
        return True
    return False

# Apply the filters
# 1. Filter for card_scheme == 'SwiftCharge'
# 2. Filter for account_type matching the criteria
filtered_fees = df_fees[
    (df_fees['card_scheme'] == target_scheme) & 
    (df_fees['account_type'].apply(check_account_match))
]

# Output the results to verify the step
print(f"Total rules in fees.json: {len(df_fees)}")
print(f"Rules matching scheme '{target_scheme}' and account type '{target_account_type}' (incl. wildcards): {len(filtered_fees)}")
print("\nSample of filtered rows:")
print(filtered_fees[['ID', 'card_scheme', 'account_type', 'fixed_amount', 'rate']].head())
