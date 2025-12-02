# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1674
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2876 characters (FULL CODE)
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

# Load the fees.json file
file_path = '/output/chunk2/data/context/fees.json'
df_fees = pd.read_json(file_path)

# Define the target account type and card scheme
target_account_type = 'S'
target_scheme = 'NexPay'

# Function to check if the account_type list matches the criteria
# Criteria: The list contains 'S' OR the list is empty (wildcard)
def matches_account_criteria(account_type_list):
    if not account_type_list:  # Empty list acts as a wildcard
        return True
    if target_account_type in account_type_list:
        return True
    return False

# Filter the dataframe
# 1. Filter by card_scheme
df_nexpay = df_fees[df_fees['card_scheme'] == target_scheme].copy()

# 2. Filter by account_type using the helper function
df_filtered = df_nexpay[df_nexpay['account_type'].apply(matches_account_criteria)]

# Output the result
print(f"Total rows in fees.json: {len(df_fees)}")
print(f"Rows matching scheme '{target_scheme}': {len(df_nexpay)}")
print(f"Rows matching scheme '{target_scheme}' AND account type '{target_account_type}' (or wildcard): {len(df_filtered)}")
print("\nSample of filtered data:")
print(df_filtered[['ID', 'card_scheme', 'account_type', 'fixed_amount', 'rate']].head())
