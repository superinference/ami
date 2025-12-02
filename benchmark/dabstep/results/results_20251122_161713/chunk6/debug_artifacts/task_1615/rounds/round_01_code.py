# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1615
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2794 characters (FULL CODE)
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

# Define file path
fees_file_path = '/output/chunk6/data/context/fees.json'

# Load fees.json into a dataframe
with open(fees_file_path, 'r') as f:
    fees_data = json.load(f)

df_fees = pd.DataFrame(fees_data)

# Define filtering function for account_type
def filter_account_type(account_type_list):
    # If list is empty, it's a wildcard (matches all account types including 'H')
    if not account_type_list:
        return True
    # If list is not empty, check if 'H' is specifically included
    return 'H' in account_type_list

# Apply filters:
# 1. card_scheme must be 'SwiftCharge'
# 2. account_type must contain 'H' or be empty
df_filtered = df_fees[
    (df_fees['card_scheme'] == 'SwiftCharge') & 
    (df_fees['account_type'].apply(filter_account_type))
].copy()

# Output results to verify the step
print(f"Total rows in fees.json: {len(df_fees)}")
print(f"Rows after filtering for SwiftCharge and Account Type 'H' (or wildcard): {len(df_filtered)}")
print("\nFiltered DataFrame (First 5 rows):")
print(df_filtered[['ID', 'card_scheme', 'account_type', 'fixed_amount', 'rate']].head().to_string())
