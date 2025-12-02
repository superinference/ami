# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1438
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3172 characters (FULL CODE)
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
import numpy as np

# Define file path
fees_path = '/output/chunk5/data/context/fees.json'

# Load the fees data
df_fees = pd.read_json(fees_path)

# Display initial shape
print(f"Initial shape of fees dataframe: {df_fees.shape}")

# Function to handle merchant_category_code lists
# - None or [] indicates a wildcard (applies to all MCCs).
# - We replace these with ['ALL'] to preserve the rule during explosion.
def process_mcc_column(mcc_list):
    if mcc_list is None:
        return ['ALL']
    if isinstance(mcc_list, list):
        if len(mcc_list) == 0:
            return ['ALL']
        return mcc_list
    return [mcc_list]  # Fallback for single values if any

# Apply the processing function
df_fees['merchant_category_code'] = df_fees['merchant_category_code'].apply(process_mcc_column)

# Explode the merchant_category_code column
# Each row will now represent a fee rule for a specific MCC (or 'ALL')
df_fees_exploded = df_fees.explode('merchant_category_code')

# Reset index for cleaner look, though not strictly necessary
df_fees_exploded = df_fees_exploded.reset_index(drop=True)

# Verify the results
print(f"Shape after exploding merchant_category_code: {df_fees_exploded.shape}")
print("\nFirst 5 rows of the exploded dataframe:")
print(df_fees_exploded[['ID', 'merchant_category_code', 'fixed_amount', 'rate']].head())

print("\nSample of wildcard ('ALL') rules:")
print(df_fees_exploded[df_fees_exploded['merchant_category_code'] == 'ALL'][['ID', 'fixed_amount', 'rate']].head())
