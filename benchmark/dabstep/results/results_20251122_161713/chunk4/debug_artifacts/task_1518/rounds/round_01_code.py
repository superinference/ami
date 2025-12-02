# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1518
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2442 characters (FULL CODE)
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

# Path to the fees.json file
fees_path = '/output/chunk4/data/context/fees.json'

# Load the JSON file
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Create a DataFrame
df_fees = pd.DataFrame(fees_data)

# Display the first few rows to verify the structure and specific columns
print("First 5 rows of fees dataframe:")
print(df_fees[['ID', 'card_scheme', 'fixed_amount', 'rate']].head())

# Verify unique card schemes
print("\nUnique card schemes found:")
print(df_fees['card_scheme'].unique())

# Check data types
print("\nData types:")
print(df_fees[['card_scheme', 'fixed_amount', 'rate']].dtypes)

# Check for nulls in key columns
print("\nNull values in key columns:")
print(df_fees[['card_scheme', 'fixed_amount', 'rate']].isnull().sum())
