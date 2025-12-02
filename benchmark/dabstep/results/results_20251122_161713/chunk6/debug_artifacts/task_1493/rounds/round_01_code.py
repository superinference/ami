# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1493
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2454 characters (FULL CODE)
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
file_path = '/output/chunk6/data/context/fees.json'

# Load the JSON file
with open(file_path, 'r') as f:
    fees_data = json.load(f)

# Convert to DataFrame
df_fees = pd.DataFrame(fees_data)

# Display structure and content to inspect criteria
print("DataFrame Shape:", df_fees.shape)
print("\nColumns:", df_fees.columns.tolist())
print("\nFirst 5 rows:")
print(df_fees.head().to_string())

# Inspect the specific columns relevant to the question (account_type and aci)
# to understand if they are lists, strings, or mixed types.
print("\nData Types:")
print(df_fees.dtypes)

print("\nSample of 'account_type' (first 10):")
print(df_fees['account_type'].head(10).tolist())

print("\nSample of 'aci' (first 10):")
print(df_fees['aci'].head(10).tolist())
