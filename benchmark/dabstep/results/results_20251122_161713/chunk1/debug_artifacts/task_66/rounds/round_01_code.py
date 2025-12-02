# ═══════════════════════════════════════════════════════════
# Round 1 - Task 66
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: pending
# Plan steps: 1
# Code length: 2757 characters (FULL CODE)
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
fees_path = '/output/chunk1/data/context/fees.json'

# Load the JSON data
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Create DataFrame
df_fees = pd.DataFrame(fees_data)

# Analyze columns to identify boolean factors
boolean_columns = []

for col in df_fees.columns:
    # Get unique non-null values
    unique_vals = df_fees[col].dropna().unique()
    
    # Check if the column is explicitly boolean
    if pd.api.types.is_bool_dtype(df_fees[col]):
        boolean_columns.append(col)
    # Check if the column contains only 0/1 or 0.0/1.0 which are often used as booleans
    elif len(unique_vals) > 0 and set(unique_vals).issubset({0, 1, 0.0, 1.0}):
        boolean_columns.append(col)

print("Analysis of fees.json structure:")
print(f"Columns found: {list(df_fees.columns)}")
print(f"Identified boolean columns: {boolean_columns}")
print("\nSample data (first 5 rows):")
print(df_fees[boolean_columns].head())
print("\nUnique values in boolean columns:")
for col in boolean_columns:
    print(f"{col}: {df_fees[col].unique()}")
