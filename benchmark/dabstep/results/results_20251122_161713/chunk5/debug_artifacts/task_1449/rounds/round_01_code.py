# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1449
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2474 characters (FULL CODE)
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
fees_path = '/output/chunk5/data/context/fees.json'

# Load the JSON data
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Convert to DataFrame
df_fees = pd.DataFrame(fees_data)

# Filter for 'SwiftCharge' card scheme
# And filter for rules applicable to credit transactions (is_credit is True or None/NaN)
# Note: None in JSON becomes None or NaN in pandas depending on dtype
filtered_fees = df_fees[
    (df_fees['card_scheme'] == 'SwiftCharge') & 
    ((df_fees['is_credit'] == True) | (df_fees['is_credit'].isnull()))
]

# Inspect the resulting dataframe
print("Filtered Fee Rules for SwiftCharge (Credit):")
print(filtered_fees[['ID', 'card_scheme', 'is_credit', 'aci', 'fixed_amount', 'rate']].head())
print(f"\nTotal rules found: {len(filtered_fees)}")
