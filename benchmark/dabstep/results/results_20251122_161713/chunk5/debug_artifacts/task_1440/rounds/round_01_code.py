# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1440
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 2832 characters (FULL CODE)
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

# Load the fees.json file
fees_file_path = '/output/chunk5/data/context/fees.json'

with open(fees_file_path, 'r') as f:
    fees_data = json.load(f)

df_fees = pd.DataFrame(fees_data)

# Calculate the fee for a transaction of 5000 euros for each rule
# Formula from manual: fee = fixed_amount + (rate * amount / 10000)
amount = 5000
df_fees['calculated_fee'] = df_fees['fixed_amount'] + (df_fees['rate'] * amount / 10000)

# Find the maximum fee possible for this amount
max_fee = df_fees['calculated_fee'].max()

# Filter the dataframe to find rules that result in this maximum fee
most_expensive_rules = df_fees[df_fees['calculated_fee'] == max_fee]

# Extract the MCCs associated with these rules
expensive_mccs = set()
for mccs in most_expensive_rules['merchant_category_code']:
    # Check if the list is not empty
    # (Empty list [] usually implies wildcard/all, but typically high fees are specific)
    if isinstance(mccs, list):
        for mcc in mccs:
            expensive_mccs.add(mcc)

# Convert to sorted list
result_list = sorted(list(expensive_mccs))

print(f"Max fee found: {max_fee}")
print(f"Most expensive MCCs: {result_list}")
