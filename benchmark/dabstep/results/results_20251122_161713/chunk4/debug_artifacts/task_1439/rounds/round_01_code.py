# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1439
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 3122 characters (FULL CODE)
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
fees_file_path = '/output/chunk4/data/context/fees.json'

# Load the fees data
with open(fees_file_path, 'r') as f:
    fees_data = json.load(f)

# Convert to DataFrame
df_fees = pd.DataFrame(fees_data)

# Define the transaction amount
transaction_amount = 1000.0

# Calculate the fee for each rule based on the formula: fee = fixed_amount + (rate / 10000 * amount)
# We use the 'rate' and 'fixed_amount' columns directly.
# Note: rate is in basis points (per 10000) as per manual.
df_fees['calculated_fee_1000eur'] = df_fees['fixed_amount'] + (df_fees['rate'] / 10000 * transaction_amount)

# Find the maximum fee value
max_fee = df_fees['calculated_fee_1000eur'].max()

# Filter the rules that result in this maximum fee
most_expensive_rules = df_fees[df_fees['calculated_fee_1000eur'] == max_fee]

# Extract the Merchant Category Codes (MCCs) associated with these most expensive rules
expensive_mccs = []
for index, row in most_expensive_rules.iterrows():
    mccs = row['merchant_category_code']
    if mccs is None:
        expensive_mccs.append("All (Wildcard Rule)")
    elif isinstance(mccs, list):
        expensive_mccs.extend(mccs)
    else:
        expensive_mccs.append(mccs)

# Remove duplicates and sort the list of MCCs
unique_expensive_mccs = sorted(list(set(expensive_mccs)))

# Output the results
print(f"Maximum Fee for 1000 EUR: {max_fee}")
print(f"Most Expensive MCCs: {unique_expensive_mccs}")
