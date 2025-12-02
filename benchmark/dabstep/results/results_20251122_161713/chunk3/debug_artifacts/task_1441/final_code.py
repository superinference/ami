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
fees_file_path = '/output/chunk3/data/context/fees.json'

# Load the fees data
with open(fees_file_path, 'r') as f:
    fees_data = json.load(f)

# Create DataFrame
df_fees = pd.DataFrame(fees_data)

# Display basic info to verify load
print("Fees DataFrame loaded successfully.")
print(f"Shape: {df_fees.shape}")
print(df_fees.head())

# --- Analysis for the Goal ---
# Question: What is the most expensive MCC for a transaction of 10000 euros?
# Formula: fee = fixed_amount + (rate * amount / 10000)
transaction_amount = 10000

# Calculate fee for each rule
df_fees['calculated_fee'] = df_fees['fixed_amount'] + (df_fees['rate'] * transaction_amount / 10000)

# Find the maximum fee value
max_fee = df_fees['calculated_fee'].max()

# Filter for rules that result in this maximum fee
expensive_rules = df_fees[df_fees['calculated_fee'] == max_fee]

# Extract Merchant Category Codes (MCCs) from these rules
expensive_mccs = []
for index, row in expensive_rules.iterrows():
    mcc_entry = row['merchant_category_code']
    # Check if it's a list and not empty (specific MCCs)
    if isinstance(mcc_entry, list) and len(mcc_entry) > 0:
        expensive_mccs.extend(mcc_entry)
    elif mcc_entry is None or (isinstance(mcc_entry, list) and len(mcc_entry) == 0):
        # If a wildcard rule is the most expensive, note it (though usually high fees are specific)
        print("Warning: One of the most expensive rules is a wildcard (applies to all MCCs).")

# Remove duplicates and sort
unique_expensive_mccs = sorted(list(set(expensive_mccs)))

print("\n--- Results ---")
print(f"Highest calculated fee for 10,000 EUR: {max_fee}")
print(f"Number of rules with this fee: {len(expensive_rules)}")
print(f"Most expensive MCCs: {unique_expensive_mccs}")