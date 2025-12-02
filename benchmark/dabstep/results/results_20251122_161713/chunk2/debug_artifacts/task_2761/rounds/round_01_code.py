# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2761
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2935 characters (FULL CODE)
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

# Define file paths
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'

# Load payments data
df_payments = pd.read_csv(payments_path)

# Filter for merchant 'Golfclub_Baron_Friso' and year 2023
merchant_name = 'Golfclub_Baron_Friso'
target_year = 2023

filtered_payments = df_payments[
    (df_payments['merchant'] == merchant_name) & 
    (df_payments['year'] == target_year)
]

# Calculate average eur_amount
average_amount = filtered_payments['eur_amount'].mean()

# Load merchant metadata
with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

# Retrieve specific metadata for the merchant
merchant_metadata = {}
for entry in merchant_data_list:
    if entry['merchant'] == merchant_name:
        merchant_metadata = {
            'merchant_category_code': entry.get('merchant_category_code'),
            'account_type': entry.get('account_type'),
            'acquirer': entry.get('acquirer')
        }
        break

# Output the results
print(f"Merchant: {merchant_name}")
print(f"Year: {target_year}")
print(f"Transaction Count: {len(filtered_payments)}")
print(f"Average EUR Amount: {average_amount}")
print(f"Metadata: {merchant_metadata}")
