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

# Step 1: Load the fees.json file
file_path = '/output/chunk3/data/context/fees.json'
with open(file_path, 'r') as f:
    fees_data = json.load(f)

df_fees = pd.DataFrame(fees_data)

# Step 2: Filter for 'TransactPlus' card scheme
# We look for entries where card_scheme is exactly 'TransactPlus'
df_transact = df_fees[df_fees['card_scheme'] == 'TransactPlus'].copy()

# Step 3: Filter for entries related to account type 'S'
# The 'account_type' field is a list. 
# An empty list [] acts as a wildcard (applies to all types).
# A list containing 'S' applies to 'S'.
def applies_to_account_s(account_types):
    if account_types is None:
        return True # Treat None as wildcard if present
    if len(account_types) == 0:
        return True # Empty list is wildcard
    if 'S' in account_types:
        return True # Explicit match
    return False

df_relevant_rules = df_transact[df_transact['account_type'].apply(applies_to_account_s)].copy()

# Step 4: Calculate the fee for a transaction value of 1234 EUR for each matching rule
# Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
transaction_value = 1234.0

# Ensure numeric types
df_relevant_rules['fixed_amount'] = pd.to_numeric(df_relevant_rules['fixed_amount'])
df_relevant_rules['rate'] = pd.to_numeric(df_relevant_rules['rate'])

df_relevant_rules['fee'] = df_relevant_rules['fixed_amount'] + (df_relevant_rules['rate'] * transaction_value / 10000.0)

# Step 5: Calculate the average fee
average_fee = df_relevant_rules['fee'].mean()

# Step 6: Print the result in EUR with 6 decimals
print(f"{average_fee:.6f}")