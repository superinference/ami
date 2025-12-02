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
fees_file_path = '/output/chunk2/data/context/fees.json'

with open(fees_file_path, 'r') as f:
    fees_data = json.load(f)

df_fees = pd.DataFrame(fees_data)

# Inspect the dataframe columns relevant to the calculation
print("Inspecting fee rules (first 5 rows):")
print(df_fees[['fixed_amount', 'rate', 'merchant_category_code']].head())
print("-" * 30)

# Calculate the cost for a 10 euro transaction for each rule
# Formula from manual: fee = fixed_amount + (rate / 10000 * amount)
amount = 10.0
df_fees['transaction_cost'] = df_fees['fixed_amount'] + (df_fees['rate'] / 10000 * amount)

# Find the maximum transaction cost found in the rules
max_cost = df_fees['transaction_cost'].max()

# Identify the rules that result in this maximum cost
expensive_rules = df_fees[df_fees['transaction_cost'] == max_cost]

# Extract the MCCs from these most expensive rules
expensive_mccs = []
for mccs in expensive_rules['merchant_category_code']:
    if isinstance(mccs, list):
        expensive_mccs.extend(mccs)
    elif mccs is None:
        expensive_mccs.append("Any (Wildcard)")
    else:
        expensive_mccs.append(mccs)

# Remove duplicates and sort the list of MCCs
unique_expensive_mccs = sorted(list(set(expensive_mccs)))

print(f"Maximum transaction cost for 10 EUR: {max_cost}")
print(f"Most expensive MCCs: {unique_expensive_mccs}")