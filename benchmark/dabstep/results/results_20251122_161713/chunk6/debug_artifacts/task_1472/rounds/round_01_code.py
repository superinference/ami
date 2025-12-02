# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1472
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 2818 characters (FULL CODE)
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

# Load the fees.json file into a pandas DataFrame
file_path = '/output/chunk6/data/context/fees.json'
df_fees = pd.read_json(file_path)

# Define the target criteria from the question
target_account_type = 'D'
target_aci = 'C'

# Function to filter rules based on the criteria
def is_match(row):
    # Check Account Type
    # Logic: If the list is empty, it's a wildcard (matches all). 
    # If not empty, the target must be in the list.
    account_types = row['account_type']
    # Ensure it's a list and check content
    if isinstance(account_types, list) and len(account_types) > 0:
        if target_account_type not in account_types:
            return False
    
    # Check ACI
    # Logic: If the list is empty, it's a wildcard (matches all).
    # If not empty, the target must be in the list.
    acis = row['aci']
    if isinstance(acis, list) and len(acis) > 0:
        if target_aci not in acis:
            return False
            
    return True

# Apply the filter
matching_fees = df_fees[df_fees.apply(is_match, axis=1)]

# Get the list of matching IDs
fee_ids = matching_fees['ID'].tolist()

# Print the result
print(fee_ids)
