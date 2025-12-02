# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1594
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3034 characters (FULL CODE)
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
fees_path = '/output/chunk6/data/context/fees.json'

# Load the fees data
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Create DataFrame
df_fees = pd.DataFrame(fees_data)

# Define filtering logic for account_type
# Logic: Keep row if account_type is empty (wildcard) OR contains 'H'
def is_applicable_account_type(account_type_list):
    # Check if list is empty (wildcard matches all)
    if not account_type_list:
        return True
    # Check if 'H' is in the list
    if 'H' in account_type_list:
        return True
    return False

# Step 1: Filter for card_scheme 'NexPay'
df_nexpay = df_fees[df_fees['card_scheme'] == 'NexPay'].copy()

# Step 2: Filter for account_type 'H' or wildcard
# We use apply because account_type is a list column
df_filtered = df_nexpay[df_nexpay['account_type'].apply(is_applicable_account_type)].copy()

# Display results to verify the step
print(f"Total fees rules: {len(df_fees)}")
print(f"NexPay rules: {len(df_nexpay)}")
print(f"Filtered rules (NexPay + Account H/Wildcard): {len(df_filtered)}")
print("\nSample of filtered data:")
print(df_filtered[['ID', 'card_scheme', 'account_type', 'fixed_amount', 'rate']].head())

# Save the filtered dataframe for the next step (optional but good practice in a pipeline, 
# here we just print as requested by the prompt structure)
