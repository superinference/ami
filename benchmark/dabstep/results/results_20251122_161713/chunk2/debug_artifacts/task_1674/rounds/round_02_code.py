# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1674
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 2807 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

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

# Load the fees.json file
file_path = '/output/chunk2/data/context/fees.json'
df_fees = pd.read_json(file_path)

# Define parameters
target_scheme = 'NexPay'
target_account_type = 'S'
transaction_value = 1234.0

# Filter for Card Scheme 'NexPay'
df_nexpay = df_fees[df_fees['card_scheme'] == target_scheme].copy()

# Filter for Account Type 'S' (handling wildcards)
# Logic: A rule applies if 'account_type' list contains 'S' OR if the list is empty/None (wildcard)
def is_applicable_account(account_list):
    # Check for wildcard (empty list or None)
    if not account_list: 
        return True
    # Check if specific type is in list
    if target_account_type in account_list:
        return True
    return False

df_applicable = df_nexpay[df_nexpay['account_type'].apply(is_applicable_account)].copy()

# Calculate fee for each applicable rule
# Formula from manual.md: fee = fixed_amount + rate * transaction_value / 10000
calculated_fees = []

for index, row in df_applicable.iterrows():
    fixed_amount = coerce_to_float(row['fixed_amount'])
    rate = coerce_to_float(row['rate'])
    
    fee = fixed_amount + (rate * transaction_value / 10000)
    calculated_fees.append(fee)

# Calculate average fee
if calculated_fees:
    average_fee = sum(calculated_fees) / len(calculated_fees)
    # Output formatted to 6 decimal places
    print(f"{average_fee:.6f}")
else:
    print("0.000000")
