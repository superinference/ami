# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1645
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3215 characters (FULL CODE)
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
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'
payments_path = '/output/chunk6/data/context/payments.csv'

# 1. Load fees.json to inspect fee rules
print("Loading fees.json...")
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
df_fees = pd.DataFrame(fees_data)

# Inspect the fees dataframe structure
print(f"Fees loaded: {df_fees.shape}")
print(df_fees.head())

# 2. Load merchant_data.json (Context: Needed to identify merchants with Account Type 'F')
print("\nLoading merchant_data.json...")
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
df_merchants = pd.DataFrame(merchant_data)
print(f"Merchants loaded: {df_merchants.shape}")

# 3. Load payments.csv (Context: Needed to simulate transactions for the average fee calculation)
print("\nLoading payments.csv...")
df_payments = pd.read_csv(payments_path)
print(f"Payments loaded: {df_payments.shape}")

# Preliminary Inspection for the specific question
# Identify merchants with Account Type F
merchants_type_f = df_merchants[df_merchants['account_type'] == 'F']['merchant'].tolist()
print(f"\nMerchants with Account Type F: {merchants_type_f}")

# Inspect fee rules specifically for GlobalCard (as requested in the goal)
print("\nSample of GlobalCard Fee Rules:")
global_card_rules = df_fees[df_fees['card_scheme'] == 'GlobalCard']
print(global_card_rules[['ID', 'account_type', 'fixed_amount', 'rate', 'aci', 'merchant_category_code']].head())
