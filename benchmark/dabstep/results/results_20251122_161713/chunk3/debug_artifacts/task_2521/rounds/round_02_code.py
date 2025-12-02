# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2521
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 4247 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

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

# Load Data
fees_path = '/output/chunk3/data/context/fees.json'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
payments_path = '/output/chunk3/data/context/payments.csv'

# 1. Load Fee Rule ID 1
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
fee_rule = next((fee for fee in fees_data if fee['ID'] == 1), None)

if not fee_rule:
    print("Fee ID 1 not found.")
    exit()

# Extract criteria from Fee ID 1
target_scheme = fee_rule.get('card_scheme')
target_mccs = fee_rule.get('merchant_category_code') # List of ints
target_is_credit = fee_rule.get('is_credit') # Boolean
target_aci = fee_rule.get('aci') # List of strings
target_account_types = fee_rule.get('account_type') # List, empty = wildcard

# 2. Load Merchant Data (to get MCCs)
with open(merchant_data_path, 'r') as f:
    merchant_list = json.load(f)
df_merchants = pd.DataFrame(merchant_list)

# 3. Load Payments Data
df_payments = pd.read_csv(payments_path)

# Filter for 2023
df_payments = df_payments[df_payments['year'] == 2023]

# 4. Merge Payments with Merchant Data to get MCC
# Ensure we have the MCC for each transaction
df_merged = pd.merge(df_payments, df_merchants[['merchant', 'merchant_category_code']], on='merchant', how='left')

# 5. Apply Filters based on Fee ID 1
# Filter: Card Scheme
if target_scheme:
    df_merged = df_merged[df_merged['card_scheme'] == target_scheme]

# Filter: Is Credit (Handle None/Wildcard if applicable, though schema says bool)
if target_is_credit is not None:
    df_merged = df_merged[df_merged['is_credit'] == target_is_credit]

# Filter: ACI (Handle list or wildcard)
if is_not_empty(target_aci):
    df_merged = df_merged[df_merged['aci'].isin(target_aci)]

# Filter: MCC (Handle list or wildcard)
if is_not_empty(target_mccs):
    df_merged = df_merged[df_merged['merchant_category_code'].isin(target_mccs)]

# Filter: Account Type (Handle list or wildcard)
# Note: Account type is in merchant_data, not payments directly, but we didn't merge it yet.
# Let's check if we need to filter by account type.
if is_not_empty(target_account_types):
    # We need account_type from merchant data
    df_merchants_acc = df_merchants[['merchant', 'account_type']]
    df_merged = pd.merge(df_merged, df_merchants_acc, on='merchant', how='left', suffixes=('', '_y'))
    # Handle potential duplicate columns if merge happened before
    if 'account_type_y' in df_merged.columns:
        df_merged['account_type'] = df_merged['account_type_y']
    
    df_merged = df_merged[df_merged['account_type'].isin(target_account_types)]

# 6. Get Unique Merchants
affected_merchants = df_merged['merchant'].unique()

# 7. Output Result
# Sort for consistency
affected_merchants_sorted = sorted(affected_merchants)

if len(affected_merchants_sorted) > 0:
    print(", ".join(affected_merchants_sorted))
else:
    print("None")
