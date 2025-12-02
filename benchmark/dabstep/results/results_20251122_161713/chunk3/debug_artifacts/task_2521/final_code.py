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

def is_not_empty(array):
    """Check if array/list is not empty. Handles numpy arrays safely."""
    if array is None:
        return False
    if isinstance(array, list):
        return len(array) > 0
    if hasattr(array, 'size'):  # numpy array
        return array.size > 0
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
# Note: Null or empty list implies wildcard (no filter)
target_scheme = fee_rule.get('card_scheme')
target_mccs = fee_rule.get('merchant_category_code') # List of ints
target_is_credit = fee_rule.get('is_credit') # Boolean or None
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

# 4. Apply Transaction-level Filters (Scheme, Credit, ACI)
# Filter: Card Scheme
if target_scheme:
    df_payments = df_payments[df_payments['card_scheme'] == target_scheme]

# Filter: Is Credit
# Handle boolean explicitly. If None, it's a wildcard.
if target_is_credit is not None:
    df_payments = df_payments[df_payments['is_credit'] == target_is_credit]

# Filter: ACI
# If list is provided and not empty, filter by it. Empty list/None = wildcard.
if is_not_empty(target_aci):
    df_payments = df_payments[df_payments['aci'].isin(target_aci)]

# 5. Merge with Merchant Data for MCC check
# We merge only the remaining transactions to check merchant-specific properties
df_merged = pd.merge(df_payments, df_merchants[['merchant', 'merchant_category_code', 'account_type']], on='merchant', how='left')

# 6. Apply Merchant-level Filters (MCC, Account Type)
# Filter: MCC
if is_not_empty(target_mccs):
    df_merged = df_merged[df_merged['merchant_category_code'].isin(target_mccs)]

# Filter: Account Type
if is_not_empty(target_account_types):
    df_merged = df_merged[df_merged['account_type'].isin(target_account_types)]

# 7. Get Unique Merchants
affected_merchants = sorted(df_merged['merchant'].unique())

# 8. Output Result
if len(affected_merchants) > 0:
    print(", ".join(affected_merchants))
else:
    print("None")