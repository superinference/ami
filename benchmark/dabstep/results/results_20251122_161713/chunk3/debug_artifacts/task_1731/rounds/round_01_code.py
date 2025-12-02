# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1731
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3785 characters (FULL CODE)
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
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'

# 1. Load the payments dataset
df_payments = pd.read_csv(payments_path)

# 2. Filter for the specific merchant, year, and day of year
# Goal: "Martinis_Fine_Steakhouse", year 2023, day_of_year 100
target_merchant = "Martinis_Fine_Steakhouse"
target_year = 2023
target_day = 100

filtered_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
].copy()

# 3. Load merchant data to get necessary fee calculation fields (Account Type, MCC)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# Convert merchant data to a dictionary for easy lookup
# Structure: {merchant_name: {data}}
merchant_lookup = {item['merchant']: item for item in merchant_data}

# 4. Enrich the filtered transactions with merchant metadata
# We need 'account_type' and 'merchant_category_code' for fee rules
if target_merchant in merchant_lookup:
    m_data = merchant_lookup[target_merchant]
    filtered_txs['account_type'] = m_data.get('account_type')
    filtered_txs['merchant_category_code'] = m_data.get('merchant_category_code')
else:
    print(f"Warning: Merchant {target_merchant} not found in merchant_data.json")

# 5. Calculate 'intracountry' field required for fee rules
# Rule: True if issuing_country == acquirer_country
filtered_txs['intracountry'] = filtered_txs['issuing_country'] == filtered_txs['acquirer_country']

# 6. Display results
print(f"Filtered transactions for {target_merchant} on Day {target_day}, {target_year}:")
print(f"Row count: {len(filtered_txs)}")
print("\nSample columns needed for fee calculation:")
cols_to_show = ['merchant', 'card_scheme', 'eur_amount', 'aci', 'is_credit', 'intracountry', 'account_type', 'merchant_category_code']
print(filtered_txs[cols_to_show].head())

# Save for next step (optional, but good practice in pipelines)
filtered_txs.to_csv('filtered_transactions_day_100.csv', index=False)
