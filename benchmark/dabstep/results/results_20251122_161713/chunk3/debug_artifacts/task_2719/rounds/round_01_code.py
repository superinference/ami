# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2719
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3604 characters (FULL CODE)
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

# 1. Load payments data
print("Loading payments.csv...")
df_payments = pd.read_csv(payments_path)

# 2. Filter for specific criteria
# Merchant: 'Golfclub_Baron_Friso'
# Month: May (Day of year 121-151 for non-leap year 2023)
# Fraudulent: has_fraudulent_dispute == True
merchant_name = 'Golfclub_Baron_Friso'
start_day = 121
end_day = 151

filtered_txs = df_payments[
    (df_payments['merchant'] == merchant_name) &
    (df_payments['day_of_year'] >= start_day) &
    (df_payments['day_of_year'] <= end_day) &
    (df_payments['has_fraudulent_dispute'] == True)
].copy()

print(f"Filtered transactions for {merchant_name} in May (Fraudulent only): {len(filtered_txs)} rows")

# 3. Load merchant data to get MCC and Account Type
print("Loading merchant_data.json...")
with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

# Find the specific merchant entry
merchant_info = next((item for item in merchant_data_list if item["merchant"] == merchant_name), None)

if merchant_info:
    mcc = merchant_info.get('merchant_category_code')
    account_type = merchant_info.get('account_type')
    
    print(f"\nMerchant Context for {merchant_name}:")
    print(f"  - Merchant Category Code (MCC): {mcc}")
    print(f"  - Account Type: {account_type}")
    
    # Add these context columns to the dataframe for easier processing in next steps
    filtered_txs['merchant_category_code'] = mcc
    filtered_txs['account_type'] = account_type
else:
    print(f"Error: Merchant {merchant_name} not found in merchant_data.json")

# Display sample of the prepared data
print("\nSample of filtered transactions (first 5):")
print(filtered_txs[['psp_reference', 'eur_amount', 'card_scheme', 'is_credit', 'aci', 'merchant_category_code', 'account_type']].head())
