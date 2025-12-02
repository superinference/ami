# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1694
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 4188 characters (FULL CODE)
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

# Load payments data
print("Loading payments data...")
df_payments = pd.read_csv(payments_path)

# Filter for the specific merchant, year, and day
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023
target_day = 12

print(f"Filtering for Merchant: {target_merchant}, Year: {target_year}, Day: {target_day}")
filtered_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
].copy()

# Load merchant data to get MCC and Account Type (critical for fee rules)
print("Loading merchant data...")
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
df_merchant_data = pd.DataFrame(merchant_data)

# Merge payments with merchant data
# This adds 'merchant_category_code' and 'account_type' to each transaction
df_merged = pd.merge(
    filtered_txs, 
    df_merchant_data[['merchant', 'merchant_category_code', 'account_type']], 
    on='merchant', 
    how='left'
)

# Calculate 'intracountry' status (Required for fee rules)
# True if issuing_country matches acquirer_country
df_merged['intracountry'] = df_merged['issuing_country'] == df_merged['acquirer_country']

# Display summary of identified transactions
print(f"\nTotal transactions found: {len(df_merged)}")

# Display unique characteristics relevant to fee matching
# Fee rules typically depend on: card_scheme, is_credit, aci, mcc, account_type, intracountry
print("\nUnique Transaction Characteristics (Scheme, Credit, ACI, Intracountry):")
unique_chars = df_merged[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates().sort_values(by=['card_scheme', 'aci'])
print(unique_chars.to_string(index=False))

# Display Merchant-specific static characteristics
mcc = df_merged['merchant_category_code'].iloc[0] if not df_merged.empty else "N/A"
account_type = df_merged['account_type'].iloc[0] if not df_merged.empty else "N/A"
print(f"\nMerchant Static Characteristics:")
print(f"Merchant Category Code (MCC): {mcc}")
print(f"Account Type: {account_type}")

# Show a sample of the enriched transaction data
print("\nSample Enriched Transactions (First 5):")
cols_to_show = ['psp_reference', 'card_scheme', 'eur_amount', 'aci', 'is_credit', 'issuing_country', 'acquirer_country', 'intracountry']
print(df_merged[cols_to_show].head().to_string(index=False))
