# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1705
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 4407 characters (FULL CODE)
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

# Load the payments data
payments_path = '/output/chunk3/data/context/payments.csv'
df_payments = pd.read_csv(payments_path)

# Filter for the specific merchant, year, and day
merchant_name = 'Rafa_AI'
target_year = 2023
target_day = 10

# 1. Filter for Rafa_AI and Year 2023 first to calculate monthly stats
df_merchant_2023 = df_payments[
    (df_payments['merchant'] == merchant_name) & 
    (df_payments['year'] == target_year)
].copy()

# 2. Calculate Monthly Stats for January (Day 10 is in Jan, days 1-31)
# Fee rules often depend on monthly volume and fraud levels
df_jan = df_merchant_2023[(df_merchant_2023['day_of_year'] >= 1) & (df_merchant_2023['day_of_year'] <= 31)]

monthly_volume = df_jan['eur_amount'].sum()
fraud_volume = df_jan[df_jan['has_fraudulent_dispute'] == True]['eur_amount'].sum()
monthly_fraud_rate = (fraud_volume / monthly_volume) if monthly_volume > 0 else 0.0

# 3. Filter for the specific day (Day 10) to get the transactions to match
df_target_day = df_merchant_2023[df_merchant_2023['day_of_year'] == target_day].copy()

# 4. Load Merchant Data to get static attributes (account_type, mcc, capture_delay)
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# Convert to DataFrame for merging
df_merchant_meta = pd.DataFrame(merchant_data)

# Merge merchant metadata onto the transactions
df_merged = pd.merge(df_target_day, df_merchant_meta, on='merchant', how='left')

# 5. Calculate derived attributes needed for fee matching
# 'intracountry': True if issuing_country == acquirer_country
df_merged['intracountry'] = df_merged['issuing_country'] == df_merged['acquirer_country']

# 6. Extract unique combinations of attributes relevant for fee matching
# Relevant columns based on fees.json keys:
# card_scheme, is_credit, aci, merchant_category_code, account_type, capture_delay, intracountry
# We also keep issuing/acquirer country for context/verification
cols_to_keep = [
    'card_scheme', 
    'is_credit', 
    'aci', 
    'issuing_country', 
    'acquirer_country', 
    'intracountry',
    'merchant_category_code',
    'account_type',
    'capture_delay'
]

unique_attributes = df_merged[cols_to_keep].drop_duplicates().sort_values(by=['card_scheme', 'is_credit', 'aci'])

# Print results
print(f"--- Monthly Stats for {merchant_name} (Jan {target_year}) ---")
print(f"Monthly Volume: €{monthly_volume:,.2f}")
print(f"Monthly Fraud Rate: {monthly_fraud_rate:.4%}")
print("\n--- Unique Transaction Attributes for Day 10 ---")
print(unique_attributes.to_string(index=False))

# Save to a variable or file if this were a multi-step pipeline, 
# but here we print for the next step to use.
