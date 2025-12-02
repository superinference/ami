# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2729
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3181 characters (FULL CODE)
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
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'
fees_path = '/output/chunk2/data/context/fees.json'

# Load payments data
print("Loading payments data...")
df_payments = pd.read_csv(payments_path)

# Filter criteria
target_merchant = 'Golfclub_Baron_Friso'
start_day = 182
end_day = 212

# Apply filters
# 1. Merchant is 'Golfclub_Baron_Friso'
# 2. Month is July (day_of_year 182-212)
# 3. Transaction is fraudulent
filtered_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['day_of_year'] >= start_day) &
    (df_payments['day_of_year'] <= end_day) &
    (df_payments['has_fraudulent_dispute'] == True)
].copy()

print(f"Filtered transactions count: {len(filtered_txs)}")

# Display sample to verify against ground truth
# Columns relevant for fee calculation: card_scheme, is_credit, eur_amount, ip_country, issuing_country, aci
cols_to_show = ['card_scheme', 'is_credit', 'eur_amount', 'issuing_country', 'ip_country', 'acquirer_country', 'aci']
print(filtered_txs[cols_to_show].head(20))

# Load context data for next steps (Fees and Merchant Data)
# This is necessary to calculate fees for the different ACI options in the next steps
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

with open(fees_path, 'r') as f:
    fees_data = json.load(f)

print(f"Loaded {len(merchant_data)} merchant records and {len(fees_data)} fee rules.")
