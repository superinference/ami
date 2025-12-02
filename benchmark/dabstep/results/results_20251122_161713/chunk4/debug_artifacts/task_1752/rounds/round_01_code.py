# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1752
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3822 characters (FULL CODE)
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
payments_path = '/output/chunk4/data/context/payments.csv'
merchant_data_path = '/output/chunk4/data/context/merchant_data.json'

# 1. Load the payments data
print("Loading payments.csv...")
df = pd.read_csv(payments_path)

# 2. Filter for the specific merchant: 'Belles_cookbook_store'
merchant_name = 'Belles_cookbook_store'
df_merchant = df[df['merchant'] == merchant_name].copy()

# 3. Filter for February 2023
# Year must be 2023
# Day of year for Feb 2023 (non-leap year):
# Jan = 31 days. Feb 1 is day 32. Feb 28 is day 59.
df_feb = df_merchant[
    (df_merchant['year'] == 2023) & 
    (df_merchant['day_of_year'] >= 32) & 
    (df_merchant['day_of_year'] <= 59)
].copy()

# 4. Calculate derived columns needed for fee matching
# 'intracountry': True if issuing_country equals acquirer_country
df_feb['intracountry'] = df_feb['issuing_country'] == df_feb['acquirer_country']

# 5. Calculate Monthly Aggregates (Volume and Fraud)
# These are needed to determine which 'monthly_volume' and 'monthly_fraud_level' buckets apply in the fee rules.
total_volume = df_feb['eur_amount'].sum()
fraud_volume = df_feb[df_feb['has_fraudulent_dispute'] == True]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

# 6. Load Merchant Metadata (needed for Account Type and MCC matching later)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)

# 7. Output Summary
print(f"--- Analysis for {merchant_name} (Feb 2023) ---")
print(f"Transaction Count: {len(df_feb)}")
print(f"Total Volume: {total_volume:.2f}")
print(f"Fraud Volume: {fraud_volume:.2f}")
print(f"Fraud Rate: {fraud_rate:.2%}")

print("\n--- Merchant Metadata ---")
print(json.dumps(merchant_info, indent=2))

print("\n--- Unique Transaction Attributes (for Fee Matching) ---")
# Fee rules typically depend on: card_scheme, is_credit, aci, intracountry
unique_attrs = df_feb[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates().sort_values('card_scheme')
print(unique_attrs.to_string(index=False))
