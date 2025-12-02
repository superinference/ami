# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1789
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3236 characters (FULL CODE)
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

# Filter for merchant "Martinis_Fine_Steakhouse"
merchant_name = "Martinis_Fine_Steakhouse"
df_merchant = df_payments[df_payments['merchant'] == merchant_name]

# Filter for March 2023 (Year 2023, Day of Year 60-90)
# 2023 is a non-leap year. Jan=31, Feb=28 (Total 59). March 1st is Day 60. March 31st is Day 90.
df_march_2023 = df_merchant[
    (df_merchant['year'] == 2023) & 
    (df_merchant['day_of_year'] >= 60) & 
    (df_merchant['day_of_year'] <= 90)
]

print(f"Total transactions for {merchant_name} in March 2023: {len(df_march_2023)}")

# Display a sample of the filtered data to verify
print("\nSample of filtered transactions:")
print(df_march_2023[['psp_reference', 'merchant', 'year', 'day_of_year', 'card_scheme', 'eur_amount', 'aci']].head())

# Load merchant data and fees for context (needed for next steps)
print("\nLoading merchant data and fees...")
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
    
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Find specific merchant details
specific_merchant_info = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
print(f"\nMerchant Metadata for {merchant_name}:")
print(json.dumps(specific_merchant_info, indent=2))
