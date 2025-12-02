# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2758
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 4238 characters (FULL CODE)
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

# Load the payments dataset
print("Loading payments data...")
df = pd.read_csv(payments_path)

# Filter for 'Belles_cookbook_store' and year 2023
merchant_name = 'Belles_cookbook_store'
target_year = 2023

df_merchant = df[
    (df['merchant'] == merchant_name) & 
    (df['year'] == target_year)
].copy()

print(f"Filtered data for {merchant_name} in {target_year}. Row count: {len(df_merchant)}")

# Load merchant metadata to get static characteristics (MCC, Account Type)
with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

# Find merchant specific metadata
merchant_metadata = next((item for item in merchant_data_list if item["merchant"] == merchant_name), None)

# --- Analyze Characteristics Required for Fee Calculation ---

# 1. Volume and Fraud Stats
total_volume = df_merchant['eur_amount'].sum()
fraud_volume = df_merchant[df_merchant['has_fraudulent_dispute'] == True]['eur_amount'].sum()
fraud_rate_vol = (fraud_volume / total_volume) if total_volume > 0 else 0.0

# 2. Intracountry vs Intercountry
# Logic: Intracountry if issuing_country == acquirer_country
# Note: acquirer_country is available directly in payments.csv
df_merchant['is_intracountry'] = df_merchant['issuing_country'] == df_merchant['acquirer_country']
intracountry_count = df_merchant['is_intracountry'].sum()
intercountry_count = (~df_merchant['is_intracountry']).sum()

# 3. Unique Characteristics for Fee Rules
aci_present = df_merchant['aci'].unique()
credit_present = df_merchant['is_credit'].unique()
schemes_present = df_merchant['card_scheme'].unique()

# Output Analysis Results
print("\n--- Transaction Analysis Summary ---")
print(f"Total Transaction Volume (EUR): {total_volume:.2f}")
print(f"Total Fraud Volume (EUR): {fraud_volume:.2f}")
print(f"Fraud Rate (by Volume): {fraud_rate_vol:.2%}")
print(f"Intracountry Transactions: {intracountry_count}")
print(f"Intercountry Transactions: {intercountry_count}")

print("\n--- Fee Determinants Present ---")
print(f"ACI Codes: {list(aci_present)}")
print(f"Credit Indicators: {list(credit_present)}")
print(f"Card Schemes: {list(schemes_present)}")

if merchant_metadata:
    print("\n--- Merchant Metadata ---")
    print(f"Merchant Category Code (MCC): {merchant_metadata.get('merchant_category_code')}")
    print(f"Account Type: {merchant_metadata.get('account_type')}")
    print(f"Acquirers: {merchant_metadata.get('acquirer')}")
