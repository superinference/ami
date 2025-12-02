# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2764
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 4724 characters (FULL CODE)
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
payments_path = '/output/chunk5/data/context/payments.csv'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

# 1. Load Payments Data
print("Loading payments data...")
df = pd.read_csv(payments_path)

# 2. Filter for 'Martinis_Fine_Steakhouse' and Year 2023
merchant_name = 'Martinis_Fine_Steakhouse'
target_year = 2023

df_merchant = df[
    (df['merchant'] == merchant_name) & 
    (df['year'] == target_year)
].copy()

print(f"Filtered data for {merchant_name} in {target_year}. Transaction count: {len(df_merchant)}")

# 3. Calculate Transaction Profile
# A. Volume Metrics
# Fee rules use 'monthly_volume'. We calculate the total annual volume and average it per month.
total_volume_2023 = df_merchant['eur_amount'].sum()
avg_monthly_volume = total_volume_2023 / 12.0  # Assuming full year data for 2023

# B. Fraud Metrics
# Manual: "Fraud is defined as the ratio of fraudulent volume over total volume."
# Fee rules use 'monthly_fraud_level'. We calculate the overall rate.
fraud_txs = df_merchant[df_merchant['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume_2023 if total_volume_2023 > 0 else 0.0

# C. Average Transaction Amount
avg_amount = df_merchant['eur_amount'].mean()

# D. Additional Profile Metrics for Fee Rules
# Credit Ratio (for 'is_credit' rule)
credit_ratio = df_merchant['is_credit'].mean()

# Intracountry Ratio (for 'intracountry' rule)
# Intracountry = True if issuing_country == acquirer_country
df_merchant['is_intracountry'] = df_merchant['issuing_country'] == df_merchant['acquirer_country']
intra_ratio = df_merchant['is_intracountry'].mean()

# ACI Distribution (for 'aci' rule)
aci_distribution = df_merchant['aci'].value_counts(normalize=True).to_dict()

# 4. Retrieve Metadata from merchant_data.json
print("Loading merchant metadata...")
with open(merchant_data_path, 'r') as f:
    merchant_metadata_list = json.load(f)

# Find the specific merchant
merchant_meta = next((item for item in merchant_metadata_list if item["merchant"] == merchant_name), None)

if merchant_meta:
    mcc = merchant_meta.get('merchant_category_code')
    account_type = merchant_meta.get('account_type')
    acquirers = merchant_meta.get('acquirer')
else:
    mcc = None
    account_type = None
    acquirers = []
    print("Warning: Merchant metadata not found.")

# 5. Output Results
print("\n--- Merchant Transaction Profile (2023) ---")
print(f"Merchant Name: {merchant_name}")
print(f"Total Transactions: {len(df_merchant)}")
print(f"Total Volume (2023): €{total_volume_2023:,.2f}")
print(f"Avg Monthly Volume: €{avg_monthly_volume:,.2f}")
print(f"Fraud Rate (Volume): {fraud_rate:.4%}")
print(f"Avg Transaction Amount: €{avg_amount:.4f}")
print(f"Credit Ratio: {credit_ratio:.4f}")
print(f"Intracountry Ratio: {intra_ratio:.4f}")
print(f"ACI Distribution: {aci_distribution}")

print("\n--- Merchant Metadata ---")
print(f"MCC: {mcc}")
print(f"Account Type: {account_type}")
print(f"Acquirers: {acquirers}")
