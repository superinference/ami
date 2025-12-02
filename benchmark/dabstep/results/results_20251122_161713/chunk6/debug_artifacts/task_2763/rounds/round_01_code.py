# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2763
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3577 characters (FULL CODE)
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
payments_path = '/output/chunk6/data/context/payments.csv'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

# Step 1: Load payments data
print("Loading payments data...")
df = pd.read_csv(payments_path)

# Step 2: Filter for 'Martinis_Fine_Steakhouse' and year 2023
merchant_name = 'Martinis_Fine_Steakhouse'
df_merchant = df[(df['merchant'] == merchant_name) & (df['year'] == 2023)].copy()

# Step 3: Establish transaction profile (Volume, Fraud, Counts)
# Calculate volumes
total_volume = df_merchant['eur_amount'].sum()
fraud_volume = df_merchant[df_merchant['has_fraudulent_dispute']]['eur_amount'].sum()
total_transactions = len(df_merchant)

# Calculate distributions for fee-relevant columns
credit_dist = df_merchant['is_credit'].value_counts()
aci_dist = df_merchant['aci'].value_counts()
acq_country_dist = df_merchant['acquirer_country'].value_counts()

# Step 4: Load merchant metadata (MCC, Account Type, Acquirers)
with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)
    
merchant_meta = next((item for item in merchant_data_list if item["merchant"] == merchant_name), None)

# Step 5: Output the profile
print(f"--- Transaction Profile for {merchant_name} (2023) ---")
print(f"Total Volume: {total_volume:.2f}")
print(f"Fraud Volume: {fraud_volume:.2f}")
print(f"Total Transactions: {total_transactions}")

print("\n--- Credit Distribution ---")
print(credit_dist)

print("\n--- ACI Distribution ---")
print(aci_dist)

print("\n--- Acquirer Country Distribution ---")
print(acq_country_dist)

print("\n--- Merchant Metadata ---")
if merchant_meta:
    print(json.dumps(merchant_meta, indent=2))
else:
    print("Merchant metadata not found.")

# Verify against Ground Truth
# Vol:1260227.18 Fraud:115108.53
# Credit:True 10094, False 3711
# ACI: D 6195, F 3662, G 2705, C 579, A 398, B 266
# AcqCountry: FR 13805
