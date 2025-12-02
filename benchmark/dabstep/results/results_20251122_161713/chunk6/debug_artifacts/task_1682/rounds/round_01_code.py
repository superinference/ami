# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1682
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3795 characters (FULL CODE)
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

# Load payments data
print("Loading payments data...")
df_payments = pd.read_csv(payments_path)

# Filter for the specific merchant and day
target_merchant = 'Belles_cookbook_store'
target_day = 12

print(f"Filtering for merchant '{target_merchant}' on day_of_year {target_day}...")
df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['day_of_year'] == target_day)
].copy()

# Calculate 'intracountry' attribute which is needed for fee rules
# intracountry is True if issuing_country == acquirer_country
df_filtered['intracountry'] = df_filtered['issuing_country'] == df_filtered['acquirer_country']

# Identify unique transaction attributes relevant for fee matching
# Fee rules typically depend on: card_scheme, is_credit, aci, intracountry
# We also keep issuing/acquirer countries for context
relevant_columns = ['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country', 'intracountry']
unique_attributes = df_filtered[relevant_columns].drop_duplicates().sort_values(by=['card_scheme', 'aci'])

print(f"\nFound {len(unique_attributes)} unique transaction attribute combinations:")
print(unique_attributes)

# Load merchant data to get static attributes (MCC, Account Type)
print("\nLoading merchant static attributes...")
with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

merchant_info = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)

if merchant_info:
    print(f"Merchant Category Code (MCC): {merchant_info.get('merchant_category_code')}")
    print(f"Account Type: {merchant_info.get('account_type')}")
    print(f"Acquirers: {merchant_info.get('acquirer')}")
else:
    print("Merchant not found in merchant_data.json")

# Output list of dictionaries for easy copy-pasting into next steps
print("\nUnique Combinations (List of Dicts):")
combinations = unique_attributes.to_dict('records')
for combo in combinations:
    print(combo)
