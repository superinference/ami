# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2608
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2804 characters (FULL CODE)
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

# Load the payments data
file_path = '/output/chunk6/data/context/payments.csv'
df = pd.read_csv(file_path)

# Filter for the specific merchant 'Belles_cookbook_store'
merchant_name = 'Belles_cookbook_store'
df_merchant = df[df['merchant'] == merchant_name]

# Filter for April (Day of Year 91 to 120)
# 2023 is not a leap year. Jan(31)+Feb(28)+Mar(31) = 90 days. April is 91-120.
df_april = df_merchant[(df_merchant['day_of_year'] >= 91) & (df_merchant['day_of_year'] <= 120)]

# Output verification stats to confirm alignment with Ground Truth
print(f"--- Filtered Data Stats for {merchant_name} in April ---")
print(f"Row Count: {len(df_april)}")
print(f"Total Volume: {df_april['eur_amount'].sum():.2f}")
print(f"Fraud Volume: {df_april[df_april['has_fraudulent_dispute']]['eur_amount'].sum():.2f}")

print("\n--- ACI Counts ---")
print(df_april['aci'].value_counts())

print("\n--- Credit Mix ---")
print(df_april['is_credit'].value_counts())

print("\n--- Acquirer Country ---")
print(df_april['acquirer_country'].value_counts())

# Display first few rows to verify structure
print("\n--- Sample Data ---")
print(df_april.head())
