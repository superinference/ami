# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2121
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2819 characters (FULL CODE)
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
payments_path = '/output/chunk5/data/context/payments.csv'
df_payments = pd.read_csv(payments_path)

# Filter for the specific merchant
merchant_name = "Golfclub_Baron_Friso"
df_merchant = df_payments[df_payments['merchant'] == merchant_name]

# Filter for February 2023
# February in a non-leap year (2023) corresponds to days 32 to 59
# Jan = 31 days. Feb 1 = day 32. Feb 28 = day 31+28 = 59.
df_feb = df_merchant[(df_merchant['day_of_year'] >= 32) & (df_merchant['day_of_year'] <= 59)]

# Verification of the filtered data
print(f"Filtered data shape: {df_feb.shape}")
print(f"Total transactions: {len(df_feb)}")
print(f"Total volume: {df_feb['eur_amount'].sum():.2f}")

# Display first few rows to verify against ground truth
# Ground truth sample: 34.71, 7.81, 52.89, 48.95, 87.35
print("\nSample transactions (first 5):")
print(df_feb[['psp_reference', 'merchant', 'day_of_year', 'eur_amount', 'card_scheme', 'aci']].head())

# Save the filtered dataframe for the next step (simulated by returning/printing, 
# but in a real pipeline, we might pass this object)
# For this task, printing the verification is sufficient.
