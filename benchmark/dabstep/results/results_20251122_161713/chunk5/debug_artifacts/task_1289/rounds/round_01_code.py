# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1289
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2747 characters (FULL CODE)
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
df = pd.read_csv(payments_path)

# Filter for rows where card_scheme is 'GlobalCard' and is_credit is True
# We create a copy to avoid SettingWithCopyWarning when adding new columns
df_filtered = df[
    (df['card_scheme'] == 'GlobalCard') & 
    (df['is_credit'] == True)
].copy()

# Create a new column 'is_intracountry'
# True if issuing_country equals acquirer_country, else False
df_filtered['is_intracountry'] = df_filtered['issuing_country'] == df_filtered['acquirer_country']

# Verify the operation by printing the shape and a sample of the new dataframe
print(f"Original rows: {len(df)}")
print(f"Filtered rows (GlobalCard + Credit): {len(df_filtered)}")
print("\nSample of created 'is_intracountry' column:")
print(df_filtered[['merchant', 'issuing_country', 'acquirer_country', 'is_intracountry']].head())

# Verify distribution matches expectations for next steps (grouping)
print("\nDistribution of Intracountry transactions:")
print(df_filtered['is_intracountry'].value_counts())
