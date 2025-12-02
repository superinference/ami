# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2578
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2872 characters (FULL CODE)
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

# Set file path
payments_path = '/output/chunk6/data/context/payments.csv'

# Load the payments dataset
df = pd.read_csv(payments_path)

# Filter for the specific merchant 'Belles_cookbook_store'
merchant_filter = df['merchant'] == 'Belles_cookbook_store'

# Filter for the month of January (Day of Year 1 to 31)
date_filter = (df['day_of_year'] >= 1) & (df['day_of_year'] <= 31)

# Apply both filters
filtered_df = df[merchant_filter & date_filter].copy()

# Output verification stats to confirm alignment with Ground Truth
# Ground Truth expectation: Total: 1201, AvgAmt: 94.31, Credit_Count: 891
print(f"Filtered Data Shape: {filtered_df.shape}")
print(f"Total Transactions: {len(filtered_df)}")
print(f"Average EUR Amount: {filtered_df['eur_amount'].mean():.2f}")
print(f"Credit Transactions Count: {filtered_df['is_credit'].sum()}")

# Check for intracountry transactions (Issuing Country == Acquirer Country)
# Ground Truth expectation: Intra_Count: 0
intra_count = (filtered_df['issuing_country'] == filtered_df['acquirer_country']).sum()
print(f"Intracountry Transactions: {intra_count}")

# Display first 5 rows for inspection
print("\nFirst 5 rows of filtered data:")
print(filtered_df.head())
