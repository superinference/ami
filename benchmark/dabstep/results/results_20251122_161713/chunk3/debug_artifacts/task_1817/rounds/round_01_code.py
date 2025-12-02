# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1817
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2780 characters (FULL CODE)
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

# Define file paths
payments_path = '/output/chunk3/data/context/payments.csv'

# Load the payments data
print("Loading payments data...")
df = pd.read_csv(payments_path)

# Define filter criteria
merchant_name = 'Belles_cookbook_store'
start_day = 182  # July 1st in non-leap year
end_day = 212    # July 31st in non-leap year

# Apply filters
# 1. Filter by merchant
# 2. Filter by day_of_year range for July
filtered_df = df[
    (df['merchant'] == merchant_name) & 
    (df['day_of_year'] >= start_day) & 
    (df['day_of_year'] <= end_day)
].copy()

# Verify the result
print(f"Total transactions found for {merchant_name} in July (Day {start_day}-{end_day}): {len(filtered_df)}")
print("\nSample of filtered data:")
print(filtered_df[['psp_reference', 'merchant', 'day_of_year', 'eur_amount', 'card_scheme']].head())

# Check if the count matches the pre-exploration insight (1138)
if len(filtered_df) == 1138:
    print("\nVerification Successful: Transaction count matches expected value (1138).")
else:
    print(f"\nWarning: Transaction count ({len(filtered_df)}) differs from expected value (1138).")
