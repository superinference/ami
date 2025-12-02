# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2581
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2846 characters (FULL CODE)
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

# Set the file path
file_path = '/output/chunk3/data/context/payments.csv'

# Load the payments data
df = pd.read_csv(file_path)

# Define the filter conditions
# Merchant: 'Golfclub_Baron_Friso'
# Timeframe: January (day_of_year 1 to 31)
merchant_condition = df['merchant'] == 'Golfclub_Baron_Friso'
january_condition = (df['day_of_year'] >= 1) & (df['day_of_year'] <= 31)

# Apply the filters
january_transactions = df[merchant_condition & january_condition]

# Print the results to verify the step
print(f"Successfully loaded and filtered data.")
print(f"Merchant: Golfclub_Baron_Friso")
print(f"Timeframe: Day 1-31 (January)")
print(f"Transaction Count: {len(january_transactions)}")

# Display a sample of the filtered data to ensure correctness
print("\nFirst 5 rows of filtered data:")
print(january_transactions[['psp_reference', 'merchant', 'day_of_year', 'card_scheme', 'eur_amount']].head())

# Verification against ground truth (Count: 2345)
if len(january_transactions) == 2345:
    print("\n✅ Verification: Row count matches expected ground truth (2345).")
else:
    print(f"\n⚠️ Verification: Row count {len(january_transactions)} differs from expected (2345).")
