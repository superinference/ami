# ═══════════════════════════════════════════════════════════
# Round 1 - Task 341
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2346 characters (FULL CODE)
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

# Define file path
payments_path = '/output/chunk4/data/context/payments.csv'

# Load the payments data
df = pd.read_csv(payments_path)

# Apply filters as specified in the plan
# Merchant: Crossfit_Hanna
# Card Scheme: NexPay
# Day of Year: 244 to 304 (September 1st to October 31st)
filtered_df = df[
    (df['merchant'] == 'Crossfit_Hanna') &
    (df['card_scheme'] == 'NexPay') &
    (df['day_of_year'] >= 244) &
    (df['day_of_year'] <= 304)
]

# Display information about the filtered dataframe to verify the step
print(f"Original row count: {len(df)}")
print(f"Filtered row count: {len(filtered_df)}")
print("\nFirst 5 rows of filtered data:")
print(filtered_df.head())
