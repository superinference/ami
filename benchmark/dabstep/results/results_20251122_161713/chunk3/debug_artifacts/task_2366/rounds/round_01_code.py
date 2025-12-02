# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2366
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2770 characters (FULL CODE)
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

# Set display options for better visibility of the output
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Define the file path
payments_path = '/output/chunk3/data/context/payments.csv'

# Load the payments dataset
print(f"Loading payments data from {payments_path}...")
df = pd.read_csv(payments_path)

# Define filter criteria
merchant_name = 'Rafa_AI'
target_year = 2023
# February 2023 corresponds to days 32 to 59 (Non-leap year: Jan=31, Feb=28)
start_day = 32
end_day = 59

# Apply filters
print(f"Filtering for merchant '{merchant_name}' in February {target_year} (Day of year {start_day}-{end_day})...")
filtered_df = df[
    (df['merchant'] == merchant_name) &
    (df['year'] == target_year) &
    (df['day_of_year'] >= start_day) &
    (df['day_of_year'] <= end_day)
].copy()

# Print the results
print("-" * 30)
print(f"FILTERING RESULTS")
print("-" * 30)
print(f"Transactions found: {len(filtered_df)}")
print(f"Total Volume (EUR): {filtered_df['eur_amount'].sum():.2f}")
print("-" * 30)
print("First 5 rows of filtered data:")
print(filtered_df.head())
