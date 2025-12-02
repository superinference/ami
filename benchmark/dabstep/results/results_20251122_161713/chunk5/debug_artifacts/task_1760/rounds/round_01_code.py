# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1760
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3375 characters (FULL CODE)
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

# Set display options to ensure all columns are visible if needed
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Load the payments dataset
file_path = '/output/chunk5/data/context/payments.csv'
df = pd.read_csv(file_path)

# Define filter criteria for Belles_cookbook_store in October 2023
# October 2023 corresponds to day_of_year 274 to 304 (non-leap year)
target_merchant = 'Belles_cookbook_store'
target_year = 2023
oct_start_day = 274
oct_end_day = 304

# Filter the transactions
filtered_df = df[
    (df['merchant'] == target_merchant) &
    (df['year'] == target_year) &
    (df['day_of_year'] >= oct_start_day) &
    (df['day_of_year'] <= oct_end_day)
]

# Output the result summary
print(f"--- Filtering Results ---")
print(f"Merchant: {target_merchant}")
print(f"Time Period: October {target_year} (Day {oct_start_day} to {oct_end_day})")
print(f"Total Transactions Found: {len(filtered_df)}")

# Display a sample of the filtered data to verify columns relevant for fee calculation
# (card_scheme, is_credit, aci, eur_amount, issuing_country, acquirer_country)
print("\n--- Sample Transactions (First 5) ---")
cols_to_show = ['psp_reference', 'day_of_year', 'card_scheme', 'is_credit', 'aci', 'eur_amount', 'issuing_country', 'acquirer_country']
print(filtered_df[cols_to_show].head())

# Verify unique values in key columns for the next analysis steps
print("\n--- Unique Values in Filtered Data ---")
print(f"Card Schemes: {filtered_df['card_scheme'].unique()}")
print(f"ACI Codes: {filtered_df['aci'].unique()}")
print(f"Issuing Countries: {filtered_df['issuing_country'].unique()}")
print(f"Acquirer Countries: {filtered_df['acquirer_country'].unique()}")
