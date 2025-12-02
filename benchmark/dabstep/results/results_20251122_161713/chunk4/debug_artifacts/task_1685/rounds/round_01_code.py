# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1685
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2626 characters (FULL CODE)
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
df_payments = pd.read_csv(payments_path)

# Filter the rows where `merchant` is 'Belles_cookbook_store', `day_of_year` is 300, and `year` is 2023
filtered_df = df_payments[
    (df_payments['merchant'] == 'Belles_cookbook_store') &
    (df_payments['day_of_year'] == 300) &
    (df_payments['year'] == 2023)
]

# Print the result to verify
print(f"Number of rows after filtering: {len(filtered_df)}")
print("\nFirst 5 rows of filtered data:")
print(filtered_df.head())

# Print unique values for columns likely needed for fee identification in the next step
print("\nUnique attributes in filtered transactions:")
print(f"Card Schemes: {filtered_df['card_scheme'].unique()}")
print(f"ACI codes: {filtered_df['aci'].unique()}")
print(f"Is Credit: {filtered_df['is_credit'].unique()}")
print(f"Issuing Countries: {filtered_df['issuing_country'].unique()}")
