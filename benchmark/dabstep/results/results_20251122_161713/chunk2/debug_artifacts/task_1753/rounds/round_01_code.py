# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1753
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2975 characters (FULL CODE)
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
payments_path = '/output/chunk2/data/context/payments.csv'
df_payments = pd.read_csv(payments_path)

# Filter for the specific merchant and date range (March 2023: Day 60-90)
# Note: 2023 is not a leap year, so March 1st is day 60. March 31st is day 90.
merchant_name = "Belles_cookbook_store"
filtered_df = df_payments[
    (df_payments['merchant'] == merchant_name) &
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] >= 60) &
    (df_payments['day_of_year'] <= 90)
]

# Display the results
print(f"Filtered transactions for {merchant_name} in March 2023:")
print(f"Total count: {len(filtered_df)}")

# Show unique combinations of attributes relevant to fee rules (as per ground truth hint)
# Fee rules often depend on: card_scheme, is_credit, aci, issuing_country, acquirer_country
relevant_cols = ['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']
unique_attributes = filtered_df[relevant_cols].drop_duplicates().sort_values(by=relevant_cols)

print("\nUnique transaction attributes (card_scheme, is_credit, aci, issuing_country, acquirer_country):")
print(unique_attributes.to_string(index=False))

# Also print a sample of the raw filtered data
print("\nSample of filtered data (first 5 rows):")
print(filtered_df.head().to_string())
