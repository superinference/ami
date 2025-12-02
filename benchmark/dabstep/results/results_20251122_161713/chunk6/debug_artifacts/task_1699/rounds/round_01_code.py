# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1699
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2792 characters (FULL CODE)
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

# Load the dataset
df = pd.read_csv(payments_path)

# Define filter criteria
merchant_name = "Martinis_Fine_Steakhouse"
target_year = 2023
target_day = 10

# Filter for the specific merchant and date
filtered_df = df[
    (df['merchant'] == merchant_name) &
    (df['year'] == target_year) &
    (df['day_of_year'] == target_day)
]

# Select relevant columns for fee identification
# These are the transaction-level attributes that vary and affect fee rules
columns_of_interest = [
    'card_scheme', 
    'is_credit', 
    'aci', 
    'issuing_country', 
    'acquirer_country'
]

# Extract unique combinations
unique_characteristics = filtered_df[columns_of_interest].drop_duplicates()

# Sort for readability
unique_characteristics = unique_characteristics.sort_values(by=columns_of_interest)

# Display the results
print(f"Unique transaction characteristics for {merchant_name} on Day {target_day}, {target_year}:")
print(unique_characteristics.to_string(index=False))
print(f"\nTotal unique combinations found: {len(unique_characteristics)}")
