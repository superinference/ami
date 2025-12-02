# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1780
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2888 characters (FULL CODE)
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

# Load the payments dataset
file_path = '/output/chunk6/data/context/payments.csv'
df = pd.read_csv(file_path)

# Define filter criteria
merchant_name = 'Golfclub_Baron_Friso'
# June 2023 corresponds to days 152 to 181 (2023 is not a leap year)
# Jan(31)+Feb(28)+Mar(31)+Apr(30)+May(31) = 151 days. June starts day 152.
start_day = 152
end_day = 181

# Apply filters
# 1. Filter by merchant
# 2. Filter by day_of_year range for June
filtered_df = df[
    (df['merchant'] == merchant_name) & 
    (df['day_of_year'] >= start_day) & 
    (df['day_of_year'] <= end_day)
]

# Display results
print(f"Successfully loaded and filtered data for {merchant_name} in June 2023.")
print(f"Total transactions found: {len(filtered_df)}")
print("\nSample of filtered data:")
print(filtered_df.head())

# Display unique values for columns likely relevant to Fee ID matching in the next step
print("\nKey characteristics of these transactions (for Fee ID matching):")
print(f"Unique Card Schemes: {filtered_df['card_scheme'].unique()}")
print(f"Unique ACI codes: {filtered_df['aci'].unique()}")
print(f"Unique Account Types (need join later): Not in payments.csv")
print(f"Unique Is_Credit values: {filtered_df['is_credit'].unique()}")
