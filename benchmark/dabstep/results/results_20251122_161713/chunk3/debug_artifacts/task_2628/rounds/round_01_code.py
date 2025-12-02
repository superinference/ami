# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2628
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2763 characters (FULL CODE)
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
file_path = '/output/chunk3/data/context/payments.csv'
df = pd.read_csv(file_path)

# Create a datetime column from 'year' and 'day_of_year' to extract the month
# Format '%Y%j' parses Year and Day of Year (001-366)
df['date'] = pd.to_datetime(df['year'].astype(str) + df['day_of_year'].astype(str).str.zfill(3), format='%Y%j')
df['month'] = df['date'].dt.month

# Define filter criteria
merchant_name = 'Belles_cookbook_store'
target_month = 6  # June

# Filter the dataframe
filtered_df = df[
    (df['merchant'] == merchant_name) & 
    (df['month'] == target_month)
]

# Calculate verification metrics based on Ground Truth provided in prompt
transaction_count = len(filtered_df)
total_volume = filtered_df['eur_amount'].sum()
fraud_volume = filtered_df[filtered_df['has_fraudulent_dispute']]['eur_amount'].sum()

# Output results
print(f"Successfully filtered data for merchant '{merchant_name}' in June (Month {target_month}).")
print(f"Transaction Count: {transaction_count}")
print(f"Total Volume: {total_volume:.2f}")
print(f"Fraud Volume: {fraud_volume:.2f}")
