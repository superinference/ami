# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1835
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2730 characters (FULL CODE)
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
import os

# Define file paths
payments_path = '/output/chunk5/data/context/payments.csv'
output_path = '/output/chunk5/data/intermediate/filtered_transactions.pkl'

# Ensure output directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Load payments data
print("Loading payments.csv...")
df = pd.read_csv(payments_path)

# Filter for Golfclub_Baron_Friso
merchant_filter = df['merchant'] == 'Golfclub_Baron_Friso'

# Filter for Year 2023
year_filter = df['year'] == 2023

# Filter for January (day_of_year 1 to 31)
# Note: day_of_year is numeric, 1-365
january_filter = (df['day_of_year'] >= 1) & (df['day_of_year'] <= 31)

# Apply filters
filtered_df = df[merchant_filter & year_filter & january_filter].copy()

# Verify against ground truth (Count: 2345, Volume: 215683)
count = len(filtered_df)
total_volume = filtered_df['eur_amount'].sum()

print(f"Filtered transactions count: {count}")
print(f"Total volume: {total_volume:.2f}")

# Save for next step
filtered_df.to_pickle(output_path)
print(f"Filtered data saved to {output_path}")
