# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1801
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2586 characters (FULL CODE)
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

# Set display options to ensure all columns are visible if printed
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Load the payments dataset
file_path = '/output/chunk4/data/context/payments.csv'
df = pd.read_csv(file_path)

# Filter for merchant 'Rafa_AI'
# Filter for March 2023: day_of_year between 60 and 90 (inclusive)
# Note: 2023 is not a leap year. Jan (31) + Feb (28) = 59 days. March is days 60-90.
rafa_march_df = df[
    (df['merchant'] == 'Rafa_AI') & 
    (df['year'] == 2023) & 
    (df['day_of_year'] >= 60) & 
    (df['day_of_year'] <= 90)
].copy()

# Print verification metrics to confirm the filter matches Ground Truth
print(f"Filtered DataFrame Shape: {rafa_march_df.shape}")
print(f"Total Volume: {rafa_march_df['eur_amount'].sum()}")

# Display the first few rows of the filtered dataframe
print("\nFirst 5 rows of filtered data:")
print(rafa_march_df.head())
