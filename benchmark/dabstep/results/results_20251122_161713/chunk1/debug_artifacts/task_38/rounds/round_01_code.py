# ═══════════════════════════════════════════════════════════
# Round 1 - Task 38
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2459 characters (FULL CODE)
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

# Load the payments.csv file
file_path = '/output/chunk1/data/context/payments.csv'
df = pd.read_csv(file_path)

# Group by 'merchant' and calculate the sum of 'eur_amount'
# We also calculate 'count' here as it will be needed for the final percentage calculation
merchant_volume = df.groupby('merchant')['eur_amount'].agg(['sum', 'count'])

# Sort the results in descending order by the sum of eur_amount (volume)
merchant_volume_sorted = merchant_volume.sort_values(by='sum', ascending=False)

# Identify the top 2 merchants by volume
top_2_merchants = merchant_volume_sorted.head(2)

print("Top 2 Merchants by Volume (eur_amount):")
print(top_2_merchants)

# Display the list of top 2 merchant names for verification
print("\nTop 2 Merchant Names:")
print(top_2_merchants.index.tolist())
