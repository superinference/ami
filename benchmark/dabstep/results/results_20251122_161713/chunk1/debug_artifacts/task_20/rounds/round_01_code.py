# ═══════════════════════════════════════════════════════════
# Round 1 - Task 20
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 2266 characters (FULL CODE)
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
df = pd.read_csv('/output/chunk1/data/context/payments.csv')

# Count the number of transactions for each unique merchant
# value_counts() returns the counts in descending order by default
merchant_counts = df['merchant'].value_counts()

# Print the counts for all merchants
print("Transaction counts per merchant:")
print(merchant_counts)

# Explicitly identify the top merchant to address the overall goal
top_merchant = merchant_counts.idxmax()
top_count = merchant_counts.max()
print(f"\nMerchant with the highest number of transactions: {top_merchant} ({top_count})")
