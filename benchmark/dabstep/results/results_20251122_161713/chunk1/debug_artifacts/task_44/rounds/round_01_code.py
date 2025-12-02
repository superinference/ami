# ═══════════════════════════════════════════════════════════
# Round 1 - Task 44
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 2581 characters (FULL CODE)
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

# Calculate the number of transactions per shopper (email_address)
# value_counts() excludes NaN values by default, which is appropriate for counting identifiable shoppers
shopper_transaction_counts = df['email_address'].value_counts()

# Identify total unique shoppers
total_shoppers = len(shopper_transaction_counts)

# Identify repeat shoppers (those with more than 1 transaction)
repeat_shoppers = len(shopper_transaction_counts[shopper_transaction_counts > 1])

# Calculate the percentage
if total_shoppers > 0:
    repeat_percentage = (repeat_shoppers / total_shoppers) * 100
else:
    repeat_percentage = 0.0

# Print the results
print(f"Total unique shoppers: {total_shoppers}")
print(f"Repeat shoppers: {repeat_shoppers}")
print(f"Percentage of repeat customers: {repeat_percentage:.14f}%")
