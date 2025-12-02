# ═══════════════════════════════════════════════════════════
# Round 1 - Task 46
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 2344 characters (FULL CODE)
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

# Load the payments.csv file into a pandas DataFrame
df = pd.read_csv('/output/chunk1/data/context/payments.csv')

# Identify transactions with missing IP addresses (NaN values)
missing_ip_mask = df['ip_address'].isna()

# Identify transactions that have a fraudulent dispute
fraud_mask = df['has_fraudulent_dispute'] == True

# Count transactions that meet both criteria (Missing IP AND Fraudulent)
count_missing_ip_and_fraud = len(df[missing_ip_mask & fraud_mask])

# Calculate the percentage relative to the total number of transactions
total_transactions = len(df)
percentage = (count_missing_ip_and_fraud / total_transactions) * 100

# Print the result
print(percentage)
