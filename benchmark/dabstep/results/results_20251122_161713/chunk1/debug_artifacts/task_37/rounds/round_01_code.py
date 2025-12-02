# ═══════════════════════════════════════════════════════════
# Round 1 - Task 37
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 2426 characters (FULL CODE)
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
file_path = '/output/chunk1/data/context/payments.csv'
df = pd.read_csv(file_path)

# Calculate value counts for the 'is_credit' column
# True indicates Credit, False indicates Debit
counts = df['is_credit'].value_counts()

# Extract the specific counts for Credit (True) and Debit (False)
credit_count = counts.get(True, 0)
debit_count = counts.get(False, 0)

print(f"Credit Transactions (True): {credit_count}")
print(f"Debit Transactions (False): {debit_count}")

# Calculate the ratio of credit card transactions to debit card transactions
if debit_count > 0:
    ratio = credit_count / debit_count
    print(f"Ratio of Credit to Debit transactions: {ratio}")
else:
    print("Ratio cannot be calculated (division by zero).")
