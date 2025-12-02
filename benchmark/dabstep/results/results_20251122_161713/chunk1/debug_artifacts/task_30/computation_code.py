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
file_path = '/output/chunk1/data/context/payments.csv'
df = pd.read_csv(file_path)

# Filter for the year 2023
df_2023 = df[df['year'] == 2023]

# Calculate fraud rate for Ecommerce transactions
ecommerce_txs = df_2023[df_2023['shopper_interaction'] == 'Ecommerce']
ecommerce_fraud_rate = ecommerce_txs['has_fraudulent_dispute'].mean()

# Calculate fraud rate for In-Store (POS) transactions
# Note: Documentation confirms POS means in-person/in-store
pos_txs = df_2023[df_2023['shopper_interaction'] == 'POS']
pos_fraud_rate = pos_txs['has_fraudulent_dispute'].mean()

# Print rates for verification
print(f"Ecommerce Fraud Rate: {ecommerce_fraud_rate:.6f}")
print(f"In-Store (POS) Fraud Rate: {pos_fraud_rate:.6f}")

# Determine if Ecommerce fraud rate is higher
if ecommerce_fraud_rate > pos_fraud_rate:
    print("yes")
else:
    print("no")