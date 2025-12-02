# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2664
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2990 characters (FULL CODE)
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

# Define file path
payments_path = '/output/chunk6/data/context/payments.csv'

# Load the payments data
df = pd.read_csv(payments_path)

# Define filter criteria
merchant_name = 'Martinis_Fine_Steakhouse'
start_day = 244
end_day = 273

# Filter for the specific merchant and time period (September)
# Note: day_of_year is inclusive for the month definition provided in the prompt
september_txs = df[
    (df['merchant'] == merchant_name) & 
    (df['day_of_year'] >= start_day) & 
    (df['day_of_year'] <= end_day)
]

# Calculate metrics
# 1. Total Monthly Volume
total_volume = september_txs['eur_amount'].sum()

# 2. Fraud Rate (percentage of transactions that are fraudulent)
# has_fraudulent_dispute is boolean, so mean() gives the ratio, * 100 for percentage
fraud_rate = september_txs['has_fraudulent_dispute'].mean() * 100

# 3. Average Transaction Value
avg_transaction_value = september_txs['eur_amount'].mean()

# 4. Transaction Count (useful for context)
transaction_count = len(september_txs)

# Print the results
print(f"Analysis for {merchant_name} in September (Days {start_day}-{end_day}):")
print(f"Transaction Count: {transaction_count}")
print(f"Total Monthly Volume: {total_volume:.2f} EUR")
print(f"Fraud Rate: {fraud_rate:.4f}%")
print(f"Average Transaction Value: {avg_transaction_value:.2f} EUR")
