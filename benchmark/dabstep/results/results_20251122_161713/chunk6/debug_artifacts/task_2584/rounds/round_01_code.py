# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2584
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2890 characters (FULL CODE)
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

# Set file path
payments_path = '/output/chunk6/data/context/payments.csv'

# Load the payments dataset
df = pd.read_csv(payments_path)

# Filter for transactions belonging to 'Martinis_Fine_Steakhouse' in January (day_of_year 1 to 31)
merchant_name = 'Martinis_Fine_Steakhouse'
january_mask = (df['merchant'] == merchant_name) & (df['day_of_year'] >= 1) & (df['day_of_year'] <= 31)
january_txs = df[january_mask].copy()

# Calculate the requested metrics
avg_amount = january_txs['eur_amount'].mean()
credit_proportion = january_txs['is_credit'].mean() # True is 1, False is 0
debit_proportion = 1.0 - credit_proportion
transaction_count = len(january_txs)

# Calculate domestic proportion (useful context for fee calculation later)
# Domestic if issuing_country matches acquirer_country
domestic_proportion = (january_txs['issuing_country'] == january_txs['acquirer_country']).mean()

# Print the transaction profile
print(f"Transaction Profile for {merchant_name} (January):")
print(f"Total Transactions: {transaction_count}")
print(f"Average EUR Amount: {avg_amount}")
print(f"Credit Proportion: {credit_proportion}")
print(f"Debit Proportion: {debit_proportion}")
print(f"Domestic Proportion: {domestic_proportion}")
