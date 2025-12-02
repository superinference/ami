# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2771
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2628 characters (FULL CODE)
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
df = pd.read_csv('/output/chunk3/data/context/payments.csv')

# Filter for merchant 'Rafa_AI' and year 2023
rafa_2023 = df[(df['merchant'] == 'Rafa_AI') & (df['year'] == 2023)]

# Filter for fraudulent transactions
fraudulent_txs = rafa_2023[rafa_2023['has_fraudulent_dispute'] == True]

# Identify current shopper_interaction and aci
# We also include card_scheme and is_credit as these are critical for determining applicable fees in the next steps
current_characteristics = fraudulent_txs[['shopper_interaction', 'aci', 'card_scheme', 'is_credit']].drop_duplicates()

print("Current characteristics of fraudulent transactions for Rafa_AI in 2023:")
print(current_characteristics)

# Display counts to understand the distribution
print("\nDetailed breakdown of fraudulent transactions:")
breakdown = fraudulent_txs.groupby(['shopper_interaction', 'aci', 'card_scheme', 'is_credit']).size().reset_index(name='count')
print(breakdown)
