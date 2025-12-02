# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2703
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2596 characters (FULL CODE)
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

# Define file paths
payments_path = '/output/chunk5/data/context/payments.csv'

# Load the payments dataset
df = pd.read_csv(payments_path)

# Define filter criteria
merchant_name = 'Crossfit_Hanna'
start_day = 32  # Start of February
end_day = 59    # End of February

# Apply filters: Merchant is Crossfit_Hanna, Fraud is True, Timeframe is Feb
filtered_txs = df[
    (df['merchant'] == merchant_name) &
    (df['has_fraudulent_dispute'] == True) &
    (df['day_of_year'] >= start_day) &
    (df['day_of_year'] <= end_day)
]

# Output verification stats to confirm alignment with Ground Truth
print(f"Total_Transactions: {len(filtered_txs)}")
print(f"Avg_Amount: {filtered_txs['eur_amount'].mean()}")

print("\n---Schemes---")
print(filtered_txs['card_scheme'].value_counts())

print("\n---Is_Credit---")
print(filtered_txs['is_credit'].value_counts())

print("\n---Current_ACIs---")
print(filtered_txs['aci'].value_counts())
