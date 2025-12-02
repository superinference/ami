# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1966
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2758 characters (FULL CODE)
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
df_payments = pd.read_csv(payments_path)

# Define filter criteria
target_merchant = 'Belles_cookbook_store'
start_day = 274  # October 1st (approx)
end_day = 304    # October 31st (approx)

# Apply filters
# We use .copy() to avoid SettingWithCopyWarning in subsequent steps
filtered_transactions = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['day_of_year'] >= start_day) & 
    (df_payments['day_of_year'] <= end_day)
].copy()

# Output verification
print(f"Total rows in payments.csv: {len(df_payments)}")
print(f"Transactions for '{target_merchant}' between day {start_day} and {end_day}: {len(filtered_transactions)}")
print("\nFirst 5 filtered transactions:")
print(filtered_transactions[['psp_reference', 'merchant', 'day_of_year', 'eur_amount', 'card_scheme']].head())

# Save or pass this dataframe to the next step in the analysis pipeline
# (In a real script, this variable 'filtered_transactions' would be used for fee calculation)
