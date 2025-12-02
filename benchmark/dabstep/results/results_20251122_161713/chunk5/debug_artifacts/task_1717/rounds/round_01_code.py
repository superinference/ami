# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1717
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2347 characters (FULL CODE)
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
payments_path = '/output/chunk5/data/context/payments.csv'
df_payments = pd.read_csv(payments_path)

# Filter the dataframe for merchant "Crossfit_Hanna" and day_of_year 10
filtered_df = df_payments[
    (df_payments['merchant'] == 'Crossfit_Hanna') & 
    (df_payments['day_of_year'] == 10)
]

# Display the result to verify
print(f"Original shape: {df_payments.shape}")
print(f"Filtered shape: {filtered_df.shape}")
print("\nFirst 5 rows of filtered data:")
print(filtered_df.head().to_string())

# Save the filtered dataframe to a variable or file if needed for the next step, 
# but for this interaction, printing the verification is sufficient.
