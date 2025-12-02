# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2407
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2702 characters (FULL CODE)
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
payments_path = '/output/chunk3/data/context/payments.csv'
df_payments = pd.read_csv(payments_path)

# Filter for merchant 'Rafa_AI'
df_rafa = df_payments[df_payments['merchant'] == 'Rafa_AI'].copy()

# Convert year and day_of_year to a datetime object to easily filter by month
# We combine year and day_of_year into a string or integer for parsing
# Format '%Y%j' parses Year and Day of Year (1-366)
df_rafa['date'] = pd.to_datetime(
    df_rafa['year'].astype(str) + df_rafa['day_of_year'].astype(str).str.zfill(3), 
    format='%Y%j'
)

# Filter for June 2023 (Month 6)
df_rafa_june = df_rafa[
    (df_rafa['date'].dt.month == 6) & 
    (df_rafa['date'].dt.year == 2023)
]

# Output verification
print(f"Loaded payments.csv with {len(df_payments)} rows.")
print(f"Filtered for 'Rafa_AI': {len(df_rafa)} rows.")
print(f"Filtered for June 2023: {len(df_rafa_june)} rows.")
print("\nSample of filtered data:")
print(df_rafa_june[['merchant', 'date', 'eur_amount', 'card_scheme', 'aci', 'is_credit']].head())
