# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1689
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2854 characters (FULL CODE)
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
payments_path = '/output/chunk4/data/context/payments.csv'

# Load the data
try:
    df = pd.read_csv(payments_path)
    print(f"Successfully loaded payments.csv with {len(df)} rows.")
except FileNotFoundError:
    print(f"Error: File not found at {payments_path}")
    exit()

# Filter the data
# Criteria: merchant = "Crossfit_Hanna", year = 2023, day_of_year = 100
filtered_df = df[
    (df['merchant'] == 'Crossfit_Hanna') &
    (df['year'] == 2023) &
    (df['day_of_year'] == 100)
]

# Display the result
print(f"Filtered for merchant='Crossfit_Hanna', year=2023, day_of_year=100.")
print(f"Found {len(filtered_df)} transactions.")

# Display sample data to verify
print("\nSample of filtered transactions:")
print(filtered_df.head())

# Display unique values for columns relevant to fee determination
# This helps verify the data against the 'Ground Truth' provided in the context
relevant_cols = ['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']
print("\nUnique attributes in filtered data (for fee matching):")
for col in relevant_cols:
    if col in filtered_df.columns:
        print(f"{col}: {sorted(filtered_df[col].unique())}")
