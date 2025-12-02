# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2480
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3053 characters (FULL CODE)
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

# Set the file path
file_path = '/output/chunk2/data/context/payments.csv'

# Step 1: Load the payments.csv file into a dataframe
try:
    df = pd.read_csv(file_path)
    print(f"Successfully loaded data from {file_path}")
    print(f"Total rows in original dataset: {len(df)}")
except FileNotFoundError:
    print(f"Error: The file {file_path} was not found.")
    exit()

# Step 2: Filter the data to include only transactions where the merchant is 'Belles_cookbook_store' and the year is 2023
# According to schema, 'year' is int64 and 'merchant' is object (string)
filtered_df = df[
    (df['merchant'] == 'Belles_cookbook_store') & 
    (df['year'] == 2023)
]

# Step 3: Verify the filtered data
print(f"\nFiltered rows for 'Belles_cookbook_store' in 2023: {len(filtered_df)}")

# Display a sample of the filtered data, focusing on columns relevant for fee calculation
# (eur_amount, card_scheme, is_credit, aci are needed for fee matching/calculation)
columns_of_interest = ['psp_reference', 'merchant', 'year', 'eur_amount', 'card_scheme', 'is_credit', 'aci']
print("\nFirst 5 rows of filtered data (relevant columns):")
print(filtered_df[columns_of_interest].head())

# Optional: Check total volume to align with ground truth expectations
total_volume = filtered_df['eur_amount'].sum()
print(f"\nTotal transaction volume for Belles_cookbook_store in 2023: {total_volume:.2f}")
