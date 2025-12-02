# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1743
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3656 characters (FULL CODE)
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
import json

# Define file paths
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'
acquirer_countries_path = '/output/chunk6/data/context/acquirer_countries.csv'

# Load payments data
try:
    df_payments = pd.read_csv(payments_path)
    print(f"Successfully loaded payments.csv with {len(df_payments)} rows.")
except Exception as e:
    print(f"Error loading payments.csv: {e}")

# Load context files (needed for subsequent analysis steps)
try:
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchant_metadata = json.load(f)
    df_acquirer_countries = pd.read_csv(acquirer_countries_path)
    print("Successfully loaded fees.json, merchant_data.json, and acquirer_countries.csv.")
except Exception as e:
    print(f"Error loading context files: {e}")

# Filter for rows where the merchant is 'Golfclub_Baron_Friso' and the year is 2023
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023

# Ensure year is treated as integer for comparison
filtered_df = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

# Output the result of the filtering
print(f"\nFiltering for Merchant: '{target_merchant}' and Year: {target_year}")
print(f"Number of matching transactions found: {len(filtered_df)}")

# Display a sample of the filtered dataframe
print("\nFirst 5 rows of filtered data:")
print(filtered_df.head())

# Display unique values for columns critical to fee identification (card_scheme, is_credit, aci)
# This helps verify the data against the ground truth provided in the prompt
print("\nUnique values in key columns for this merchant:")
print(f"Card Schemes: {filtered_df['card_scheme'].unique()}")
print(f"Is Credit: {filtered_df['is_credit'].unique()}")
print(f"ACI codes: {filtered_df['aci'].unique()}")
