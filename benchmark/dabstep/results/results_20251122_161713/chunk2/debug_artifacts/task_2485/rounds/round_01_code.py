# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2485
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3203 characters (FULL CODE)
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
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'
acquirer_countries_path = '/output/chunk2/data/context/acquirer_countries.csv'

# Load payments data
df_payments = pd.read_csv(payments_path)

# Filter for Crossfit_Hanna and year 2023
# Note: The 'year' column is numeric based on schema
df_filtered = df_payments[
    (df_payments['merchant'] == 'Crossfit_Hanna') & 
    (df_payments['year'] == 2023)
].copy()

# Load fees data
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Load merchant data
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# Load acquirer countries data
df_acquirer_countries = pd.read_csv(acquirer_countries_path)

# Display basic info to verify the load and filter
print(f"Filtered payments rows: {len(df_filtered)}")
print(f"Total fee rules loaded: {len(fees_data)}")
print(f"Merchant data entries loaded: {len(merchant_data)}")
print(f"Acquirer countries loaded: {len(df_acquirer_countries)}")

# Check if the specific fee rule ID=276 exists in the loaded fees
fee_276 = next((fee for fee in fees_data if fee['ID'] == 276), None)
if fee_276:
    print(f"Fee rule ID=276 found: {fee_276}")
else:
    print("Fee rule ID=276 not found.")

# Show a sample of the filtered payments to ensure correct columns are present
print("\nSample filtered payment:")
print(df_filtered.head(1).to_dict(orient='records'))
