# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2641
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3250 characters (FULL CODE)
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
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_data_path = '/output/chunk4/data/context/merchant_data.json'

# 1. Load the payments CSV file
df_payments = pd.read_csv(payments_path)

# 2. Filter data for 'Golfclub_Baron_Friso' and the month of July (Day of Year 182-212)
target_merchant = 'Golfclub_Baron_Friso'
start_day = 182
end_day = 212

df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['day_of_year'] >= start_day) &
    (df_payments['day_of_year'] <= end_day)
].copy()

# 3. Load fees.json and merchant_data.json
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

# 4. Extract specific merchant attributes for the target merchant
# This is crucial for the next step of fee calculation (matching MCC, account_type, etc.)
merchant_attributes = next(
    (m for m in merchant_data_list if m['merchant'] == target_merchant), 
    None
)

# Display verification of the loaded and filtered data
print(f"Successfully filtered {len(df_filtered)} transactions for {target_merchant} in July.")
print(f"Date range in filtered data: {df_filtered['day_of_year'].min()} to {df_filtered['day_of_year'].max()}")
print(f"Merchant Attributes: {merchant_attributes}")
print(f"Total Fee Rules Loaded: {len(fees_data)}")
print("\nSample of filtered transactions:")
print(df_filtered[['psp_reference', 'card_scheme', 'eur_amount', 'day_of_year']].head())
