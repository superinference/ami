# ═══════════════════════════════════════════════════════════
# Round 1 - Task 67
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2796 characters (FULL CODE)
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

# Path to the fees.json file
fees_path = '/output/chunk1/data/context/fees.json'

# Load the fees data
try:
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    df_fees = pd.DataFrame(fees_data)
    
    # Columns of interest representing variable factors and the target variable
    factor_cols = ['monthly_volume', 'monthly_fraud_level', 'capture_delay']
    target_col = 'rate'
    
    print("--- DataFrame Info ---")
    print(df_fees.info())
    
    print("\n--- First 5 Rows ---")
    print(df_fees[factor_cols + [target_col]].head())
    
    print("\n--- Unique Values for Variable Factors ---")
    for col in factor_cols:
        unique_vals = df_fees[col].unique()
        print(f"\nUnique values in '{col}':")
        # Filter out None for cleaner printing, but keep them in mind
        print([x for x in unique_vals if x is not None])
        print(f"Count of Nulls: {df_fees[col].isnull().sum()}")

    print("\n--- Target Variable (rate) Stats ---")
    print(df_fees[target_col].describe())

except Exception as e:
    print(f"Error loading or processing fees.json: {e}")
