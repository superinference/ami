# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1297
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2921 characters (FULL CODE)
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

# Set display options to ensure all columns are visible
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Load the fees.json file
fees_path = '/output/chunk3/data/context/fees.json'
try:
    df_fees = pd.read_json(fees_path)
    print("Successfully loaded fees.json")
    print(f"Shape: {df_fees.shape}")
    
    # Inspect the columns and data types
    print("\nColumns and Data Types:")
    print(df_fees.dtypes)
    
    # Display the first few rows to understand the structure
    print("\nFirst 5 rows of Fee Rules:")
    print(df_fees.head().to_string())
    
    # Inspect unique values for key columns relevant to the question
    print("\nUnique Card Schemes:")
    print(df_fees['card_scheme'].unique())
    
    print("\nUnique values for 'is_credit' (relevant for credit transactions):")
    print(df_fees['is_credit'].unique())
    
    # Filter for GlobalCard rules to inspect specific conditions
    global_card_rules = df_fees[df_fees['card_scheme'] == 'GlobalCard']
    print(f"\nNumber of GlobalCard rules: {len(global_card_rules)}")
    print("\nSample GlobalCard Rules:")
    print(global_card_rules.head().to_string())

except Exception as e:
    print(f"Error loading fees.json: {e}")
