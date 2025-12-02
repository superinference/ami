# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1461
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2739 characters (FULL CODE)
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

def execute_step():
    # Load the fees.json file
    file_path = '/output/chunk3/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)
    
    # Convert to DataFrame
    df_fees = pd.DataFrame(fees_data)
    
    # Filter for rules where:
    # 1. card_scheme is 'SwiftCharge'
    # 2. is_credit is True (specific to credit) OR is_credit is null (wildcard, applies to all)
    # Note: In pandas, JSON nulls are typically represented as None or NaN. .isnull() catches both.
    
    filtered_fees = df_fees[
        (df_fees['card_scheme'] == 'SwiftCharge') & 
        ((df_fees['is_credit'] == True) | (df_fees['is_credit'].isnull()))
    ].copy()
    
    # Output results for verification
    print(f"Total fees loaded: {len(df_fees)}")
    print(f"Filtered fees (SwiftCharge + Credit/Wildcard): {len(filtered_fees)}")
    print("\nFirst 5 filtered rules:")
    print(filtered_fees[['ID', 'card_scheme', 'is_credit', 'aci', 'fixed_amount', 'rate']].head().to_string())

if __name__ == "__main__":
    execute_step()
