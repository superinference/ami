# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2574
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2526 characters (FULL CODE)
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

# Define file path
fees_path = '/output/chunk4/data/context/fees.json'

# Load the fees.json file
try:
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    # Create DataFrame
    df_fees = pd.DataFrame(fees_data)
    
    # Filter for the row where ID is 787
    fee_787 = df_fees[df_fees['ID'] == 787]
    
    # Check if the fee exists and print its details to understand criteria
    if not fee_787.empty:
        # Convert to dictionary to see full structure (lists, nulls, etc.)
        fee_details = fee_787.to_dict(orient='records')[0]
        print("Current Application Criteria for Fee ID 787:")
        print(json.dumps(fee_details, indent=2))
    else:
        print("Fee with ID 787 not found in the dataset.")

except Exception as e:
    print(f"An error occurred while loading or processing the data: {e}")
