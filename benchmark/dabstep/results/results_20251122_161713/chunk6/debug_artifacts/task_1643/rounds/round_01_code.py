# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1643
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3070 characters (FULL CODE)
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


import json
import pandas as pd

def load_fees_data():
    """
    Loads the fees.json file to access fee rules and parameters.
    """
    file_path = '/output/chunk6/data/context/fees.json'
    
    try:
        with open(file_path, 'r') as f:
            fees_data = json.load(f)
            
        print(f"Successfully loaded {len(fees_data)} fee rules.")
        
        # Convert to DataFrame for better visualization of the structure
        df_fees = pd.DataFrame(fees_data)
        
        # Display the columns to confirm we have fixed_amount, rate, account_type, card_scheme
        print("\nColumns available in fees.json:")
        print(df_fees.columns.tolist())
        
        # Display a sample to understand the data types (e.g., account_type is a list)
        print("\nSample fee rules (first 3):")
        print(df_fees.head(3).to_string())
        
        # Preliminary check for the specific card scheme 'SwiftCharge'
        swift_charge_rules = df_fees[df_fees['card_scheme'] == 'SwiftCharge']
        print(f"\nTotal rules for 'SwiftCharge': {len(swift_charge_rules)}")
        
        return fees_data
        
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return None
    except Exception as e:
        print(f"An error occurred while loading the file: {e}")
        return None

if __name__ == "__main__":
    fees = load_fees_data()
