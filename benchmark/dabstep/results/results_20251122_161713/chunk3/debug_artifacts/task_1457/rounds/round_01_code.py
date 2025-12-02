# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1457
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3166 characters (FULL CODE)
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

# Define the file path
fees_file_path = '/output/chunk3/data/context/fees.json'

# Define transaction parameters based on the Goal
# Question: For a credit transaction of 1000 euros on SwiftCharge...
TRANSACTION_AMOUNT = 1000
CARD_SCHEME = 'SwiftCharge'
IS_CREDIT = True
# Verified ACIs from context to be evaluated later
ACIS_TO_CHECK = ['A', 'B', 'C', 'D', 'E', 'F', 'G']

def load_fees():
    """
    Loads the fees.json file into a DataFrame to access fee rules and rates.
    """
    try:
        with open(fees_file_path, 'r') as f:
            fees_data = json.load(f)
        
        df_fees = pd.DataFrame(fees_data)
        print(f"Successfully loaded fees.json with {len(df_fees)} rules.")
        return df_fees
    except Exception as e:
        print(f"Error loading fees file: {e}")
        return None

# Execute the plan step
df_fees = load_fees()

# Verify the data structure and content relevant to the goal
if df_fees is not None:
    print("\nDataFrame Head:")
    print(df_fees.head())
    
    print("\nDataFrame Info:")
    print(df_fees.info())
    
    # Quick check for the specific card scheme mentioned in the goal
    swift_charge_rules = df_fees[df_fees['card_scheme'] == CARD_SCHEME]
    print(f"\nNumber of rules for {CARD_SCHEME}: {len(swift_charge_rules)}")
    
    # Check unique values in 'aci' column (which are lists) to understand structure
    print("\nSample 'aci' column values (lists of applicable ACIs):")
    print(df_fees['aci'].head())
