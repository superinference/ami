# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1437
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3618 characters (FULL CODE)
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

# Set display options to ensure we can see the full output
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# File path
fees_path = '/output/chunk4/data/context/fees.json'

try:
    # Load the JSON file
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    # Convert to DataFrame
    df_fees = pd.DataFrame(fees_data)
    
    print(f"Successfully loaded fees.json with {len(df_fees)} rules.")
    
    # Calculate the fee for a transaction of 100 euros
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    transaction_value = 100.0
    df_fees['calculated_fee_100eur'] = df_fees['fixed_amount'] + (df_fees['rate'] * transaction_value / 10000)
    
    # Display the structure of the dataframe
    print("\n--- DataFrame Info ---")
    print(df_fees.info())
    
    # Explode the 'merchant_category_code' column to analyze fees per MCC
    # Rules with empty lists or None in 'merchant_category_code' apply to all/general MCCs
    df_exploded = df_fees.explode('merchant_category_code')
    
    # Separate specific MCC rules from general rules
    df_specific_mcc = df_exploded[df_exploded['merchant_category_code'].notna()]
    df_general_rules = df_fees[df_fees['merchant_category_code'].apply(lambda x: x is None or (isinstance(x, list) and len(x) == 0))]
    
    print("\n--- Top 10 Most Expensive Rules (Specific MCCs) for 100 EUR ---")
    cols_to_show = ['ID', 'merchant_category_code', 'fixed_amount', 'rate', 'calculated_fee_100eur']
    print(df_specific_mcc.sort_values(by='calculated_fee_100eur', ascending=False)[cols_to_show].head(10))
    
    print("\n--- Top 5 Most Expensive General Rules (All MCCs) for 100 EUR ---")
    print(df_general_rules.sort_values(by='calculated_fee_100eur', ascending=False)[['ID', 'fixed_amount', 'rate', 'calculated_fee_100eur']].head(5))

except Exception as e:
    print(f"An error occurred: {e}")
