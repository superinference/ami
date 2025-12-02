# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1296
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3605 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

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

# Main Analysis Script
def calculate_average_fee():
    # Load fees data
    fees_path = '/output/chunk6/data/context/fees.json'
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    df_fees = pd.DataFrame(fees_data)
    
    # Filter for 'TransactPlus' card scheme
    # We are looking for rules that apply to 'TransactPlus'
    df_tp = df_fees[df_fees['card_scheme'] == 'TransactPlus'].copy()
    
    # Filter for Credit transactions
    # A rule applies to a credit transaction if:
    # 1. 'is_credit' is explicitly True
    # 2. 'is_credit' is None/NaN (Wildcard, applies to both credit and debit)
    # Note: In pandas, None in JSON usually becomes None or NaN depending on column type.
    # We check for True or Null.
    
    # Ensure is_credit is handled correctly (it might be boolean or object with None)
    # We keep rows where is_credit is True OR is_credit is null
    df_credit_rules = df_tp[
        (df_tp['is_credit'] == True) | 
        (df_tp['is_credit'].isna()) | 
        (df_tp['is_credit'].isnull())
    ].copy()
    
    # Transaction details
    transaction_value = 5000.0
    
    # Calculate fee for each applicable rule
    # Formula: fee = fixed_amount + (rate * transaction_value / 10000)
    
    calculated_fees = []
    
    for _, row in df_credit_rules.iterrows():
        fixed_amount = coerce_to_float(row['fixed_amount'])
        rate = coerce_to_float(row['rate'])
        
        fee = fixed_amount + (rate * transaction_value / 10000)
        calculated_fees.append(fee)
        
    # Calculate average
    if not calculated_fees:
        print("No matching fee rules found.")
        return
        
    average_fee = np.mean(calculated_fees)
    
    # Output the result
    # The question asks "what would be the average fee", implying a single number.
    print(average_fee)

if __name__ == "__main__":
    calculate_average_fee()
