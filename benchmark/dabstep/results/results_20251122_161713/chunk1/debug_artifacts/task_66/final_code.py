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
try:
    # Load fees data
    fees_path = '/output/chunk1/data/context/fees.json'
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    df_fees = pd.DataFrame(fees_data)
    
    # Ensure rate is float for calculation
    df_fees['rate'] = df_fees['rate'].apply(coerce_to_float)
    
    results = []
    
    # --- Factor 1: is_credit ---
    # Filter for rows where is_credit is explicitly True or False (not None)
    df_credit = df_fees[df_fees['is_credit'].notna()].copy()
    
    if not df_credit.empty:
        # Group by is_credit and calculate mean rate
        credit_stats = df_credit.groupby('is_credit')['rate'].mean()
        
        # Check if both True and False categories exist to compare
        if False in credit_stats.index and True in credit_stats.index:
            rate_false = credit_stats[False]
            rate_true = credit_stats[True]
            
            # Debug print
            print(f"DEBUG: is_credit | False (Debit): {rate_false:.2f} | True (Credit): {rate_true:.2f}")
            
            # If False is cheaper (lower rate) than True
            if rate_false < rate_true:
                results.append('is_credit')

    # --- Factor 2: intracountry ---
    # Filter for rows where intracountry is explicitly set (not None)
    df_intra = df_fees[df_fees['intracountry'].notna()].copy()
    
    if not df_intra.empty:
        # Convert 0.0/1.0 to boolean for consistent grouping
        # 0.0 -> False (International), 1.0 -> True (Domestic)
        df_intra['intra_bool'] = df_intra['intracountry'].apply(lambda x: bool(x))
        
        intra_stats = df_intra.groupby('intra_bool')['rate'].mean()
        
        if False in intra_stats.index and True in intra_stats.index:
            rate_false = intra_stats[False]
            rate_true = intra_stats[True]
            
            # Debug print
            print(f"DEBUG: intracountry | False (International): {rate_false:.2f} | True (Domestic): {rate_true:.2f}")
            
            # If False is cheaper (lower rate) than True
            if rate_false < rate_true:
                results.append('intracountry')

    # --- Final Output ---
    if results:
        print(", ".join(results))
    else:
        print("None")

except Exception as e:
    print(f"Error during analysis: {e}")