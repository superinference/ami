# ═══════════════════════════════════════════════════════════
# Round 2 - Task 39
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 2824 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import numpy as np
import json

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

def calculate_percentage_below_25th_percentile():
    # Load the payments.csv file into a dataframe
    file_path = '/output/chunk1/data/context/payments.csv'
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print("Error: payments.csv not found.")
        return

    # Ensure eur_amount is numeric
    if 'eur_amount' not in df.columns:
        print("Error: 'eur_amount' column not found.")
        return
        
    # Calculate the 25th percentile value of the eur_amount column
    # quantile(0.25) returns the value at the 25th percentile
    percentile_25 = df['eur_amount'].quantile(0.25)
    
    # Count transactions strictly below this threshold
    count_below = (df['eur_amount'] < percentile_25).sum()
    total_count = len(df)
    
    # Calculate percentage
    if total_count > 0:
        percentage = (count_below / total_count) * 100
    else:
        percentage = 0.0
    
    # Print the result
    # The question asks "What is the percentage...", so we print the value.
    print(percentage)

if __name__ == "__main__":
    calculate_percentage_below_25th_percentile()
