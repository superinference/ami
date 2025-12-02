# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2520
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3149 characters (FULL CODE)
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
import os

# Define file paths
payments_path = '/output/chunk2/data/context/payments.csv'

# Check if file exists
if not os.path.exists(payments_path):
    print(f"Error: File not found at {payments_path}")
else:
    # Step 1: Load the payments dataset
    # Using low_memory=False to ensure mixed types are handled if any, though schema suggests consistent types
    df_payments = pd.read_csv(payments_path)
    print(f"Successfully loaded payments.csv with {len(df_payments)} rows.")

    # Step 2: Filter for transactions where merchant is 'Rafa_AI' and year is 2023
    # According to schema, 'year' is int64 and 'merchant' is object (string)
    filtered_df = df_payments[
        (df_payments['merchant'] == 'Rafa_AI') & 
        (df_payments['year'] == 2023)
    ].copy()

    # Step 3: Output verification
    print(f"Filtered data contains {len(filtered_df)} transactions for Rafa_AI in 2023.")

    # Display a sample to ensure data looks correct
    if not filtered_df.empty:
        print("\nSample of filtered data:")
        print(filtered_df.head())
        
        # Verify the filter worked
        print("\nVerification:")
        print(f"Unique Merchants: {filtered_df['merchant'].unique()}")
        print(f"Unique Years: {filtered_df['year'].unique()}")
        
        # Print column info to help with next steps
        print("\nColumns available:", filtered_df.columns.tolist())
    else:
        print("No transactions found for the specified criteria.")
