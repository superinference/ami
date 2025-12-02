# ═══════════════════════════════════════════════════════════
# Round 2 - Task 402
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3480 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
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
file_path = '/output/chunk3/data/context/payments.csv'

try:
    # 1. Load the payments.csv file
    df_payments = pd.read_csv(file_path)
    print("Successfully loaded payments.csv")
    
    # 2. Apply Filters
    # Merchant: Golfclub_Baron_Friso
    # Card Scheme: SwiftCharge
    # Date Range: Nov-Dec 2023 (Day of year 305 to 365)
    
    # Filter conditions
    merchant_condition = df_payments['merchant'] == 'Golfclub_Baron_Friso'
    scheme_condition = df_payments['card_scheme'] == 'SwiftCharge'
    date_condition = df_payments['day_of_year'].between(305, 365) # Inclusive
    
    # Create subset
    subset = df_payments[merchant_condition & scheme_condition & date_condition].copy()
    
    print(f"Filtered records: {len(subset)}")
    
    if len(subset) == 0:
        print("No records found matching the criteria.")
    else:
        # 3. Group by issuing_country and calculate average eur_amount
        # The question asks for "average transaction value grouped by issuing_country"
        result = subset.groupby('issuing_country')['eur_amount'].mean()
        
        # 4. Print the result
        print("\nAverage transaction value by issuing_country (Golfclub_Baron_Friso, SwiftCharge, Nov-Dec 2023):")
        # Sorting for cleaner output, though not strictly required
        sorted_result = result.sort_index()
        
        # Print in a clear format
        for country, avg_val in sorted_result.items():
            print(f"{country}: {avg_val:.2f}")
            
        # Also print the raw dictionary/series representation if needed for parsing
        # print(sorted_result.to_dict())

except FileNotFoundError:
    print(f"Error: The file at {file_path} was not found.")
except Exception as e:
    print(f"An error occurred: {e}")
