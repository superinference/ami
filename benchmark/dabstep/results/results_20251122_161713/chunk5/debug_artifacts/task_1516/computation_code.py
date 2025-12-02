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
def analyze_fees():
    # File path
    fees_path = '/output/chunk5/data/context/fees.json'
    
    try:
        # Load fees data
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        
        df_fees = pd.DataFrame(fees_data)
        print(f"Loaded {len(df_fees)} fee rules.")
        
        # Define transaction value
        transaction_value = 5000.0
        
        # Ensure numeric types for calculation
        # Using coerce_to_float to be safe, though JSON usually loads numbers correctly
        df_fees['fixed_amount_clean'] = df_fees['fixed_amount'].apply(coerce_to_float)
        df_fees['rate_clean'] = df_fees['rate'].apply(coerce_to_float)
        
        # Calculate fee for each rule based on the formula in manual.md:
        # fee = fixed_amount + rate * transaction_value / 10000
        df_fees['calculated_fee'] = df_fees['fixed_amount_clean'] + (df_fees['rate_clean'] * transaction_value / 10000)
        
        # Group by card_scheme to find the average fee per scheme
        # "Average scenario" implies averaging the fees across all rules for a scheme
        avg_fees_by_scheme = df_fees.groupby('card_scheme')['calculated_fee'].mean()
        
        print("\nAverage Fee by Card Scheme (for 5000 EUR transaction):")
        print(avg_fees_by_scheme)
        
        # Identify the most expensive scheme
        most_expensive_scheme = avg_fees_by_scheme.idxmax()
        max_fee = avg_fees_by_scheme.max()
        
        print(f"\nMost expensive scheme: {most_expensive_scheme} (Avg Fee: {max_fee:.4f})")
        
        # Final Answer
        print(most_expensive_scheme)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    analyze_fees()