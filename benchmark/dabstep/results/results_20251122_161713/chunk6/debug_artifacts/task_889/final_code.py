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
file_path = '/output/chunk6/data/context/payments.csv'

try:
    # Load data
    df = pd.read_csv(file_path)
    
    # Define filter criteria
    target_merchant = 'Crossfit_Hanna'
    target_scheme = 'GlobalCard'
    start_day = 152  # June 1st
    end_day = 243    # August 31st
    
    # Apply filters
    # 1. Merchant
    # 2. Card Scheme
    # 3. Date Range (Day of Year)
    mask = (
        (df['merchant'] == target_merchant) &
        (df['card_scheme'] == target_scheme) &
        (df['day_of_year'] >= start_day) &
        (df['day_of_year'] <= end_day)
    )
    
    filtered_df = df[mask]
    
    # Check if data exists
    if filtered_df.empty:
        print(f"No transactions found for {target_merchant} with {target_scheme} between days {start_day}-{end_day}.")
    else:
        # Group by 'aci' and calculate mean of 'eur_amount'
        # We use groupby on 'aci' and select 'eur_amount', then calculate mean
        aci_grouped = filtered_df.groupby('aci')['eur_amount'].mean()
        
        # Print the results in the requested format
        # The question asks "What is the average transaction value grouped by aci..."
        # We will print the breakdown.
        print(f"Average transaction value grouped by aci for {target_merchant} ({target_scheme}, Days {start_day}-{end_day}):")
        
        # Sort by ACI for clean output
        for aci, avg_amount in aci_grouped.sort_index().items():
            print(f"{aci}: {avg_amount:.2f}")

except FileNotFoundError:
    print(f"Error: The file was not found at {file_path}")
except Exception as e:
    print(f"An error occurred: {e}")