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

# Main execution
try:
    # Load the CSV file
    file_path = '/output/chunk5/data/context/payments.csv'
    df_payments = pd.read_csv(file_path)
    
    # Filter criteria:
    # 1. Merchant: 'Belles_cookbook_store'
    # 2. Card Scheme: 'GlobalCard'
    # 3. Date Range: November and December 2023
    #    - 2023 is not a leap year.
    #    - Days in months: Jan(31)+Feb(28)+Mar(31)+Apr(30)+May(31)+Jun(30)+Jul(31)+Aug(31)+Sep(30)+Oct(31) = 304 days.
    #    - November 1st is day 305.
    #    - Filter: day_of_year >= 305.
    
    filtered_df = df_payments[
        (df_payments['merchant'] == 'Belles_cookbook_store') &
        (df_payments['card_scheme'] == 'GlobalCard') &
        (df_payments['day_of_year'] >= 305)
    ]
    
    if filtered_df.empty:
        print("No transactions found matching the criteria.")
    else:
        # Group by 'acquirer_country' and calculate the mean of 'eur_amount'
        average_tx_value = filtered_df.groupby('acquirer_country')['eur_amount'].mean()
        
        # Print the result
        print(average_tx_value)

except FileNotFoundError:
    print(f"Error: File not found at {file_path}")
except Exception as e:
    print(f"An error occurred: {e}")