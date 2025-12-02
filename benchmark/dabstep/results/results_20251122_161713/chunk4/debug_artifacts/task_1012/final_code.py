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
file_path = '/output/chunk4/data/context/payments.csv'

# Load the payments.csv file
df = pd.read_csv(file_path)

# Define filter criteria
target_merchant = 'Martinis_Fine_Steakhouse'
target_scheme = 'SwiftCharge'
target_year = 2023
# November 1st is the 305th day of a non-leap year (2023)
start_day = 305 
end_day = 365

# Apply filters
# 1. Merchant
# 2. Card Scheme
# 3. Year
# 4. Date Range (Nov-Dec)
filtered_df = df[
    (df['merchant'] == target_merchant) &
    (df['card_scheme'] == target_scheme) &
    (df['year'] == target_year) &
    (df['day_of_year'] >= start_day) &
    (df['day_of_year'] <= end_day)
]

# Calculate average transaction value grouped by aci
# Group by 'aci' and calculate mean of 'eur_amount'
avg_tx_value_by_aci = filtered_df.groupby('aci')['eur_amount'].mean()

# Print the result
print("Average transaction value grouped by aci for Martinis_Fine_Steakhouse (SwiftCharge, Nov-Dec 2023):")
print(avg_tx_value_by_aci)