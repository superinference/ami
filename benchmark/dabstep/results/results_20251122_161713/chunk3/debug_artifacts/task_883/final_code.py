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

# Load the payments.csv file
file_path = '/output/chunk3/data/context/payments.csv'
df = pd.read_csv(file_path)

# Define filter criteria
target_merchant = 'Crossfit_Hanna'
target_scheme = 'GlobalCard'
# January (31 days) + February (28 days in 2023) = 59 days
max_day_of_year = 59 

# Apply filters
# 1. Merchant and Card Scheme
# 2. Time period: Jan-Feb 2023 (Day of year 1 to 59)
filtered_df = df[
    (df['merchant'] == target_merchant) & 
    (df['card_scheme'] == target_scheme) &
    (df['day_of_year'] >= 1) & 
    (df['day_of_year'] <= max_day_of_year)
]

# Check if data exists after filtering
if filtered_df.empty:
    print("No transactions found for the specified criteria.")
else:
    # Group by 'aci' and calculate the average 'eur_amount'
    # Using .mean() automatically handles the float calculation
    average_by_aci = filtered_df.groupby('aci')['eur_amount'].mean()

    # Print the result
    print("Average transaction value grouped by aci for Crossfit_Hanna (GlobalCard, Jan-Feb 2023):")
    # Sort for cleaner output, though not strictly required
    print(average_by_aci.sort_index().to_string())