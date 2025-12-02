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

# Load the payments dataset
file_path = '/output/chunk4/data/context/payments.csv'
df = pd.read_csv(file_path)

# Step 1: Filter for specific merchant and card scheme
# Merchant: 'Golfclub_Baron_Friso'
# Card Scheme: 'SwiftCharge'
filtered_df = df[
    (df['merchant'] == 'Golfclub_Baron_Friso') & 
    (df['card_scheme'] == 'SwiftCharge')
]

# Step 2: Filter for date range (January and February 2023)
# 2023 is not a leap year. Jan = 31 days, Feb = 28 days. Total = 59 days.
# day_of_year <= 59 covers Jan 1 to Feb 28.
date_filtered_df = filtered_df[filtered_df['day_of_year'] <= 59]

# Step 3: Group by acquirer_country and calculate average transaction value
if not date_filtered_df.empty:
    result = date_filtered_df.groupby('acquirer_country')['eur_amount'].mean()
    
    # Print the result in a readable format
    # The question asks "What is the average transaction value grouped by acquirer_country..."
    # We will print the dictionary/series representation.
    print("Average transaction value by acquirer_country (Jan-Feb 2023):")
    print(result.to_dict())
    
    # Also print formatted for clarity
    for country, avg in result.items():
        print(f"{country}: {avg:.2f}")
else:
    print("No transactions found for the specified criteria.")