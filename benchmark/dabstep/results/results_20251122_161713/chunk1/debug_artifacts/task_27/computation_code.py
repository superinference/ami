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

# Load the payments data
file_path = '/output/chunk1/data/context/payments.csv'
df = pd.read_csv(file_path)

# Step 1: Calculate mean and standard deviation of 'eur_amount'
eur_mean = df['eur_amount'].mean()
eur_std = df['eur_amount'].std()

# Step 2: Calculate the threshold for outliers (Z-Score > 3)
# Formula: Value > Mean + (3 * StdDev)
threshold = eur_mean + (3 * eur_std)

# Step 3: Filter the dataframe to get only outlier transactions
outliers = df[df['eur_amount'] > threshold]

# Step 4: Group by 'hour_of_day' and count occurrences
# We use value_counts() which sorts by frequency descending by default
hourly_counts = outliers['hour_of_day'].value_counts()

# Step 5: Identify the hour with the highest count
if not hourly_counts.empty:
    most_frequent_hour = hourly_counts.idxmax()
    print(most_frequent_hour)
else:
    print("No outliers found")