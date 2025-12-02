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

# Load the payments.csv file
df = pd.read_csv('/output/chunk1/data/context/payments.csv')

# Calculate the Z-Score for the 'eur_amount' column
# Z = (X - Mean) / Standard Deviation
mean_amount = df['eur_amount'].mean()
std_amount = df['eur_amount'].std()

df['z_score'] = (df['eur_amount'] - mean_amount) / std_amount

# Identify transactions with a Z-Score greater than 3
outliers = df[df['z_score'] > 3]

# Group by merchant and count the number of outlier transactions
merchant_outlier_counts = outliers['merchant'].value_counts()

# Identify the merchant with the highest number of outlier transactions
# .idxmax() returns the index (merchant name) associated with the maximum count
if not merchant_outlier_counts.empty:
    top_merchant = merchant_outlier_counts.idxmax()
    print(top_merchant)
else:
    print("No outliers found with Z-Score > 3")