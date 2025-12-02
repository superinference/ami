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

# Load the payments dataset
df = pd.read_csv('/output/chunk1/data/context/payments.csv')

# Calculate the number of missing (null) values in the specified columns
missing_ip = df['ip_address'].isnull().sum()
missing_email = df['email_address'].isnull().sum()

# Print the counts for verification
print(f"Missing IP Address count: {missing_ip}")
print(f"Missing Email Address count: {missing_email}")

# Determine the answer based on the counts
if missing_ip > 0 and missing_email > 0:
    answer = "C. both ip_address and email_address"
elif missing_ip > 0:
    answer = "A. ip_address"
elif missing_email > 0:
    answer = "B. email_address"
else:
    answer = "D. neither"

print(f"Answer: {answer}")