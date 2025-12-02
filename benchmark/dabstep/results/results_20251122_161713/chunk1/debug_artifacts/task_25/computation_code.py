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

def load_payments_data():
    """
    Loads the payments.csv file into a pandas DataFrame.
    """
    # Define the file path based on the provided context
    file_path = '/output/chunk1/data/context/payments.csv'
    
    print(f"Attempting to load data from: {file_path}")
    
    try:
        # Load the CSV file
        df = pd.read_csv(file_path)
        print("Data loaded successfully.")
        return df
        
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def calculate_outliers(df):
    """
    Calculates the number of outliers in 'eur_amount' using Z-score > 3.
    Uses the specific mean and standard deviation provided in the instructions.
    """
    if df is None:
        return

    if 'eur_amount' not in df.columns:
        print("Error: 'eur_amount' column not found in DataFrame.")
        return

    # Constants provided in the instructions
    MEAN_VAL = 91.852321
    STD_DEV = 121.730514

    print(f"Calculating Z-scores using Mean={MEAN_VAL} and StdDev={STD_DEV}")

    # Calculate Z-score: (Value - Mean) / StdDev
    # We use the .copy() to avoid SettingWithCopyWarning if df is a slice
    df = df.copy()
    df['z_score'] = (df['eur_amount'] - MEAN_VAL) / STD_DEV

    # Filter for outliers where absolute Z-score > 3
    outliers = df[df['z_score'].abs() > 3]
    
    outlier_count = len(outliers)
    
    print(f"Total rows: {len(df)}")
    print(f"Outliers found (|Z| > 3): {outlier_count}")
    
    # Print the final answer as a clean integer
    print(outlier_count)

if __name__ == "__main__":
    df = load_payments_data()
    calculate_outliers(df)