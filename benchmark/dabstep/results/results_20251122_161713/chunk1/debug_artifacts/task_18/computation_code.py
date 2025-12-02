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

def execute_step():
    # Define the file path
    file_path = '/output/chunk1/data/context/payments.csv'
    
    # Load the dataset
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded data. Shape: {df.shape}")
    except FileNotFoundError:
        print(f"File not found at: {file_path}")
        return

    # Filter the data to include only rows where 'year' is 2023
    df_2023 = df[df['year'] == 2023].copy()
    print(f"Rows after filtering for year 2023: {len(df_2023)}")
    
    # Group by 'card_scheme' and calculate the mean of 'has_fraudulent_dispute'
    # 'has_fraudulent_dispute' is boolean, so mean() calculates the percentage of True values (fraud rate)
    fraud_rates = df_2023.groupby('card_scheme')['has_fraudulent_dispute'].mean()
    
    # Sort descending to see the ranking (optional, for debugging/verification)
    fraud_rates_sorted = fraud_rates.sort_values(ascending=False)
    print("\nFraud rates by card scheme (2023):")
    print(fraud_rates_sorted)
    
    # Identify the card scheme with the highest fraud rate
    # idxmax() returns the index (card_scheme name) corresponding to the maximum value
    highest_fraud_scheme = fraud_rates.idxmax()
    highest_fraud_rate = fraud_rates.max()
    
    print(f"\nScheme with highest fraud rate: {highest_fraud_scheme} ({highest_fraud_rate:.4%})")
    
    # The question asks "Which payment method...", so we print the name.
    print(highest_fraud_scheme)

if __name__ == "__main__":
    execute_step()