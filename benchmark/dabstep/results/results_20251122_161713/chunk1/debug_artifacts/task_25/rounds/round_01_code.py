# ═══════════════════════════════════════════════════════════
# Round 1 - Task 25
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3113 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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
import numpy as np

def load_payments_data():
    """
    Loads the payments.csv file into a pandas DataFrame as the first step 
    of the outlier analysis plan.
    """
    # Define the file path based on the provided context
    file_path = '/output/chunk1/data/context/payments.csv'
    
    print(f"Attempting to load data from: {file_path}")
    
    try:
        # Load the CSV file
        df = pd.read_csv(file_path)
        
        # Verify the load
        print("Data loaded successfully.")
        print(f"DataFrame Shape: {df.shape}")
        
        # Display column names to verify structure matches expectations
        print("Columns:", df.columns.tolist())
        
        # Check specifically for the target column 'eur_amount' required for Z-score calculation
        if 'eur_amount' in df.columns:
            print(f"\nTarget column 'eur_amount' found. Data type: {df['eur_amount'].dtype}")
            # Display basic stats to get a sense of the distribution before outlier detection
            print(df['eur_amount'].describe())
        else:
            print("\nWARNING: 'eur_amount' column NOT found!")
            
        return df
        
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

if __name__ == "__main__":
    df = load_payments_data()
