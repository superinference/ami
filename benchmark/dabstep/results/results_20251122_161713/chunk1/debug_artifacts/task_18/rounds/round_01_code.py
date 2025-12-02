# ═══════════════════════════════════════════════════════════
# Round 1 - Task 18
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2494 characters (FULL CODE)
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
    # Note: Based on pre-exploration, all rows are 2023, but this step ensures robustness.
    df_2023 = df[df['year'] == 2023].copy()
    
    # Verify the filtering
    print(f"Rows after filtering for year 2023: {len(df_2023)}")
    
    # Display first few rows to confirm structure
    print("\nFirst 5 rows of filtered data:")
    print(df_2023.head())

    return df_2023

if __name__ == "__main__":
    execute_step()
