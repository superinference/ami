# ═══════════════════════════════════════════════════════════
# Round 1 - Task 889
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2945 characters (FULL CODE)
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

# Define the file path based on the provided context
file_path = '/output/chunk6/data/context/payments.csv'

# Step 1: Load the payments.csv file into a pandas DataFrame
try:
    df = pd.read_csv(file_path)
    print("Successfully loaded payments.csv")
    
    # Verify the presence of columns required for the analysis:
    # merchant, card scheme, aci, amount (eur_amount), and date information (day_of_year, year)
    required_columns = ['merchant', 'card_scheme', 'aci', 'eur_amount', 'day_of_year', 'year']
    
    print("\nDataFrame Shape:", df.shape)
    
    print("\nColumns present in the DataFrame:")
    print(df.columns.tolist())
    
    # Check if specific required columns exist
    missing_cols = [col for col in required_columns if col not in df.columns]
    if not missing_cols:
        print(f"\nVerification Successful: All required columns {required_columns} are present.")
    else:
        print(f"\nWarning: Missing columns: {missing_cols}")
        
    # Display the first few rows to inspect the data structure
    print("\nFirst 5 rows of the DataFrame:")
    print(df.head())

except FileNotFoundError:
    print(f"Error: The file was not found at {file_path}")
except Exception as e:
    print(f"An error occurred while loading the file: {e}")
