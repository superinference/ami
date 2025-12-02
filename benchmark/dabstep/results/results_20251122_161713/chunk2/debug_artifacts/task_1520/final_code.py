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
import json

# Define file path
fees_file_path = '/output/chunk2/data/context/fees.json'

# Step 1: Load the fees.json file into a DataFrame
try:
    with open(fees_file_path, 'r') as f:
        fees_data = json.load(f)
    
    df_fees = pd.DataFrame(fees_data)
    
    # Verify the DataFrame structure and relevant columns
    print("Fees DataFrame loaded successfully.")
    print(f"Shape: {df_fees.shape}")
    print(f"Columns: {df_fees.columns.tolist()}")
    
    # Check for required columns
    required_columns = ['fixed_amount', 'rate', 'card_scheme']
    if all(col in df_fees.columns for col in required_columns):
        print(f"\nRequired columns {required_columns} are present.")
        
        # Preview the data
        print("\nFirst 5 rows of relevant columns:")
        print(df_fees[required_columns].head())
        
        # Perform calculation for the specific question: 
        # "In the average scenario, which card scheme would provide the most expensive fee for a transaction value of 4321 EUR?"
        # Interpretation: Calculate the fee for 4321 EUR for ALL rules, then average by card_scheme.
        
        transaction_val = 4321
        
        # Fee formula: fixed_amount + (rate / 10000 * amount)
        # Note: rate is in basis points (per 10,000)
        df_fees['calculated_fee_4321'] = df_fees['fixed_amount'] + (df_fees['rate'] / 10000 * transaction_val)
        
        # Group by card_scheme and calculate the mean fee
        average_fees = df_fees.groupby('card_scheme')['calculated_fee_4321'].mean().sort_values(ascending=False)
        
        print("\nAverage Fee per Card Scheme for 4321 EUR Transaction:")
        print(average_fees)
        
        most_expensive_scheme = average_fees.idxmax()
        max_fee = average_fees.max()
        
        print(f"\nMost expensive scheme in the average scenario: {most_expensive_scheme} ({max_fee:.4f} EUR)")
        
    else:
        print(f"Error: Missing one or more required columns: {required_columns}")

except Exception as e:
    print(f"An error occurred: {e}")