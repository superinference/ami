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

# Define the file path
file_path = '/output/chunk3/data/context/payments.csv'

# Load the CSV file into a pandas DataFrame
try:
    df = pd.read_csv(file_path)
    
    # Filter criteria:
    # 1. Merchant: 'Rafa_AI'
    # 2. Card Scheme: 'SwiftCharge'
    # 3. Time period: May and June 2023 (Day of year 121 to 181 inclusive)
    
    # Create masks for filtering
    mask_merchant = df['merchant'] == 'Rafa_AI'
    mask_scheme = df['card_scheme'] == 'SwiftCharge'
    mask_date = (df['day_of_year'] >= 121) & (df['day_of_year'] <= 181)
    
    # Apply filters
    filtered_df = df[mask_merchant & mask_scheme & mask_date]
    
    # Check if data exists after filtering
    if filtered_df.empty:
        print("No transactions found matching the criteria.")
    else:
        # Group by 'shopper_interaction' and calculate the mean of 'eur_amount'
        result = filtered_df.groupby('shopper_interaction')['eur_amount'].mean()
        
        # Print the results clearly
        # The question asks for the average transaction value grouped by shopper_interaction
        for interaction, avg_value in result.items():
            print(f"{interaction} {avg_value:.4f}")

except FileNotFoundError:
    print(f"Error: The file at {file_path} was not found.")
except Exception as e:
    print(f"An error occurred while processing the data: {e}")