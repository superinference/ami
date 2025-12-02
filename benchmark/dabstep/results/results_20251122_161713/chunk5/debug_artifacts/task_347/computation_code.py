import pandas as pd

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

# Main Analysis Script
file_path = '/output/chunk5/data/context/payments.csv'

try:
    # Load the data
    df = pd.read_csv(file_path)
    
    # Define filter criteria
    target_merchant = 'Golfclub_Baron_Friso'
    target_scheme = 'NexPay'
    start_day = 121  # May 1st
    end_day = 181    # June 30th
    
    # Apply filters
    # 1. Merchant
    # 2. Card Scheme
    # 3. Date Range (May-June)
    mask = (
        (df['merchant'] == target_merchant) &
        (df['card_scheme'] == target_scheme) &
        (df['day_of_year'] >= start_day) &
        (df['day_of_year'] <= end_day)
    )
    
    filtered_df = df[mask]
    
    if filtered_df.empty:
        print("No transactions found matching the criteria.")
    else:
        # Group by issuing_country and calculate mean eur_amount
        avg_transaction_value = filtered_df.groupby('issuing_country')['eur_amount'].mean()
        
        # Print the results formatted to 2 decimal places
        print("Average transaction value grouped by issuing_country:")
        for country, value in avg_transaction_value.items():
            print(f"{country}: {value:.2f}")

except FileNotFoundError:
    print(f"Error: File not found at {file_path}")
except Exception as e:
    print(f"An error occurred: {e}")