import pandas as pd
import json

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

def main():
    # Define file path
    fees_file_path = '/output/chunk6/data/context/fees.json'

    # Load fees.json into a dataframe
    try:
        with open(fees_file_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {fees_file_path}")
        return

    df_fees = pd.DataFrame(fees_data)

    # Define filtering function for account_type
    # Logic: If the list is empty, it applies to ALL account types (wildcard).
    # If it is not empty, 'H' must be present in the list.
    def filter_account_type(account_type_list):
        if not account_type_list:
            return True
        return 'H' in account_type_list

    # Apply filters:
    # 1. card_scheme must be 'SwiftCharge'
    # 2. account_type must contain 'H' or be empty
    df_filtered = df_fees[
        (df_fees['card_scheme'] == 'SwiftCharge') & 
        (df_fees['account_type'].apply(filter_account_type))
    ].copy()

    if df_filtered.empty:
        print("No matching fee rules found.")
        return

    # Calculate Fee
    # Formula from manual.md: fee = fixed_amount + rate * transaction_value / 10000
    transaction_value = 4321.0

    # Ensure numeric types using helper
    df_filtered['fixed_amount'] = df_filtered['fixed_amount'].apply(coerce_to_float)
    df_filtered['rate'] = df_filtered['rate'].apply(coerce_to_float)

    # Perform calculation for each applicable rule
    df_filtered['calculated_fee'] = df_filtered['fixed_amount'] + (df_filtered['rate'] * transaction_value / 10000)

    # Calculate Average of the calculated fees
    average_fee = df_filtered['calculated_fee'].mean()

    # Output result formatted to 6 decimals as requested
    print(f"{average_fee:.6f}")

if __name__ == "__main__":
    main()