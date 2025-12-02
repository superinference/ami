# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1729
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3567 characters (FULL CODE)
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

# Set display options to ensure output is readable
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None)

def load_and_filter_data():
    # Define file path
    file_path = '/output/chunk2/data/context/payments.csv'
    
    try:
        # Load the payments dataset
        print(f"Loading data from {file_path}...")
        df = pd.read_csv(file_path)
        
        # Define filter criteria
        target_merchant = "Martinis_Fine_Steakhouse"
        target_year = 2023
        target_day = 10
        
        # Apply filters
        # Criteria: merchant is "Martinis_Fine_Steakhouse", year is 2023, day_of_year is 10
        filtered_df = df[
            (df['merchant'] == target_merchant) &
            (df['year'] == target_year) &
            (df['day_of_year'] == target_day)
        ].copy()
        
        # Output results
        print(f"\nFilter applied: Merchant='{target_merchant}', Year={target_year}, Day={target_day}")
        print(f"Number of transactions found: {len(filtered_df)}")
        
        # Display relevant columns to verify against ground truth
        # Columns needed for fee calculation: card_scheme, is_credit, eur_amount, issuing_country, aci, acquirer_country
        columns_of_interest = ['card_scheme', 'is_credit', 'eur_amount', 'issuing_country', 'aci', 'acquirer_country']
        
        if not filtered_df.empty:
            print("\nFiltered Transactions:")
            print(filtered_df[columns_of_interest].to_string(index=False))
        else:
            print("No transactions found matching the criteria.")
            
        return filtered_df
        
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    load_and_filter_data()
