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

# Main Analysis Script
def analyze_transactions():
    file_path = '/output/chunk2/data/context/payments.csv'
    
    try:
        # Load data
        df = pd.read_csv(file_path)
        
        # Define filters
        target_merchant = 'Belles_cookbook_store'
        target_scheme = 'SwiftCharge'
        
        # Date range: June 1st to October 31st, 2023
        # 2023 is not a leap year.
        # Jan: 31, Feb: 28, Mar: 31, Apr: 30, May: 31 -> Total 151 days before June 1st.
        # June 1st is day 152.
        # June(30) + July(31) + Aug(31) + Sept(30) + Oct(31) = 153 days.
        # End date = 151 + 153 = 304.
        start_day = 152
        end_day = 304
        
        # Apply filters
        # 1. Merchant
        # 2. Card Scheme
        # 3. Day of Year range
        filtered_df = df[
            (df['merchant'] == target_merchant) &
            (df['card_scheme'] == target_scheme) &
            (df['day_of_year'] >= start_day) &
            (df['day_of_year'] <= end_day)
        ]
        
        if filtered_df.empty:
            print("No transactions found matching the criteria.")
            return

        # Group by ip_country and calculate average eur_amount
        # Using groupby and mean()
        grouped_stats = filtered_df.groupby('ip_country')['eur_amount'].mean()
        
        # Sort for better readability (optional but good practice)
        grouped_stats = grouped_stats.sort_index()
        
        # Print the result in a clear format
        # The question asks "What is the average transaction value...", implying a breakdown.
        print("Average transaction value (EUR) by IP Country:")
        for country, avg_amount in grouped_stats.items():
            print(f"{country}: {avg_amount:.2f}")
            
        # Also print as a dictionary for programmatic parsing if needed
        # print("\nRaw Dictionary:")
        # print(grouped_stats.to_dict())

    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    analyze_transactions()