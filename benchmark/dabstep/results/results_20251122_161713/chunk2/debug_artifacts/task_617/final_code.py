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
    # Load the payments.csv file
    file_path = '/output/chunk2/data/context/payments.csv'
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return

    # Filter criteria
    # Merchant: 'Rafa_AI'
    # Card Scheme: 'SwiftCharge'
    # Date Range: May and June 2023
    # Calculation:
    # Jan=31, Feb=28, Mar=31, Apr=30 -> Total 120 days
    # May starts Day 121. May(31) + June(30) = 61 days. End Day = 120 + 61 = 181.
    
    filtered_df = df[
        (df['merchant'] == 'Rafa_AI') &
        (df['card_scheme'] == 'SwiftCharge') &
        (df['day_of_year'] >= 121) &
        (df['day_of_year'] <= 181)
    ]

    if filtered_df.empty:
        print("No transactions found matching the criteria.")
        return

    # Group by device_type and calculate average eur_amount
    # We use .mean() which automatically handles the float calculation
    avg_transaction_by_device = filtered_df.groupby('device_type')['eur_amount'].mean()

    # Sort for better readability (optional but good practice)
    avg_transaction_by_device = avg_transaction_by_device.sort_values(ascending=False)

    print("Average transaction value grouped by device_type for Rafa_AI (SwiftCharge, May-June 2023):")
    print(avg_transaction_by_device)
    
    # Also print formatted version for clarity
    print("\nFormatted Results:")
    for device, amount in avg_transaction_by_device.items():
        print(f"{device}: {amount:.2f}")

if __name__ == "__main__":
    analyze_transactions()