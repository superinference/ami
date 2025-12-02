# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1518
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3174 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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

# Main Analysis Script
def analyze_fees():
    # Path to the fees.json file
    fees_path = '/output/chunk4/data/context/fees.json'

    # Load the JSON file
    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {fees_path}")
        return

    # Create a DataFrame
    df_fees = pd.DataFrame(fees_data)

    # Ensure numeric columns are floats
    df_fees['fixed_amount'] = df_fees['fixed_amount'].apply(coerce_to_float)
    df_fees['rate'] = df_fees['rate'].apply(coerce_to_float)

    # Define the transaction amount
    transaction_amount = 1234.0

    # Calculate the fee for each rule based on the formula:
    # fee = fixed_amount + rate * transaction_value / 10000
    df_fees['calculated_fee'] = df_fees['fixed_amount'] + (df_fees['rate'] * transaction_amount / 10000)

    # Group by card_scheme and calculate the mean fee (average scenario)
    average_fees_by_scheme = df_fees.groupby('card_scheme')['calculated_fee'].mean()

    # Identify the card scheme with the highest average fee
    most_expensive_scheme = average_fees_by_scheme.idxmax()
    highest_avg_fee = average_fees_by_scheme.max()

    # Print intermediate results for verification
    print("Average fees by card scheme for 1234 EUR transaction:")
    print(average_fees_by_scheme)
    print("-" * 30)

    # Print the final answer
    print(most_expensive_scheme)

if __name__ == "__main__":
    analyze_fees()
