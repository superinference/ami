# ═══════════════════════════════════════════════════════════
# Round 2 - Task 38
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3322 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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
def analyze_top_merchants_percentage():
    # Load the payments.csv file
    file_path = '/output/chunk1/data/context/payments.csv'
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return

    # Ensure eur_amount is float
    df['eur_amount'] = df['eur_amount'].apply(coerce_to_float)

    # Group by 'merchant' and calculate the sum of 'eur_amount' (volume) and count of transactions
    merchant_stats = df.groupby('merchant')['eur_amount'].agg(['sum', 'count'])

    # Sort the results in descending order by volume (sum of eur_amount)
    merchant_stats_sorted = merchant_stats.sort_values(by='sum', ascending=False)

    # Identify the top 2 merchants by volume
    top_2_merchants = merchant_stats_sorted.head(2)
    
    print("Top 2 Merchants by Volume:")
    print(top_2_merchants)

    # Calculate the total number of transactions for the top 2 merchants
    top_2_transaction_count = top_2_merchants['count'].sum()

    # Calculate the total number of transactions in the entire dataset
    total_transactions = len(df)

    # Calculate the percentage
    if total_transactions > 0:
        percentage = (top_2_transaction_count / total_transactions) * 100
    else:
        percentage = 0.0

    # Output the result
    print(f"\nTotal transactions for top 2 merchants: {top_2_transaction_count}")
    print(f"Total transactions in dataset: {total_transactions}")
    print(f"Percentage of transactions from top 2 merchants: {percentage:.2f}%")

if __name__ == "__main__":
    analyze_top_merchants_percentage()
