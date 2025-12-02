# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1739
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2697 characters (FULL CODE)
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

def load_and_filter_data():
    # Define the file path
    payments_path = '/output/chunk5/data/context/payments.csv'
    
    # Load the payments data
    print(f"Loading data from {payments_path}...")
    df = pd.read_csv(payments_path)
    
    # Filter for transactions where the merchant is 'Rafa_AI'
    target_merchant = 'Rafa_AI'
    rafa_ai_transactions = df[df['merchant'] == target_merchant]
    
    # Verify the filtering
    print(f"Total transactions in file: {len(df)}")
    print(f"Transactions for '{target_merchant}': {len(rafa_ai_transactions)}")
    
    # Display the first few rows of the filtered data to inspect columns
    print("\nFirst 5 transactions for Rafa_AI:")
    print(rafa_ai_transactions.head())
    
    # Check if day 300 exists in this filtered set (anticipating next steps)
    day_300_count = len(rafa_ai_transactions[rafa_ai_transactions['day_of_year'] == 300])
    print(f"\nTransactions on Day 300 for Rafa_AI: {day_300_count}")

if __name__ == "__main__":
    load_and_filter_data()
