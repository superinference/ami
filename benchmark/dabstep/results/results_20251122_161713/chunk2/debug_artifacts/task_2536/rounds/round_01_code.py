# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2536
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2830 characters (FULL CODE)
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

def execute_step():
    # Define file path
    payments_path = '/output/chunk2/data/context/payments.csv'
    
    # Load the payments.csv file
    print(f"Loading data from {payments_path}...")
    try:
        df = pd.read_csv(payments_path)
    except FileNotFoundError:
        print("Error: payments.csv not found.")
        return

    # Filter the data for 'Crossfit_Hanna'
    target_merchant = 'Crossfit_Hanna'
    merchant_txs = df[df['merchant'] == target_merchant].copy()
    
    # Print verification stats
    print(f"Total rows in dataset: {len(df)}")
    print(f"Rows for merchant '{target_merchant}': {len(merchant_txs)}")
    
    # Verify against ground truth context if available (55139 expected)
    expected_count = 55139
    if len(merchant_txs) == expected_count:
        print(f"Verification PASSED: Count matches expected value of {expected_count}.")
    else:
        print(f"Verification WARNING: Count {len(merchant_txs)} differs from expected {expected_count}.")

    # Display a sample of the filtered data
    print("\nFirst 5 rows of filtered data:")
    print(merchant_txs.head())

if __name__ == "__main__":
    execute_step()
