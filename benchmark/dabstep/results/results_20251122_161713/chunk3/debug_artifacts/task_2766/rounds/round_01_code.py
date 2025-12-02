# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2766
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2755 characters (FULL CODE)
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
    payments_path = '/output/chunk3/data/context/payments.csv'
    
    # Load the dataset
    print(f"Loading {payments_path}...")
    df = pd.read_csv(payments_path)
    
    # Step: Filter for merchant 'Rafa_AI' and year 2023
    # We use bitwise AND (&) for multiple conditions in pandas
    filtered_df = df[
        (df['merchant'] == 'Rafa_AI') & 
        (df['year'] == 2023)
    ].copy()
    
    # Output results to verify the step
    row_count = len(filtered_df)
    avg_amount = filtered_df['eur_amount'].mean()
    
    print("-" * 30)
    print("FILTERING RESULTS")
    print("-" * 30)
    print(f"Merchant: Rafa_AI")
    print(f"Year: 2023")
    print(f"Transaction Count: {row_count}")
    print(f"Average Amount: {avg_amount:.4f}")
    print("-" * 30)
    
    # Display a sample of the filtered data for inspection
    print("\nFirst 5 rows of filtered data:")
    print(filtered_df[['psp_reference', 'merchant', 'year', 'eur_amount', 'card_scheme', 'aci']].head().to_string())

if __name__ == "__main__":
    execute_step()
