# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1280
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2772 characters (FULL CODE)
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
    payments_path = '/output/chunk6/data/context/payments.csv'
    
    # Load the payments dataset
    try:
        df = pd.read_csv(payments_path)
        print(f"Successfully loaded payments.csv with {len(df)} rows.")
    except FileNotFoundError:
        print("Error: payments.csv not found.")
        return

    # Ensure is_credit is boolean (handling potential string representations)
    if df['is_credit'].dtype == 'object':
        df['is_credit'] = df['is_credit'].map({'True': True, 'False': False, True: True, False: False})

    # Filter for rows where card_scheme is 'TransactPlus' and is_credit is True
    filtered_df = df[
        (df['card_scheme'] == 'TransactPlus') & 
        (df['is_credit'] == True)
    ]
    
    # Output results
    print(f"Filtered rows (TransactPlus + Credit): {len(filtered_df)}")
    print("Sample of filtered data:")
    print(filtered_df.head())
    
    # Return the dataframe for potential next steps (or save it if this were a pipeline)
    return filtered_df

if __name__ == "__main__":
    execute_step()
