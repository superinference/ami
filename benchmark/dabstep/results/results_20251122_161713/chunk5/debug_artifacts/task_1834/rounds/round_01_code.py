# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1834
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3223 characters (FULL CODE)
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

def load_and_filter_transactions():
    # Define file path
    payments_path = '/output/chunk5/data/context/payments.csv'
    
    try:
        # Load the payments data
        print(f"Loading data from {payments_path}...")
        df = pd.read_csv(payments_path)
        
        # Filter for merchant 'Crossfit_Hanna'
        merchant_filter = df['merchant'] == 'Crossfit_Hanna'
        
        # Filter for December 2023 (day_of_year >= 335)
        # Note: The prompt explicitly specifies day_of_year >= 335 for December
        date_filter = df['day_of_year'] >= 335
        
        # Apply filters
        df_filtered = df[merchant_filter & date_filter].copy()
        
        # Verification against Ground Truth
        count = len(df_filtered)
        total_volume = df_filtered['eur_amount'].sum()
        
        print(f"Filtering complete.")
        print(f"Merchant: Crossfit_Hanna")
        print(f"Timeframe: December 2023 (Day >= 335)")
        print(f"Transaction Count: {count}")
        print(f"Total Volume: {total_volume:.2f}")
        
        # Display a sample of the filtered data to ensure correctness
        print("\nSample of filtered transactions:")
        print(df_filtered[['psp_reference', 'merchant', 'day_of_year', 'eur_amount', 'card_scheme']].head())
        
        return df_filtered

    except FileNotFoundError:
        print(f"Error: The file {payments_path} was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    load_and_filter_transactions()
