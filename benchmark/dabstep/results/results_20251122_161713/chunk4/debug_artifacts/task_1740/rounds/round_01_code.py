# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1740
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3156 characters (FULL CODE)
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
    payments_path = '/output/chunk4/data/context/payments.csv'
    
    try:
        # Load the dataset
        print("Loading payments.csv...")
        df = pd.read_csv(payments_path)
        
        # Apply filters: Merchant 'Rafa_AI', Year 2023, Day 365
        print("Filtering for Rafa_AI on day 365 of 2023...")
        condition = (
            (df['merchant'] == 'Rafa_AI') & 
            (df['year'] == 2023) & 
            (df['day_of_year'] == 365)
        )
        
        rafa_txs = df[condition].copy()
        
        # Display summary
        print(f"Total transactions found: {len(rafa_txs)}")
        
        # Display first few rows with relevant columns for verification
        cols_to_show = ['psp_reference', 'merchant', 'card_scheme', 'eur_amount', 
                        'day_of_year', 'issuing_country', 'acquirer_country', 'is_credit']
        print("\nSample of filtered transactions:")
        print(rafa_txs[cols_to_show].head().to_string())
        
        # Save filtered data for the next analysis step
        output_file = 'rafa_ai_day_365_2023.csv'
        rafa_txs.to_csv(output_file, index=False)
        print(f"\nFiltered data saved to {output_file}")
        
    except FileNotFoundError:
        print(f"Error: File not found at {payments_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    load_and_filter_transactions()
