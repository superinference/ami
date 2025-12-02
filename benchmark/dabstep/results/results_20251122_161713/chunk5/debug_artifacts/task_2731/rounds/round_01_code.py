# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2731
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2818 characters (FULL CODE)
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
    # Path to the payments file
    payments_path = '/output/chunk5/data/context/payments.csv'
    
    # Load the dataframe
    try:
        df = pd.read_csv(payments_path)
    except FileNotFoundError:
        print(f"Error: File not found at {payments_path}")
        return

    # Define the filter criteria
    # Merchant: Rafa_AI
    # Timeframe: July (Day of year 182 to 212 inclusive)
    # Status: Fraudulent (has_fraudulent_dispute = True)
    
    merchant_filter = df['merchant'] == 'Rafa_AI'
    date_filter = (df['day_of_year'] >= 182) & (df['day_of_year'] <= 212)
    fraud_filter = df['has_fraudulent_dispute'] == True
    
    # Apply filters
    filtered_df = df[merchant_filter & date_filter & fraud_filter].copy()
    
    # Output results to verify the step
    print(f"Filtered Data Shape: {filtered_df.shape}")
    print(f"Total EUR Amount involved: {filtered_df['eur_amount'].sum():.2f}")
    print("\nFirst 5 rows of filtered data:")
    print(filtered_df[['psp_reference', 'merchant', 'day_of_year', 'eur_amount', 'card_scheme', 'aci']].head().to_string())

if __name__ == "__main__":
    execute_step()
