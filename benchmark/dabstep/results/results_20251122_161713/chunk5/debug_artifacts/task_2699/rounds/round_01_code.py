# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2699
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2777 characters (FULL CODE)
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
    # Load the payments dataset
    file_path = '/output/chunk5/data/context/payments.csv'
    df = pd.read_csv(file_path)

    # Apply filters based on the plan step:
    # 1. Merchant is 'Golfclub_Baron_Friso'
    # 2. Month is January (day_of_year between 1 and 31)
    # 3. Transaction is fraudulent (has_fraudulent_dispute is True)
    
    filtered_df = df[
        (df['merchant'] == 'Golfclub_Baron_Friso') &
        (df['day_of_year'] >= 1) & 
        (df['day_of_year'] <= 31) &
        (df['has_fraudulent_dispute'] == True)
    ].copy()

    # Output the result to verify the filter
    print(f"Filtered DataFrame Shape: {filtered_df.shape}")
    print("\nFirst 5 rows of filtered data:")
    print(filtered_df[['psp_reference', 'merchant', 'day_of_year', 'eur_amount', 'aci', 'card_scheme', 'has_fraudulent_dispute']].head())
    
    # Check unique ACIs currently present in this fraudulent set (useful context for the overall goal)
    print("\nCurrent ACIs in fraudulent transactions:")
    print(filtered_df['aci'].unique())

if __name__ == "__main__":
    execute_step()
