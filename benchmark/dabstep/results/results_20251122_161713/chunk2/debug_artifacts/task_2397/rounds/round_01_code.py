# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2397
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2539 characters (FULL CODE)
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
    # Load the payments.csv file
    file_path = '/output/chunk2/data/context/payments.csv'
    df = pd.read_csv(file_path)

    # Filter for transactions where the merchant is 'Rafa_AI'
    # and the day_of_year corresponds to May 2023 (days 121 to 151)
    # 2023 is a non-leap year:
    # Jan (31) + Feb (28) + Mar (31) + Apr (30) = 120 days.
    # May starts on day 121 and ends on day 151 (31 days).
    
    rafa_may_df = df[
        (df['merchant'] == 'Rafa_AI') & 
        (df['day_of_year'] >= 121) & 
        (df['day_of_year'] <= 151)
    ].copy()

    # Print the result to verify the step
    print(f"Filtered DataFrame shape: {rafa_may_df.shape}")
    print(f"Total EUR Amount: {rafa_may_df['eur_amount'].sum()}")
    print("\nFirst 5 rows of filtered data:")
    print(rafa_may_df.head())

if __name__ == "__main__":
    execute_step()
