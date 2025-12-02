# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2623
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2582 characters (FULL CODE)
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

# Load the payments data
df = pd.read_csv('/output/chunk3/data/context/payments.csv')

# Define filter criteria
merchant_name = 'Martinis_Fine_Steakhouse'
start_day = 121
end_day = 151

# Filter for the specific merchant and date range (May)
filtered_df = df[
    (df['merchant'] == merchant_name) & 
    (df['day_of_year'] >= start_day) & 
    (df['day_of_year'] <= end_day)
]

# Calculate metrics
total_volume = filtered_df['eur_amount'].sum()
fraud_volume = filtered_df[filtered_df['has_fraudulent_dispute']]['eur_amount'].sum()
avg_amount = filtered_df['eur_amount'].mean()
transaction_count = len(filtered_df)

# Print the results
print(f"Metrics for {merchant_name} in May (Day {start_day}-{end_day}):")
print(f"Transaction Count: {transaction_count}")
print(f"Total Monthly Volume: {total_volume:.2f}")
print(f"Total Fraud Volume: {fraud_volume:.2f}")
print(f"Average Transaction Amount: {avg_amount:.2f}")
