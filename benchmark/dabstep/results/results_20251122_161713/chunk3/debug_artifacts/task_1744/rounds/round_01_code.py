# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1744
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2631 characters (FULL CODE)
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

# Define file path
payments_path = '/output/chunk3/data/context/payments.csv'

# Load the payments dataset
df = pd.read_csv(payments_path)

# Filter the dataset for the specific merchant and year
merchant_name = "Martinis_Fine_Steakhouse"
target_year = 2023

filtered_df = df[
    (df['merchant'] == merchant_name) & 
    (df['year'] == target_year)
]

# Calculate summary metrics to verify against ground truth
total_volume = filtered_df['eur_amount'].sum()
fraud_volume = filtered_df[filtered_df['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = (fraud_volume / total_volume * 100) if total_volume > 0 else 0

# Print results
print(f"Data loaded and filtered for {merchant_name} in {target_year}")
print(f"Number of transactions: {len(filtered_df)}")
print(f"Total Volume: {total_volume:.2f}")
print(f"Fraud Volume: {fraud_volume:.2f}")
print(f"Fraud Rate: {fraud_rate:.2f}%")
print("\nSample of filtered data:")
print(filtered_df.head().to_string())
