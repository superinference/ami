# ═══════════════════════════════════════════════════════════
# Round 1 - Task 71
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3168 characters (FULL CODE)
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

# Load the payments dataset
file_path = '/output/chunk1/data/context/payments.csv'
df = pd.read_csv(file_path)

# Step 1: Calculate the fraud rate for each merchant based on volume (eur_amount)
# Filter for fraudulent transactions
fraudulent_txns = df[df['has_fraudulent_dispute'] == True]

# Group by merchant and sum the eur_amount for fraudulent transactions
fraud_volume_by_merchant = fraudulent_txns.groupby('merchant')['eur_amount'].sum()

# Group by merchant and sum the eur_amount for all transactions
total_volume_by_merchant = df.groupby('merchant')['eur_amount'].sum()

# Calculate the fraud rate (Fraud Volume / Total Volume)
# We use .reindex to ensure all merchants are included, filling missing fraud volumes with 0
fraud_rates = (fraud_volume_by_merchant.reindex(total_volume_by_merchant.index, fill_value=0) / total_volume_by_merchant)

# Convert to percentage for easier reading
fraud_rates_percentage = fraud_rates * 100

print("Fraud Rates by Merchant (Volume-based):")
print(fraud_rates_percentage)

# For verification against ground truth hints (which seemed count-based), let's also calculate count-based rates
fraud_count_by_merchant = fraudulent_txns.groupby('merchant').size()
total_count_by_merchant = df.groupby('merchant').size()
fraud_rates_count_percentage = (fraud_count_by_merchant.reindex(total_count_by_merchant.index, fill_value=0) / total_count_by_merchant) * 100

print("\n(Reference) Fraud Rates by Merchant (Count-based):")
print(fraud_rates_count_percentage)
