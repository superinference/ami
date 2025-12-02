# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2678
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2983 characters (FULL CODE)
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

# Define file paths
payments_path = '/output/chunk5/data/context/payments.csv'

# Load the payments data
df = pd.read_csv(payments_path)

# Filter for the specific merchant 'Belles_cookbook_store'
merchant_name = 'Belles_cookbook_store'
df_merchant = df[df['merchant'] == merchant_name]

# Filter for November (day_of_year between 305 and 334 inclusive)
# Note: 305 is Nov 1st, 334 is Nov 30th in non-leap year (2023 is non-leap)
df_november = df_merchant[(df_merchant['day_of_year'] >= 305) & (df_merchant['day_of_year'] <= 334)]

# Calculate verification metrics to compare with Ground Truth
# Ground Truth: Total_Vol: 100629.89, Fraud_Vol: 9244.17, Fraud_Rate: 9.1863%
total_vol = df_november['eur_amount'].sum()
fraud_vol = df_november[df_november['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = (fraud_vol / total_vol) * 100 if total_vol > 0 else 0

# Print results
print(f"Filtered data for {merchant_name} in November (Day 305-334)")
print(f"Row count: {len(df_november)}")
print(f"Total Volume: {total_vol:.2f}")
print(f"Fraud Volume: {fraud_vol:.2f}")
print(f"Fraud Rate (Vol): {fraud_rate:.4f}%")

# Display first few rows to check structure
print("\nSample rows:")
print(df_november[['psp_reference', 'merchant', 'day_of_year', 'eur_amount', 'card_scheme', 'has_fraudulent_dispute']].head())
