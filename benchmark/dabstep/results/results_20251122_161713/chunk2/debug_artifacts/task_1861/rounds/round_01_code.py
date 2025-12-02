# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1861
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3197 characters (FULL CODE)
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
import os

# Define file paths
payments_path = '/output/chunk2/data/context/payments.csv'
output_path = 'rafa_ai_march_transactions.pkl'

# Load the payments data
print(f"Loading payments data from {payments_path}...")
df_payments = pd.read_csv(payments_path)

# Filter for merchant 'Rafa_AI'
# Note: 'Rafa_AI' is confirmed as a merchant in the ground truth, though not in the sample unique values list in the prompt description (which only showed 5). 
# The prompt description says "merchant: 5 unique" but lists 'Rafa_AI' as one of them.
df_rafa = df_payments[df_payments['merchant'] == 'Rafa_AI'].copy()

# Filter for March 2023
# 2023 is a non-leap year.
# Jan: 31 days
# Feb: 28 days
# March starts on day 60 (31+28+1) and ends on day 90 (31+28+31).
# The plan specifies day_of_year between 60 and 90 inclusive.
df_rafa_march = df_rafa[(df_rafa['day_of_year'] >= 60) & (df_rafa['day_of_year'] <= 90)]

# Verify the filtering
print(f"Total transactions for Rafa_AI: {len(df_rafa)}")
print(f"Transactions for Rafa_AI in March (Day 60-90): {len(df_rafa_march)}")

# Display sample data to ensure columns needed for fee calculation are present
# Columns needed: eur_amount, card_scheme, is_credit, aci, acquirer_country, issuing_country, merchant
print("\nSample of filtered data:")
print(df_rafa_march[['psp_reference', 'day_of_year', 'eur_amount', 'card_scheme', 'is_credit', 'aci']].head())

# Save the filtered dataframe for the next step
df_rafa_march.to_pickle(output_path)
print(f"\nFiltered data saved to {output_path}")
