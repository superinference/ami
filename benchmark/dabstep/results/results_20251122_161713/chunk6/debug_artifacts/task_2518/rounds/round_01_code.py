# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2518
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2826 characters (FULL CODE)
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
import json

# Paths
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'

# 1. Load payments and filter
print("Loading payments data...")
df = pd.read_csv(payments_path)
rafa_txs = df[(df['merchant'] == 'Rafa_AI') & (df['year'] == 2023)].copy()

print(f"Total transactions for Rafa_AI in 2023: {len(rafa_txs)}")

# 2. Load fees and find ID=787
print("Loading fees data...")
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

fee_787 = next((fee for fee in fees_data if fee['ID'] == 787), None)

if fee_787:
    print(f"Fee ID 787 found.")
    print(f"Original Rate: {fee_787.get('rate')}")
    print(f"Fixed Amount: {fee_787.get('fixed_amount')}")
    print("Matching Criteria:")
    for key, value in fee_787.items():
        if key not in ['ID', 'rate', 'fixed_amount']:
            print(f"  {key}: {value}")
else:
    print("Fee ID 787 not found.")

# 3. Save intermediate data for the next step (optional, but good for verification in logs)
# We will just print the head of the dataframe to ensure it loaded correctly
print("\nSample of Rafa_AI transactions:")
print(rafa_txs.head())
