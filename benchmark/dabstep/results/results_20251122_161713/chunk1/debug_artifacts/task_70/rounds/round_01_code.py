# ═══════════════════════════════════════════════════════════
# Round 1 - Task 70
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 3231 characters (FULL CODE)
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

# Step 1: Calculate metric
# Load the payments data
df = pd.read_csv('/output/chunk1/data/context/payments.csv')

# Filter for the merchant "Martinis_Fine_Steakhouse"
merchant_name = "Martinis_Fine_Steakhouse"
merchant_txs = df[df['merchant'] == merchant_name]

# Calculate fraud rate defined as: transactions with has_fraudulent_dispute = True / total transactions
total_transactions = len(merchant_txs)
fraud_transactions = merchant_txs['has_fraudulent_dispute'].sum()

if total_transactions > 0:
    fraud_rate = fraud_transactions / total_transactions
else:
    fraud_rate = 0.0

# Step 2: Check manual.md for policy
with open('/output/chunk1/data/context/manual.md', 'r') as f:
    manual = f.read()

# Search for policy keywords
# The manual mentions "penalties" (Section 7.2, 7.4) and "fraud levels" (Section 5).
# While a specific numeric threshold for a "fine" (e.g., "1%") is not explicitly stated in the provided text,
# the calculated fraud rate for this merchant is ~8.0% (0.08), which is extremely high compared to 
# standard industry thresholds (typically 0.9% or 1% for monitoring programs/fines).
# Given the question asks if they are in "danger", and the manual mentions penalties for not maintaining desired levels,
# we compare against a standard high-risk threshold (1%).

if 'fine' in manual.lower() or 'penalty' in manual.lower():
    # Using 0.01 (1%) as the threshold for "danger" of fines/penalties
    threshold = 0.01
    if fraud_rate > threshold:
        print("yes")
    else:
        print("no")
else:
    print("Not Applicable")
