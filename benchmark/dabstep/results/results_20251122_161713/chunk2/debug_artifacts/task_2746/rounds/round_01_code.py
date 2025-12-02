# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2746
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

# Define file paths
payments_path = '/output/chunk2/data/context/payments.csv'
output_path = '/output/chunk2/data/context/rafa_oct_fraud.csv'

# Load the payments data
df = pd.read_csv(payments_path)

# Filter for 'Rafa_AI'
# Filter for October (day_of_year between 274 and 304 inclusive)
# Filter for fraudulent transactions (has_fraudulent_dispute is True)
rafa_oct_fraud = df[
    (df['merchant'] == 'Rafa_AI') &
    (df['day_of_year'] >= 274) &
    (df['day_of_year'] <= 304) &
    (df['has_fraudulent_dispute'] == True)
].copy()

# Verify against Ground Truth
# Ground Truth: Count 202, Avg_Amount 110.67
count = len(rafa_oct_fraud)
avg_amount = rafa_oct_fraud['eur_amount'].mean()

print(f"Filtered Transaction Count: {count}")
print(f"Average Amount: {avg_amount:.2f}")

# Check card schemes distribution to match ground truth
# Ground Truth Schemes: NexPay 62, SwiftCharge 24, TransactPlus 41, GlobalCard 75
print("\nCard Scheme Distribution:")
print(rafa_oct_fraud['card_scheme'].value_counts())

# Save the filtered dataset for the next step
rafa_oct_fraud.to_csv(output_path, index=False)
print(f"\nFiltered data saved to: {output_path}")
