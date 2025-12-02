# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2441
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3534 characters (FULL CODE)
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

# Define file paths
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

# Load data
print("Loading data files...")
try:
    payments_df = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchant_data = json.load(f)
    print("Files loaded successfully.")
except FileNotFoundError as e:
    print(f"Error loading files: {e}")
    exit()

# Filter criteria
target_merchant = 'Rafa_AI'
target_year = 2023
start_day = 274  # October 1st
end_day = 304    # October 31st

# Apply filtering
print(f"Filtering for merchant '{target_merchant}' in October {target_year} (Day {start_day}-{end_day})...")
filtered_df = payments_df[
    (payments_df['merchant'] == target_merchant) &
    (payments_df['year'] == target_year) &
    (payments_df['day_of_year'] >= start_day) &
    (payments_df['day_of_year'] <= end_day)
].copy()

# Verification
print(f"\nFiltered Dataset Summary:")
print(f"Total transactions found: {len(filtered_df)}")
print(f"Day of year range: {filtered_df['day_of_year'].min()} to {filtered_df['day_of_year'].max()}")

# Display sample rows to verify against ground truth
# Columns chosen to match the ground truth sample format: Scheme, Credit, Amount, Issuing, ACI, Acquirer
print("\nSample transactions (first 10):")
sample_cols = ['card_scheme', 'is_credit', 'eur_amount', 'issuing_country', 'aci', 'acquirer_country']
print(filtered_df[sample_cols].head(10))

# Check if the count matches the expected 2369
if len(filtered_df) == 2369:
    print("\nSUCCESS: Transaction count matches expected volume (2369).")
else:
    print(f"\nWARNING: Transaction count ({len(filtered_df)}) differs from expected (2369).")
