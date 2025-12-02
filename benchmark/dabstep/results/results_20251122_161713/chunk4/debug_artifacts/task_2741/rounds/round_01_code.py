# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2741
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2920 characters (FULL CODE)
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
payments_path = '/output/chunk4/data/context/payments.csv'

# Load the dataset
try:
    df = pd.read_csv(payments_path)
    print(f"Successfully loaded payments.csv with {len(df)} rows.")
except FileNotFoundError:
    print(f"Error: File not found at {payments_path}")
    exit()

# Define filter criteria
merchant_name = 'Rafa_AI'
start_day = 244  # September 1st (non-leap year)
end_day = 273    # September 30th
is_fraud = True

# Apply filters
# 1. Merchant is 'Rafa_AI'
# 2. Month is September (day_of_year 244-273)
# 3. has_fraudulent_dispute is True
filtered_df = df[
    (df['merchant'] == merchant_name) &
    (df['day_of_year'] >= start_day) &
    (df['day_of_year'] <= end_day) &
    (df['has_fraudulent_dispute'] == is_fraud)
]

# Display results
print(f"Filtered transactions for {merchant_name} in September (Fraudulent): {len(filtered_df)}")

print("\nSample of filtered data (relevant columns):")
print(filtered_df[['psp_reference', 'eur_amount', 'aci', 'card_scheme', 'issuing_country', 'acquirer_country']].head(10))

# Show distribution of current ACIs to understand the starting point for the analysis
print("\nCurrent ACI distribution in these fraudulent transactions:")
print(filtered_df['aci'].value_counts())
