# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1840
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2824 characters (FULL CODE)
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
file_path = '/output/chunk4/data/context/payments.csv'
df = pd.read_csv(file_path)

# Define filter criteria
target_merchant = 'Golfclub_Baron_Friso'
start_day = 152  # June 1st
end_day = 181    # June 30th

# Apply filters: Merchant matches 'Golfclub_Baron_Friso' AND day_of_year is in June (152-181)
filtered_df = df[
    (df['merchant'] == target_merchant) & 
    (df['day_of_year'] >= start_day) & 
    (df['day_of_year'] <= end_day)
].copy()

# Output verification metrics
print(f"Successfully loaded and filtered data.")
print(f"Merchant: {target_merchant}")
print(f"Time Period: Day {start_day} to {end_day} (June 2023)")
print(f"Transaction Count: {len(filtered_df)}")

# Display a sample of the relevant columns for fee calculation to verify against ground truth
# Columns needed for fees: card_scheme, is_credit, eur_amount, aci, acquirer_country, ip_country, issuing_country
cols_to_show = ['psp_reference', 'day_of_year', 'card_scheme', 'is_credit', 'eur_amount', 'aci', 'acquirer_country', 'issuing_country']
print("\nSample filtered transactions:")
print(filtered_df[cols_to_show].head(10).to_string(index=False))
