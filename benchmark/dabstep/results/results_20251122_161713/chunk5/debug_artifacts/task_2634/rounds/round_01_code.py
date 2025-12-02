# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2634
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3083 characters (FULL CODE)
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

# Load the payments data
payments_path = '/output/chunk5/data/context/payments.csv'
df = pd.read_csv(payments_path)

# Filter for the specific merchant
merchant_name = 'Martinis_Fine_Steakhouse'
df_merchant = df[df['merchant'] == merchant_name]

# Filter for the month of June
# 2023 is not a leap year.
# Days: Jan(31) + Feb(28) + Mar(31) + Apr(30) + May(31) = 151 days.
# June starts on day 152 and has 30 days, ending on day 181.
june_start = 152
june_end = 181
df_june = df_merchant[(df_merchant['day_of_year'] >= june_start) & (df_merchant['day_of_year'] <= june_end)]

# Calculate transaction profile metrics
total_volume = df_june['eur_amount'].sum()
avg_amount = df_june['eur_amount'].mean()
tx_count = len(df_june)

# Calculate additional details useful for fee analysis (Credit, Intracountry, ACI)
credit_count = df_june['is_credit'].sum()
# Intracountry is defined as issuing_country == acquirer_country
intra_count = (df_june['issuing_country'] == df_june['acquirer_country']).sum()
aci_counts = df_june['aci'].value_counts().to_dict()

# Print the established profile
print(f"Transaction Profile for {merchant_name} in June (Days {june_start}-{june_end}):")
print(f"Total Volume: {total_volume:.2f}")
print(f"Average Amount: {avg_amount:.2f}")
print(f"Transaction Count: {tx_count}")
print(f"Credit Count: {credit_count}")
print(f"Intracountry Count: {intra_count}")
print(f"ACI Counts: {aci_counts}")
