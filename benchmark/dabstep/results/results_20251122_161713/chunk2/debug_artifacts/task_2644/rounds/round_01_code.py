# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2644
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3255 characters (FULL CODE)
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
file_path = '/output/chunk2/data/context/payments.csv'
df = pd.read_csv(file_path)

# Filter for the specific merchant
merchant_name = 'Martinis_Fine_Steakhouse'
df_merchant = df[df['merchant'] == merchant_name]

# Filter for July (Day of Year 182 to 212 for non-leap year 2023)
# Jan(31)+Feb(28)+Mar(31)+Apr(30)+May(31)+Jun(30) = 181 days
# July 1st is day 182. July 31st is day 212.
df_july = df_merchant[(df_merchant['day_of_year'] >= 182) & (df_merchant['day_of_year'] <= 212)]

# Calculate verification metrics based on Ground Truth
count = len(df_july)
total_vol = df_july['eur_amount'].sum()
avg_amt = df_july['eur_amount'].mean()
credit_pct = (df_july['is_credit'].sum() / count) * 100
# Intra-country: issuer country == acquirer country. 
# Note: acquirer_country is in payments.csv, but sometimes needs to be joined from merchant_data if missing. 
# However, payments.csv schema shows 'acquirer_country' column exists.
intra_pct = (df_july[df_july['issuing_country'] == df_july['acquirer_country']].shape[0] / count) * 100
aci_counts = df_july['aci'].value_counts()

print(f"Filtered Data Summary for {merchant_name} in July:")
print(f"Count: {count}")
print(f"Total Volume: {total_vol:.2f}")
print(f"Average Amount: {avg_amt:.2f}")
print(f"Credit Percentage: {credit_pct:.2f}%")
print(f"Intra-country Percentage: {intra_pct:.2f}%")
print("\nACI Counts:")
print(aci_counts)

# Save the filtered dataframe for the next step (optional, but good practice in a pipeline)
# For this single script execution, printing the verification is the primary output.
