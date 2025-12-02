# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2765
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3575 characters (FULL CODE)
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
df_payments = pd.read_csv(payments_path)

# Filter for Merchant 'Rafa_AI' and Year 2023
merchant_name = 'Rafa_AI'
df_rafa = df_payments[(df_payments['merchant'] == merchant_name) & (df_payments['year'] == 2023)]

# Calculate Total Volume for the year
total_volume_year = df_rafa['eur_amount'].sum()

# Calculate Average Monthly Volume
# The manual states "monthly_volume" is a rule based on monthly totals.
# Since we have a full year of data (2023), we calculate the average monthly volume.
avg_monthly_volume = total_volume_year / 12

# Calculate Fraud Volume
# The manual states "monthly_fraud_level" is the ratio between monthly total volume and monthly volume notified as fraud.
fraud_volume = df_rafa[df_rafa['has_fraudulent_dispute'] == True]['eur_amount'].sum()

# Calculate Fraud Rate (Volume-based)
if total_volume_year > 0:
    fraud_rate = fraud_volume / total_volume_year
else:
    fraud_rate = 0.0

# Load Merchant Data to get static attributes (MCC, Account Type) which are necessary for fee matching
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'
df_merchant_data = pd.read_json(merchant_data_path)
merchant_info = df_merchant_data[df_merchant_data['merchant'] == merchant_name].iloc[0]

# Output the calculated statistics
print(f"--- Analysis for {merchant_name} (2023) ---")
print(f"Total Yearly Volume: €{total_volume_year:,.2f}")
print(f"Average Monthly Volume: €{avg_monthly_volume:,.2f}")
print(f"Total Fraud Volume: €{fraud_volume:,.2f}")
print(f"Fraud Rate (Volume-based): {fraud_rate:.4%}")
print(f"Merchant Category Code (MCC): {merchant_info['merchant_category_code']}")
print(f"Account Type: {merchant_info['account_type']}")

# Additional stats for context
print(f"Total Transactions: {len(df_rafa)}")
print(f"Card Schemes Used: {list(df_rafa['card_scheme'].unique())}")
