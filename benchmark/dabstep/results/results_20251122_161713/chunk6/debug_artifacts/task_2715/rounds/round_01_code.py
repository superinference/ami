# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2715
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3444 characters (FULL CODE)
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

def solve():
    # Load the payments dataset
    file_path = '/output/chunk6/data/context/payments.csv'
    df = pd.read_csv(file_path)

    # Filter for Merchant "Martinis_Fine_Steakhouse"
    merchant_name = "Martinis_Fine_Steakhouse"
    df_merchant = df[df['merchant'] == merchant_name]

    # Filter for April (Month 4)
    # The dataset uses 'day_of_year'. 2023 is a non-leap year.
    # Jan (31) + Feb (28) + Mar (31) = 90 days.
    # April starts on day 91 and ends on day 120 (30 days).
    df_april = df_merchant[(df_merchant['day_of_year'] >= 91) & (df_merchant['day_of_year'] <= 120)]

    # Calculate Merchant's Total Monthly Volume (needed for Fraud Rate calculation)
    monthly_total_volume = df_april['eur_amount'].sum()
    
    # Isolate Fraudulent Transactions
    fraud_txs = df_april[df_april['has_fraudulent_dispute'] == True].copy()
    
    # Calculate Fraud Metrics
    fraud_volume = fraud_txs['eur_amount'].sum()
    fraud_count = len(fraud_txs)
    
    # Calculate Fraud Rate (Volume-based as per Manual Section 7)
    # "Fraud is defined as the ratio of fraudulent volume over total volume."
    if monthly_total_volume > 0:
        fraud_rate = fraud_volume / monthly_total_volume
    else:
        fraud_rate = 0.0

    # Output results
    print(f"--- Analysis for {merchant_name} in April ---")
    print(f"Total Monthly Volume: {monthly_total_volume:.2f}")
    print(f"Fraudulent Volume: {fraud_volume:.2f}")
    print(f"Fraudulent Transactions Count: {fraud_count}")
    print(f"Fraud Rate (Volume): {fraud_rate:.4%}")
    
    print("\n--- Sample of Isolated Fraudulent Transactions ---")
    print(fraud_txs[['psp_reference', 'eur_amount', 'card_scheme', 'aci', 'has_fraudulent_dispute']].head())

if __name__ == "__main__":
    solve()
