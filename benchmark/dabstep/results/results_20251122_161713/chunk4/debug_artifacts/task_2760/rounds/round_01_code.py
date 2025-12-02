# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2760
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3622 characters (FULL CODE)
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

def calculate_merchant_metrics():
    # Load payments data
    payments_path = '/output/chunk4/data/context/payments.csv'
    df = pd.read_csv(payments_path)
    
    # Filter for merchant 'Crossfit_Hanna' and year 2023
    merchant_name = 'Crossfit_Hanna'
    target_year = 2023
    
    df_merchant = df[
        (df['merchant'] == merchant_name) & 
        (df['year'] == target_year)
    ].copy()
    
    if df_merchant.empty:
        print(f"No data found for {merchant_name} in {target_year}")
        return

    # Calculate metrics required for fee tier matching
    
    # 1. Total Volume and Count
    total_volume = df_merchant['eur_amount'].sum()
    count = len(df_merchant)
    
    # 2. Average Transaction Amount
    avg_transaction_amount = total_volume / count if count > 0 else 0
    
    # 3. Monthly Transaction Volume 
    # Since the dataset covers the full year 2023 (day_of_year 1-365), we divide total by 12
    monthly_transaction_volume = total_volume / 12
    
    # 4. Monthly Fraud Rate (Volume-based)
    # fees.json defines this as "ratio between monthly total volume and monthly volume notified as fraud"
    # We calculate the annualized ratio which represents the average monthly level.
    fraud_volume = df_merchant[df_merchant['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0
    
    # Output results
    print(f"--- Metrics for {merchant_name} in {target_year} ---")
    print(f"Total Transaction Count: {count}")
    print(f"Total Volume: {total_volume:.2f}")
    print(f"Average Transaction Amount: {avg_transaction_amount:.2f}")
    print(f"Monthly Transaction Volume (Avg): {monthly_transaction_volume:.2f}")
    print(f"Total Fraud Volume: {fraud_volume:.2f}")
    print(f"Fraud Rate (Volume-based): {fraud_rate:.4%}")
    print(f"Fraud Rate (Raw): {fraud_rate:.6f}")

if __name__ == "__main__":
    calculate_merchant_metrics()
