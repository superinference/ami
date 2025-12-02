# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2762
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 4398 characters (FULL CODE)
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

def execute_step():
    # File paths
    merchant_data_path = '/output/chunk4/data/context/merchant_data.json'
    payments_path = '/output/chunk4/data/context/payments.csv'

    # 1. Load Merchant Data
    try:
        with open(merchant_data_path, 'r') as f:
            merchant_data = json.load(f)
    except Exception as e:
        print(f"Error loading merchant_data.json: {e}")
        return

    # Retrieve metadata for 'Golfclub_Baron_Friso'
    target_merchant = 'Golfclub_Baron_Friso'
    merchant_metadata = next((item for item in merchant_data if item["merchant"] == target_merchant), None)

    if not merchant_metadata:
        print(f"Merchant '{target_merchant}' not found in merchant_data.json")
        return

    # 2. Load Payments Data
    try:
        df = pd.read_csv(payments_path)
    except Exception as e:
        print(f"Error loading payments.csv: {e}")
        return

    # Filter for merchant and year 2023
    df_merchant = df[(df['merchant'] == target_merchant) & (df['year'] == 2023)]

    if df_merchant.empty:
        print(f"No payment records found for '{target_merchant}' in 2023.")
        return

    # 3. Calculate Metrics
    # Total Volume
    total_volume = df_merchant['eur_amount'].sum()
    
    # Monthly Volume (Average)
    # We divide by 12 to estimate the monthly volume for fee bracket comparison
    avg_monthly_volume = total_volume / 12

    # Fraud Volume
    # 'has_fraudulent_dispute' is boolean
    fraud_volume = df_merchant[df_merchant['has_fraudulent_dispute'] == True]['eur_amount'].sum()

    # Monthly Fraud Level (Fraud Rate)
    # Defined as Fraud Volume / Total Volume (ratio)
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

    # Average Transaction Amount
    tx_count = len(df_merchant)
    avg_eur_amount = total_volume / tx_count if tx_count > 0 else 0.0

    # 4. Output Results
    results = {
        "merchant": target_merchant,
        "metadata": {
            "merchant_category_code": merchant_metadata.get("merchant_category_code"),
            "account_type": merchant_metadata.get("account_type"),
            "acquirer": merchant_metadata.get("acquirer"),
            "capture_delay": merchant_metadata.get("capture_delay")
        },
        "metrics_2023": {
            "total_volume": total_volume,
            "avg_monthly_volume": avg_monthly_volume,
            "total_fraud_volume": fraud_volume,
            "fraud_rate_ratio": fraud_rate,
            "fraud_rate_percentage": fraud_rate * 100,
            "transaction_count": tx_count,
            "average_eur_amount": avg_eur_amount
        }
    }

    print(json.dumps(results, indent=4))

if __name__ == "__main__":
    execute_step()
