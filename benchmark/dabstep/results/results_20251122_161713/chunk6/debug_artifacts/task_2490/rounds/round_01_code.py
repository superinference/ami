# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2490
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3759 characters (FULL CODE)
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

def load_and_verify_data():
    # Define file paths
    payments_path = '/output/chunk6/data/context/payments.csv'
    fees_path = '/output/chunk6/data/context/fees.json'
    merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

    # 1. Load payments.csv
    print(f"Loading {payments_path}...")
    df_payments = pd.read_csv(payments_path)
    print(f"Payments loaded. Shape: {df_payments.shape}")
    print(f"Columns: {list(df_payments.columns)}")

    # 2. Load fees.json
    print(f"\nLoading {fees_path}...")
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    df_fees = pd.DataFrame(fees_data)
    print(f"Fees loaded. Shape: {df_fees.shape}")
    
    # 3. Load merchant_data.json
    print(f"\nLoading {merchant_data_path}...")
    with open(merchant_data_path, 'r') as f:
        merchant_list = json.load(f)
    df_merchants = pd.DataFrame(merchant_list)
    print(f"Merchant data loaded. Shape: {df_merchants.shape}")

    # 4. Verify data relevant to the specific goal
    # Goal: "In the year 2023 what delta would Crossfit_Hanna pay if the relative fee of the fee with ID=792 changed to 99?"
    
    target_merchant = 'Crossfit_Hanna'
    target_fee_id = 792

    # Check if merchant exists in payments
    merchant_txs = df_payments[df_payments['merchant'] == target_merchant]
    print(f"\nVerification for merchant '{target_merchant}':")
    print(f"Found {len(merchant_txs)} transactions.")
    
    # Check if merchant exists in merchant_data
    merchant_info = df_merchants[df_merchants['merchant'] == target_merchant]
    if not merchant_info.empty:
        print(f"Merchant metadata found: {merchant_info.iloc[0].to_dict()}")
    else:
        print("Merchant metadata NOT found.")

    # Check if Fee ID exists
    fee_info = df_fees[df_fees['ID'] == target_fee_id]
    if not fee_info.empty:
        print(f"\nVerification for Fee ID {target_fee_id}:")
        print(fee_info.iloc[0].to_dict())
    else:
        print(f"\nFee ID {target_fee_id} NOT found.")

if __name__ == "__main__":
    load_and_verify_data()
