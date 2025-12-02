# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2496
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3519 characters (FULL CODE)
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
    payments_path = '/output/chunk2/data/context/payments.csv'
    fees_path = '/output/chunk2/data/context/fees.json'
    merchant_data_path = '/output/chunk2/data/context/merchant_data.json'

    # 1. Load payments.csv
    try:
        df_payments = pd.read_csv(payments_path)
        print(f"Loaded payments.csv with {len(df_payments)} rows.")
    except Exception as e:
        print(f"Error loading payments.csv: {e}")
        return

    # 2. Load fees.json
    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        print(f"Loaded fees.json with {len(fees_data)} rules.")
    except Exception as e:
        print(f"Error loading fees.json: {e}")
        return

    # 3. Load merchant_data.json
    try:
        with open(merchant_data_path, 'r') as f:
            merchant_data = json.load(f)
        print(f"Loaded merchant_data.json with {len(merchant_data)} merchants.")
    except Exception as e:
        print(f"Error loading merchant_data.json: {e}")
        return

    # 4. Filter payments for 'Golfclub_Baron_Friso' in 2023
    target_merchant = 'Golfclub_Baron_Friso'
    df_filtered = df_payments[
        (df_payments['merchant'] == target_merchant) & 
        (df_payments['year'] == 2023)
    ]
    print(f"Filtered payments for '{target_merchant}' in 2023: {len(df_filtered)} transactions.")

    # 5. Locate fee rule with ID=709
    target_fee_id = 709
    fee_rule_709 = next((rule for rule in fees_data if rule['ID'] == target_fee_id), None)

    if fee_rule_709:
        print(f"Found Fee Rule ID={target_fee_id}:")
        print(json.dumps(fee_rule_709, indent=2))
        print(f"Original Rate: {fee_rule_709.get('rate')}")
    else:
        print(f"Fee Rule ID={target_fee_id} not found.")

if __name__ == "__main__":
    execute_step()
