# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1336
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 4807 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# Helper functions for robust data processing
# ═══════════════════════════════════════════════════════════

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

# ═══════════════════════════════════════════════════════════
# Main Analysis Script
# ═══════════════════════════════════════════════════════════

def main():
    # File paths
    mcc_file_path = '/output/chunk6/data/context/merchant_category_codes.csv'
    fees_file_path = '/output/chunk6/data/context/fees.json'

    # 1. Identify the MCC for "Eating Places and Restaurants"
    try:
        df_mcc = pd.read_csv(mcc_file_path)
        target_description = "Eating Places and Restaurants"
        
        # Filter for the description
        matching_rows = df_mcc[df_mcc['description'] == target_description]
        
        if matching_rows.empty:
            print(f"Error: No MCC found for description '{target_description}'")
            return

        # Get the MCC code (assuming integer)
        target_mcc = matching_rows['mcc'].iloc[0]
        print(f"Identified MCC for '{target_description}': {target_mcc}")
        
    except Exception as e:
        print(f"Error reading MCC file: {e}")
        return

    # 2. Load Fees Data
    try:
        with open(fees_file_path, 'r') as f:
            fees_data = json.load(f)
    except Exception as e:
        print(f"Error reading fees file: {e}")
        return

    # 3. Filter Rules and Calculate Fees
    # Criteria:
    # - card_scheme: 'TransactPlus'
    # - account_type: Contains 'H' OR is empty (wildcard)
    # - merchant_category_code: Contains target_mcc OR is empty (wildcard)
    
    target_scheme = 'TransactPlus'
    target_account_type = 'H'
    transaction_value = 4321.0
    
    applicable_fees = []
    
    print(f"Filtering fees for Scheme: {target_scheme}, Account: {target_account_type}, MCC: {target_mcc}")

    for rule in fees_data:
        # Check Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # Check Account Type (Wildcard logic: empty list matches all)
        rule_account_types = rule.get('account_type', [])
        if is_not_empty(rule_account_types):
            if target_account_type not in rule_account_types:
                continue
        
        # Check MCC (Wildcard logic: empty list matches all)
        rule_mccs = rule.get('merchant_category_code', [])
        if is_not_empty(rule_mccs):
            # Ensure type consistency for comparison (int vs int)
            if int(target_mcc) not in [int(x) for x in rule_mccs]:
                continue
        
        # If we reached here, the rule applies
        # Calculate Fee: fixed_amount + (rate * amount / 10000)
        try:
            fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
            rate = coerce_to_float(rule.get('rate', 0))
            
            fee = fixed_amount + (rate * transaction_value / 10000)
            applicable_fees.append(fee)
            
        except Exception as e:
            print(f"Error calculating fee for rule ID {rule.get('ID')}: {e}")
            continue

    # 4. Compute Average and Output
    if not applicable_fees:
        print("No matching fee rules found.")
    else:
        average_fee = sum(applicable_fees) / len(applicable_fees)
        print(f"Found {len(applicable_fees)} matching rules.")
        print(f"{average_fee:.6f}")

if __name__ == "__main__":
    main()
