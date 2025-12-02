# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1394
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 4367 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

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

def solve():
    # 1. Identify the MCC for "Fast Food Restaurants"
    mcc_file_path = '/output/chunk6/data/context/merchant_category_codes.csv'
    try:
        df_mcc = pd.read_csv(mcc_file_path)
        target_description = "Fast Food Restaurants"
        
        # Filter for the description
        # Using strip() to handle potential whitespace issues
        mcc_row = df_mcc[df_mcc['description'].str.strip() == target_description]
        
        if mcc_row.empty:
            # Fallback: Case-insensitive search if exact match fails
            mcc_row = df_mcc[df_mcc['description'].str.strip().str.lower() == target_description.lower()]
            
        if mcc_row.empty:
            print(f"Error: Could not find MCC for description '{target_description}'")
            return

        target_mcc = int(mcc_row.iloc[0]['mcc'])
        # print(f"DEBUG: Found MCC {target_mcc} for '{target_description}'")
        
    except Exception as e:
        print(f"Error processing MCC file: {e}")
        return

    # 2. Load Fee Rules
    fees_file_path = '/output/chunk6/data/context/fees.json'
    try:
        with open(fees_file_path, 'r') as f:
            fees_data = json.load(f)
    except Exception as e:
        print(f"Error loading fees.json: {e}")
        return

    # 3. Filter Rules and Calculate Fees
    # Criteria:
    # - Card Scheme: NexPay
    # - Account Type: H (or wildcard)
    # - MCC: target_mcc (or wildcard)
    # - Transaction Value: 1234 EUR
    
    target_scheme = 'NexPay'
    target_account_type = 'H'
    transaction_value = 1234.0
    
    matching_fees = []

    for rule in fees_data:
        # Check Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # Check Account Type
        # Logic: If list is empty/null, it applies to ALL (Wildcard). 
        # If list is not empty, 'H' must be present.
        rule_account_types = rule.get('account_type')
        if is_not_empty(rule_account_types):
            if target_account_type not in rule_account_types:
                continue
        
        # Check Merchant Category Code
        # Logic: If list is empty/null, it applies to ALL (Wildcard).
        # If list is not empty, target_mcc must be present.
        rule_mccs = rule.get('merchant_category_code')
        if is_not_empty(rule_mccs):
            if target_mcc not in rule_mccs:
                continue
        
        # If we reach here, the rule applies. Calculate the fee.
        # Formula: fee = fixed_amount + (rate * transaction_value / 10000)
        fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        calculated_fee = fixed_amount + (rate * transaction_value / 10000)
        matching_fees.append(calculated_fee)

    # 4. Calculate Average and Output
    if not matching_fees:
        print("No matching fee rules found.")
    else:
        average_fee = sum(matching_fees) / len(matching_fees)
        # Output formatted to 6 decimals as requested
        print(f"{average_fee:.6f}")

if __name__ == "__main__":
    solve()
