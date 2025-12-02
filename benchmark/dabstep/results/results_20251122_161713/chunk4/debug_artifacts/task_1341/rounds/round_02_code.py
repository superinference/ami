# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1341
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 4016 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
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
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def is_wildcard(field_value):
    """Check if a field in the fee rule is a wildcard (None or empty list)."""
    if field_value is None:
        return True
    if isinstance(field_value, list) and len(field_value) == 0:
        return True
    return False

def calculate_fee(amount, rule):
    """Calculate fee based on fixed amount and rate."""
    fixed = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    # Formula: fee = fixed + (rate * amount / 10000)
    return fixed + (rate * amount / 10000)

# ═══════════════════════════════════════════════════════════
# Main Analysis Logic
# ═══════════════════════════════════════════════════════════

def main():
    # Paths
    mcc_path = '/output/chunk4/data/context/merchant_category_codes.csv'
    fees_path = '/output/chunk4/data/context/fees.json'

    # 1. Get MCC for the specific description
    try:
        df_mcc = pd.read_csv(mcc_path)
        target_desc = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"
        
        # Filter for exact match
        match = df_mcc[df_mcc['description'] == target_desc]
        
        if match.empty:
            print(f"Error: Description '{target_desc}' not found in MCC file.")
            return
        
        target_mcc = int(match.iloc[0]['mcc'])
        # print(f"DEBUG: Found MCC {target_mcc} for description.")
        
    except Exception as e:
        print(f"Error reading MCC file: {e}")
        return

    # 2. Load Fees and Filter
    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
            
        matching_fees = []
        transaction_amount = 50.0
        
        for rule in fees_data:
            # Filter 1: Card Scheme
            if rule.get('card_scheme') != 'GlobalCard':
                continue
                
            # Filter 2: Account Type (Match 'H' or Wildcard)
            r_account_types = rule.get('account_type')
            if not is_wildcard(r_account_types):
                if 'H' not in r_account_types:
                    continue
            
            # Filter 3: MCC (Match target_mcc or Wildcard)
            r_mccs = rule.get('merchant_category_code')
            if not is_wildcard(r_mccs):
                # Ensure MCCs in json are compared correctly (integers)
                if target_mcc not in [int(x) for x in r_mccs]:
                    continue
            
            # If we reach here, the rule applies
            fee = calculate_fee(transaction_amount, rule)
            matching_fees.append(fee)
            
        # 3. Calculate Average
        if not matching_fees:
            print("No matching fee rules found.")
        else:
            average_fee = sum(matching_fees) / len(matching_fees)
            print(f"{average_fee:.6f}")
            
    except Exception as e:
        print(f"Error processing fees: {e}")

if __name__ == "__main__":
    main()
