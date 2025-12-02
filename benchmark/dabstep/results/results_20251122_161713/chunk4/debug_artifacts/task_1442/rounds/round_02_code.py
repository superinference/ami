# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1442
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3407 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

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

def main():
    # Load the fees data
    fees_file_path = '/output/chunk4/data/context/fees.json'
    with open(fees_file_path, 'r') as f:
        fees_data = json.load(f)

    # Define the transaction amount
    transaction_amount = 50000.0

    # Variables to track the maximum fee and associated MCCs
    max_fee = -1.0
    expensive_mccs = set()
    wildcard_at_max = False

    # Iterate through each fee rule to find the most expensive one(s)
    for rule in fees_data:
        # Extract rate and fixed_amount
        # Use coerce_to_float to handle any potential string formatting issues
        rate = coerce_to_float(rule.get('rate', 0))
        fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
        
        # Calculate fee based on the formula from manual.md:
        # fee = fixed_amount + rate * transaction_value / 10000
        current_fee = fixed_amount + (rate * transaction_amount / 10000.0)
        
        # Get MCCs for this rule
        mccs = rule.get('merchant_category_code')
        
        # Check if this fee is the new maximum (or equal to it)
        # Use a small epsilon for float comparison
        if current_fee > max_fee + 1e-9:
            max_fee = current_fee
            expensive_mccs = set()
            wildcard_at_max = False
            
            if is_not_empty(mccs):
                expensive_mccs.update(mccs)
            else:
                wildcard_at_max = True
                
        elif abs(current_fee - max_fee) < 1e-9:
            if is_not_empty(mccs):
                expensive_mccs.update(mccs)
            else:
                wildcard_at_max = True

    # Prepare output
    if wildcard_at_max and not expensive_mccs:
        # If only wildcard rules (applying to ALL MCCs) gave the max fee, 
        # then all MCCs are technically the most expensive.
        try:
            mcc_df = pd.read_csv('/output/chunk4/data/context/merchant_category_codes.csv')
            all_mccs = mcc_df['mcc'].unique().tolist()
            print(sorted(all_mccs))
        except Exception as e:
            # Fallback if CSV cannot be read
            print("All MCCs")
    else:
        # Convert set to sorted list
        result_list = sorted(list(expensive_mccs))
        print(result_list)

if __name__ == "__main__":
    main()
