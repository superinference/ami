# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1401
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3031 characters (FULL CODE)
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

def calculate_fee(amount, rule):
    """Calculate fee based on fixed amount and rate."""
    fixed = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    return fixed + (rate * amount / 10000)

def main():
    # 1. Get MCC for "Taxicabs and Limousines"
    mcc_path = '/output/chunk6/data/context/merchant_category_codes.csv'
    df_mcc = pd.read_csv(mcc_path)
    
    target_description = "Taxicabs and Limousines"
    mcc_row = df_mcc[df_mcc['description'] == target_description]
    
    if mcc_row.empty:
        print(f"Error: Description '{target_description}' not found in MCC file.")
        return

    target_mcc = int(mcc_row.iloc[0]['mcc'])
    
    # 2. Load Fees
    fees_path = '/output/chunk6/data/context/fees.json'
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)

    # 3. Filter Rules and Calculate Fees
    target_scheme = 'GlobalCard'
    target_account = 'H'
    transaction_amount = 10.0
    
    matching_fees = []
    
    for rule in fees_data:
        # Filter by Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # Filter by Account Type (Wildcard check)
        # If list is empty or None, it applies to all. If not empty, must contain target.
        rule_accounts = rule.get('account_type')
        if rule_accounts and target_account not in rule_accounts:
            continue
            
        # Filter by MCC (Wildcard check)
        # If list is empty or None, it applies to all. If not empty, must contain target.
        rule_mccs = rule.get('merchant_category_code')
        if rule_mccs and target_mcc not in rule_mccs:
            continue
            
        # If we passed filters, calculate fee
        fee = calculate_fee(transaction_amount, rule)
        matching_fees.append(fee)

    # 4. Calculate Average
    if not matching_fees:
        print("No matching rules found")
    else:
        average_fee = sum(matching_fees) / len(matching_fees)
        # Output formatted to 6 decimals as requested
        print(f"{average_fee:.6f}")

if __name__ == "__main__":
    main()
