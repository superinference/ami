# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1577
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3071 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
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

def calculate_fee(amount, rule):
    """Calculates fee based on fixed_amount and rate."""
    fixed = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    return fixed + (rate * amount / 10000.0)

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------
def main():
    fees_path = '/output/chunk4/data/context/fees.json'
    
    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
    except Exception as e:
        print(f"Error loading fees.json: {e}")
        return

    target_scheme = 'GlobalCard'
    target_account_type = 'D'
    transaction_amount = 1234.0
    
    applicable_fees = []
    
    print(f"Analyzing {len(fees_data)} fee rules...")
    
    for rule in fees_data:
        # 1. Filter by Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # 2. Filter by Account Type
        # Logic: Rule applies if account_type is None/Empty (Wildcard) OR contains 'D'
        rule_account_types = rule.get('account_type')
        
        is_applicable_account = False
        if not rule_account_types: # Checks for None or empty list []
            is_applicable_account = True
        elif isinstance(rule_account_types, list) and target_account_type in rule_account_types:
            is_applicable_account = True
            
        if is_applicable_account:
            # 3. Calculate Fee
            fee = calculate_fee(transaction_amount, rule)
            applicable_fees.append(fee)

    # 4. Calculate Average
    if applicable_fees:
        average_fee = sum(applicable_fees) / len(applicable_fees)
        print(f"Found {len(applicable_fees)} applicable rules for {target_scheme} and Account Type {target_account_type}.")
        print(f"{average_fee:.6f}")
    else:
        print("No applicable fee rules found.")

if __name__ == "__main__":
    main()
