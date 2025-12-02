# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1570
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 2791 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import json
import pandas as pd
import numpy as np

# Helper functions for robust data processing
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
    return 0.0

def execute_step():
    # File path
    fees_path = '/output/chunk2/data/context/fees.json'
    
    # Load fees.json
    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {fees_path}")
        return

    # Parameters
    target_scheme = 'NexPay'
    target_account_type = 'D'
    transaction_value = 1000.0
    
    calculated_fees = []
    
    # Iterate through rules to find matches and calculate fees
    for rule in fees_data:
        # 1. Check Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # 2. Check Account Type
        # Logic: Match if list contains target OR list is empty/None (wildcard)
        account_types = rule.get('account_type')
        is_match = False
        
        if account_types is None:
            is_match = True # Wildcard (null in JSON)
        elif isinstance(account_types, list):
            if len(account_types) == 0:
                is_match = True # Wildcard (empty list)
            elif target_account_type in account_types:
                is_match = True # Explicit match
        
        if is_match:
            # 3. Calculate Fee
            # Formula: fee = fixed_amount + rate * transaction_value / 10000
            fixed_amount = coerce_to_float(rule.get('fixed_amount'))
            rate = coerce_to_float(rule.get('rate'))
            
            fee = fixed_amount + (rate * transaction_value / 10000.0)
            calculated_fees.append(fee)

    # Output results
    if calculated_fees:
        average_fee = sum(calculated_fees) / len(calculated_fees)
        # Print strictly the formatted number as requested
        print(f"{average_fee:.6f}")
    else:
        print("No matching rules found")

if __name__ == "__main__":
    execute_step()
