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
    """Calculates fee based on manual formula: fixed + rate * amount / 10000"""
    fixed = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000)

# File path
fees_path = '/output/chunk4/data/context/fees.json'

try:
    # Load the JSON file
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    transaction_amount = 100.0
    rule_fees = []

    # Calculate fee for each rule
    for rule in fees_data:
        fee = calculate_fee(transaction_amount, rule)
        mccs = rule.get('merchant_category_code')
        
        rule_fees.append({
            'fee': fee,
            'mccs': mccs,
            'id': rule.get('ID')
        })

    # Find the maximum fee
    if not rule_fees:
        print("No fee rules found.")
    else:
        max_fee = max(item['fee'] for item in rule_fees)
        
        # Filter rules that result in the maximum fee
        # Using a small epsilon for float comparison safety
        expensive_rules = [item for item in rule_fees if abs(item['fee'] - max_fee) < 1e-9]
        
        expensive_mccs = set()
        wildcard_present = False

        for item in expensive_rules:
            mccs = item['mccs']
            
            # Check for wildcard (None or empty list)
            if mccs is None or (isinstance(mccs, list) and len(mccs) == 0):
                wildcard_present = True
            elif isinstance(mccs, list):
                for mcc in mccs:
                    expensive_mccs.add(int(mcc))
            elif isinstance(mccs, (int, str)):
                expensive_mccs.add(int(mccs))

        # Prepare result list
        result_list = sorted(list(expensive_mccs))
        
        # Output the result
        # If a wildcard rule was the most expensive, technically all MCCs are expensive, 
        # but usually specific high-risk codes are listed explicitly in high-fee rules.
        # We prioritize the explicit list if available.
        print(result_list)

except Exception as e:
    print(f"An error occurred: {e}")