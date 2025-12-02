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
    # Formula: fixed + (rate * amount / 10000)
    return fixed + (rate * amount / 10000)

def main():
    fees_file_path = '/output/chunk5/data/context/fees.json'
    
    try:
        with open(fees_file_path, 'r') as f:
            fees_data = json.load(f)
    except Exception as e:
        print(f"Error loading fees file: {e}")
        return

    # Parameters from the question
    target_scheme = 'TransactPlus'
    target_account_type = 'D'
    transaction_amount = 4321.0
    
    matching_fees = []
    
    # Iterate through all fee rules to find matches
    for rule in fees_data:
        # 1. Filter by Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # 2. Filter by Account Type
        # Logic: If account_type is empty/null (wildcard), it matches ALL types (including D).
        #        If it has values, 'D' must be in the list to match.
        rule_account_types = rule.get('account_type')
        
        is_match = False
        if not rule_account_types: # Handles None and [] (Wildcard)
            is_match = True
        elif target_account_type in rule_account_types: # Explicit match
            is_match = True
            
        if is_match:
            # Calculate fee for this specific rule
            fee = calculate_fee(transaction_amount, rule)
            matching_fees.append(fee)
            
    # Calculate Average
    if matching_fees:
        avg_fee = sum(matching_fees) / len(matching_fees)
        # Output formatted to 6 decimals as requested
        print(f"{avg_fee:.6f}")
    else:
        print("No matching rules found")

if __name__ == "__main__":
    main()