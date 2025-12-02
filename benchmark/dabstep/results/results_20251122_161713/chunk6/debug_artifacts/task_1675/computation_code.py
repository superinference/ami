import json
import pandas as pd
import numpy as np

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

# Main Analysis Script
def main():
    # Load the fees data
    file_path = '/output/chunk6/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)

    # Parameters from the question
    target_scheme = 'SwiftCharge'
    target_account_type = 'S'
    transaction_value = 1234.0

    matching_fees = []

    # Iterate through all fee rules
    for rule in fees_data:
        # 1. Filter by Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue

        # 2. Filter by Account Type
        # The rule applies if 'S' is in the list OR if the list is empty (wildcard)
        account_types = rule.get('account_type')
        
        is_applicable = False
        if account_types is None:
            # Treat None as wildcard (though schema suggests list)
            is_applicable = True
        elif isinstance(account_types, list):
            if len(account_types) == 0:
                # Empty list is a wildcard matching ALL types
                is_applicable = True
            elif target_account_type in account_types:
                # Explicit match found
                is_applicable = True
        
        if is_applicable:
            # 3. Calculate Fee
            # Formula: fee = fixed_amount + (rate * transaction_value / 10000)
            fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
            rate = coerce_to_float(rule.get('rate', 0))
            
            fee = fixed_amount + (rate * transaction_value / 10000.0)
            matching_fees.append(fee)

    # 4. Calculate Average
    if matching_fees:
        average_fee = sum(matching_fees) / len(matching_fees)
        # Print result formatted to 6 decimals as requested
        print(f"{average_fee:.6f}")
    else:
        print("No matching fee rules found.")

if __name__ == "__main__":
    main()