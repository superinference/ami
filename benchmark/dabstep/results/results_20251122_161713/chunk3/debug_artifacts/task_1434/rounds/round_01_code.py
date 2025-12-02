# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1434
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 3835 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default


import json
import pandas as pd

def calculate_fee(amount, rule):
    """
    Calculates the fee for a given amount based on the rule.
    Formula: fee = fixed_amount + rate * transaction_value / 10000
    """
    fixed = rule.get('fixed_amount')
    rate = rule.get('rate')
    
    # Handle None values (treat as 0 if missing, though schema implies they exist)
    if fixed is None: fixed = 0.0
    if rate is None: rate = 0.0
    
    return fixed + (rate * amount / 10000)

def get_most_expensive_mccs():
    fees_path = '/output/chunk3/data/context/fees.json'
    
    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {fees_path}")
        return

    transaction_amount = 5.0
    mcc_max_fees = {}

    # Iterate through all fee rules
    for rule in fees_data:
        # Get the list of MCCs this rule applies to
        mccs = rule.get('merchant_category_code')
        
        # Skip rules that are wildcards (empty list or None) for MCC
        # We are looking for specific MCCs that are the most expensive
        if not mccs:
            continue
            
        # Calculate the fee for a 5 euro transaction
        fee = calculate_fee(transaction_amount, rule)
        
        # Update the maximum fee seen for each MCC in the list
        for mcc in mccs:
            if mcc not in mcc_max_fees:
                mcc_max_fees[mcc] = fee
            else:
                # Keep the highest fee found for this MCC across all rules
                if fee > mcc_max_fees[mcc]:
                    mcc_max_fees[mcc] = fee

    if not mcc_max_fees:
        print([])
        return

    # Find the global maximum fee value among all MCCs
    max_fee = max(mcc_max_fees.values())
    
    # Identify all MCCs that have this maximum fee
    # Using a small epsilon for float comparison safety
    expensive_mccs = [mcc for mcc, fee in mcc_max_fees.items() if abs(fee - max_fee) < 1e-9]
    
    # Sort the list for consistent output
    expensive_mccs.sort()
    
    # Output the list
    print(expensive_mccs)

if __name__ == "__main__":
    get_most_expensive_mccs()
