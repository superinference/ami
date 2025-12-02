# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1538
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3034 characters (FULL CODE)
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

# Main analysis script
def calculate_average_fee():
    # Load fees data
    fees_file_path = '/output/chunk5/data/context/fees.json'
    with open(fees_file_path, 'r') as f:
        fees_data = json.load(f)

    # Parameters from the question
    target_scheme = 'NexPay'
    target_account_type = 'R'
    transaction_amount = 1000.0

    applicable_fees = []

    # Iterate through all fee rules
    for rule in fees_data:
        # Check Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue

        # Check Account Type
        # Rule applies if account_type list is empty (wildcard) OR contains the target type
        rule_account_types = rule.get('account_type')
        if rule_account_types and target_account_type not in rule_account_types:
            continue
        
        # If we are here, the rule applies
        
        # Extract fee components
        fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        # Calculate fee for this rule
        # Formula: fee = fixed_amount + (rate * amount / 10000)
        # Note: rate is typically in basis points or similar, divided by 10000 as per manual
        fee = fixed_amount + (rate * transaction_amount / 10000.0)
        
        applicable_fees.append(fee)

    # Calculate average
    if not applicable_fees:
        print("No applicable fee rules found.")
        return

    average_fee = sum(applicable_fees) / len(applicable_fees)

    # Output result formatted to 6 decimals
    print(f"{average_fee:.6f}")

if __name__ == "__main__":
    calculate_average_fee()
