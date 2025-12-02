# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1643
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3857 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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

def load_fees_data():
    """
    Loads the fees.json file to access fee rules and parameters.
    """
    file_path = '/output/chunk6/data/context/fees.json'
    try:
        with open(file_path, 'r') as f:
            fees_data = json.load(f)
        return fees_data
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return []
    except Exception as e:
        print(f"An error occurred while loading the file: {e}")
        return []

def calculate_fee(transaction_value, rule):
    """
    Calculates the fee for a given transaction value based on a fee rule.
    Formula: fee = fixed_amount + rate * transaction_value / 10000
    """
    fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    
    fee = fixed_amount + (rate * transaction_value / 10000.0)
    return fee

def main():
    # 1. Load Data
    fees = load_fees_data()
    if not fees:
        return

    # 2. Define Parameters
    target_scheme = 'SwiftCharge'
    target_account_type = 'F'
    transaction_value = 1234.0

    # 3. Filter Rules and Calculate Fees
    applicable_fees = []
    
    print(f"Analyzing {len(fees)} fee rules for Scheme: {target_scheme}, Account Type: {target_account_type}...")

    for rule in fees:
        # Check Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # Check Account Type
        # Rule applies if account_type list is empty (Wildcard) OR contains the target type
        rule_account_types = rule.get('account_type', [])
        
        # Handle None as empty list just in case
        if rule_account_types is None:
            rule_account_types = []
            
        if len(rule_account_types) == 0 or target_account_type in rule_account_types:
            # Calculate fee for this rule
            fee = calculate_fee(transaction_value, rule)
            applicable_fees.append(fee)

    # 4. Compute Average
    if not applicable_fees:
        print("No applicable fee rules found.")
    else:
        average_fee = sum(applicable_fees) / len(applicable_fees)
        print(f"Found {len(applicable_fees)} applicable rules.")
        print(f"Calculated fees sample: {applicable_fees[:5]}...")
        
        # 5. Output Result
        # Question asks for answer in EUR and 6 decimals
        print(f"{average_fee:.6f}")

if __name__ == "__main__":
    main()
