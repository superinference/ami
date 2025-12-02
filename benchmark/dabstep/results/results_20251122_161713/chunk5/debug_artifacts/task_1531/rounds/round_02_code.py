# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1531
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3027 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# ═══════════════════════════════════════════════════════════
# Helper functions for robust data processing
# ═══════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════
# Main Analysis Script
# ═══════════════════════════════════════════════════════════

# 1. Load Data
fees_file_path = '/output/chunk5/data/context/fees.json'
with open(fees_file_path, 'r') as f:
    fees_data = json.load(f)

# 2. Define Parameters
target_scheme = 'SwiftCharge'
target_account_type = 'R'
transaction_value = 100.0

# 3. Filter Rules and Calculate Fees
calculated_fees = []

for rule in fees_data:
    # Check Card Scheme
    if rule.get('card_scheme') != target_scheme:
        continue
        
    # Check Account Type (Wildcard logic: empty list or None means ALL types)
    rule_account_types = rule.get('account_type')
    is_match = False
    
    if not rule_account_types: # Handles None and []
        is_match = True
    elif target_account_type in rule_account_types:
        is_match = True
        
    if is_match:
        # Calculate Fee
        # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
        fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        fee = fixed_amount + (rate * transaction_value / 10000.0)
        calculated_fees.append(fee)

# 4. Compute Average
if not calculated_fees:
    print("No matching fee rules found.")
else:
    average_fee = sum(calculated_fees) / len(calculated_fees)
    
    # 5. Output Result
    # Question asks for answer in EUR and 6 decimals
    print(f"{average_fee:.6f}")
