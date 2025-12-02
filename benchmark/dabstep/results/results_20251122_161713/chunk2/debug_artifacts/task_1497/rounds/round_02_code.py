# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1497
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 2443 characters (FULL CODE)
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

# Load fees data
fees_path = '/output/chunk2/data/context/fees.json'
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Target criteria
target_account_type = 'S'
target_aci = 'G'

matching_ids = []

# Iterate through fee rules to find matches
for rule in fees_data:
    # Check account_type: Match if wildcard (empty/None) OR if target is in list
    # Note: In Python, empty list [] evaluates to False, None evaluates to False
    account_type_match = (not rule.get('account_type')) or (target_account_type in rule.get('account_type', []))
    
    # Check aci: Match if wildcard (empty/None) OR if target is in list
    aci_match = (not rule.get('aci')) or (target_aci in rule.get('aci', []))
    
    if account_type_match and aci_match:
        matching_ids.append(rule['ID'])

# Sort IDs for consistent output
matching_ids.sort()

# Print result
# The question asks "What is the fee ID or IDs", so we print the list/values found.
if matching_ids:
    print(', '.join(map(str, matching_ids)))
else:
    print("No matching fee IDs found.")
