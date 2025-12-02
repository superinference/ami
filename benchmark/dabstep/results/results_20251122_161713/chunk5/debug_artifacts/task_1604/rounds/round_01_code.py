# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1604
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3205 characters (FULL CODE)
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

# Define file path
fees_path = '/output/chunk5/data/context/fees.json'

# Load the fees data
try:
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    print(f"Successfully loaded {len(fees_data)} fee rules from {fees_path}")
except FileNotFoundError:
    print(f"Error: File not found at {fees_path}")
    fees_data = []

# Filter for entries where card_scheme is 'TransactPlus' 
# and account_type is either empty (wildcard) or contains 'H'
matching_rules = []

for rule in fees_data:
    # Check card_scheme
    if rule.get('card_scheme') == 'TransactPlus':
        # Check account_type
        # Logic: If list is empty (wildcard) OR 'H' is in the list
        account_types = rule.get('account_type', [])
        
        # Handle potential None values if schema varies, though context says it's a list
        if account_types is None: 
            account_types = []
            
        if len(account_types) == 0 or 'H' in account_types:
            matching_rules.append(rule)

# Output the results
print(f"Number of matching rules found: {len(matching_rules)}")

if len(matching_rules) > 0:
    print("\nSample of first 3 matching rules:")
    print(json.dumps(matching_rules[:3], indent=2))
    
    # Verify specific fields for the first match
    first = matching_rules[0]
    print(f"\nVerification - First Match ID: {first.get('ID')}")
    print(f"Card Scheme: {first.get('card_scheme')}")
    print(f"Account Type: {first.get('account_type')}")
else:
    print("No matching rules found.")
