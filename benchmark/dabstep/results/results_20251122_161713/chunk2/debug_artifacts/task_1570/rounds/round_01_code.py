# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1570
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3486 characters (FULL CODE)
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

def execute_step():
    # File path
    fees_path = '/output/chunk2/data/context/fees.json'
    
    # Load fees.json
    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        print(f"Loaded {len(fees_data)} rules from fees.json")
    except FileNotFoundError:
        print(f"Error: File not found at {fees_path}")
        return

    # Filter rules
    # Criteria: 
    # 1. card_scheme == 'NexPay'
    # 2. account_type list contains 'D' OR account_type list is empty (wildcard)
    
    filtered_rules = []
    for rule in fees_data:
        # Check card scheme
        if rule.get('card_scheme') == 'NexPay':
            # Check account type
            acct_types = rule.get('account_type', [])
            
            # If list is empty (wildcard) or contains 'D'
            if not acct_types or 'D' in acct_types:
                filtered_rules.append(rule)

    # Output results
    print(f"Rules matching NexPay and Account Type D (or wildcard): {len(filtered_rules)}")
    
    # Convert to DataFrame for clear visualization of the filtered data
    if filtered_rules:
        df_filtered = pd.DataFrame(filtered_rules)
        # Display key columns to verify filtering
        print("\nFirst 5 filtered rules:")
        print(df_filtered[['ID', 'card_scheme', 'account_type', 'fixed_amount', 'rate']].head().to_string())
        
        # Verify the filtering logic with a breakdown
        wildcard_count = df_filtered['account_type'].apply(lambda x: len(x) == 0).sum()
        specific_count = len(df_filtered) - wildcard_count
        print(f"\nBreakdown: {wildcard_count} wildcard rules (empty list), {specific_count} specific rules (containing 'D')")
    else:
        print("No matching rules found.")

if __name__ == "__main__":
    execute_step()
