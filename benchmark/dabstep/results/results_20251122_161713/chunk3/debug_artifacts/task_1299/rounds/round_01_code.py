# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1299
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 4385 characters (FULL CODE)
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


import pandas as pd
import json

def calculate_fee(amount, rule):
    """
    Calculates fee based on formula: fee = fixed_amount + (rate * amount / 10000)
    """
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000)

def solve():
    # Load fees
    fees_path = '/output/chunk3/data/context/fees.json'
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    df_fees = pd.DataFrame(fees_data)
    
    # Filter for SwiftCharge and Credit
    # We want rules that APPLY to credit transactions.
    # This includes rules where is_credit is explicitly True, OR rules where is_credit is None (wildcard).
    # We exclude rules where is_credit is explicitly False.
    relevant_fees = df_fees[
        (df_fees['card_scheme'] == 'SwiftCharge') & 
        ((df_fees['is_credit'] == True) | (df_fees['is_credit'].isnull()))
    ]
    
    print(f"Found {len(relevant_fees)} relevant fee rules for SwiftCharge Credit.")
    
    # Transaction Amount
    amount = 1234.0
    
    # Weights from Ground Truth (ACI distribution for SwiftCharge Credit)
    aci_weights = {
        'A': 217,
        'B': 235,
        'C': 424,
        'D': 4761,
        'E': 1466,
        'F': 617,
        'G': 2408
    }
    
    total_txs = sum(aci_weights.values())
    weighted_fee_sum = 0.0
    
    print("\nCalculating fees per ACI:")
    
    # Calculate fee for each ACI type
    for aci, count in aci_weights.items():
        # Find matching rule for this ACI
        # Priority: Specific ACI match > Wildcard ACI match
        
        matched_rule = None
        
        # 1. Try specific match (ACI in list)
        for _, rule in relevant_fees.iterrows():
            rule_acis = rule.get('aci')
            if isinstance(rule_acis, list) and aci in rule_acis:
                matched_rule = rule
                break
        
        # 2. If no specific match, try wildcard (ACI is None)
        if matched_rule is None:
            for _, rule in relevant_fees.iterrows():
                if rule.get('aci') is None:
                    matched_rule = rule
                    break
        
        if matched_rule is not None:
            fee = calculate_fee(amount, matched_rule)
            weighted_fee_sum += fee * count
            # print(f"ACI {aci}: Fee {fee:.4f} (Rule ID {matched_rule['ID']}) * Count {count}")
        else:
            print(f"WARNING: No rule found for ACI {aci}")
            
    average_fee = weighted_fee_sum / total_txs
    
    print(f"\nTotal Transactions: {total_txs}")
    print(f"Weighted Fee Sum: {weighted_fee_sum:.4f}")
    print(f"Average Fee: {average_fee:.14f}")

if __name__ == "__main__":
    solve()
