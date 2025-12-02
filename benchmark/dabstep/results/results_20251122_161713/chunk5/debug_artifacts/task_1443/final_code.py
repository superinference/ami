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
import numpy as np

def solve():
    # Load the fees data
    fees_path = '/output/chunk5/data/context/fees.json'
    df_fees = pd.read_json(fees_path)

    # Filter for GlobalCard
    # We are looking for rules applicable to 'GlobalCard'
    df_global = df_fees[df_fees['card_scheme'] == 'GlobalCard'].copy()

    # Filter for Credit transactions
    # is_credit can be True, False, or None (wildcard)
    # We want rules that apply to is_credit=True, so we exclude is_credit=False
    # Note: In Python/Pandas, None != False is True, so this preserves None (wildcards) and True.
    df_global_credit = df_global[df_global['is_credit'] != False].copy()

    # Calculate the fee for a 1 Euro transaction for each rule
    # Fee = fixed_amount + (rate / 10000 * amount)
    transaction_amount = 1.0
    df_global_credit['calculated_fee'] = df_global_credit['fixed_amount'] + (df_global_credit['rate'] / 10000 * transaction_amount)

    # Define all possible ACIs based on the manual/data
    all_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    
    # Dictionary to store the maximum fee found for each ACI
    aci_max_fees = {aci: 0.0 for aci in all_acis}

    # Iterate through the applicable rules to find the max fee for each ACI
    for _, row in df_global_credit.iterrows():
        rule_acis = row['aci']
        fee = row['calculated_fee']
        
        # Determine which ACIs this rule applies to
        # If rule_acis is None, empty list, or NaN, it applies to ALL ACIs (Wildcard)
        applicable_acis = []
        if rule_acis is None:
            applicable_acis = all_acis
        elif isinstance(rule_acis, list):
            if len(rule_acis) == 0:
                applicable_acis = all_acis
            else:
                applicable_acis = rule_acis
        elif pd.isna(rule_acis):
             applicable_acis = all_acis
        else:
            # Fallback if it's a single string (though schema says array)
            applicable_acis = [rule_acis]
            
        # Update the max fee for the applicable ACIs
        # We are looking for the "most expensive" scenario for each ACI
        for aci in applicable_acis:
            if aci in aci_max_fees:
                if fee > aci_max_fees[aci]:
                    aci_max_fees[aci] = fee

    # Find the maximum fee value across all ACIs
    if not aci_max_fees:
        print([])
        return

    max_fee = max(aci_max_fees.values())

    # Identify which ACIs have this maximum fee
    most_expensive = [aci for aci, fee in aci_max_fees.items() if fee == max_fee]

    # Sort alphabetically to handle ties (lowest alphabetical order)
    most_expensive.sort()

    # Return the first one as a list
    print([most_expensive[0]])

if __name__ == "__main__":
    solve()