# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1438
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3512 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import numpy as np
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

def process_mcc_column(mcc_list):
    """
    Handle None or empty lists in MCC column by replacing with ['ALL'].
    This ensures wildcard rules are preserved during explosion.
    """
    if mcc_list is None:
        return ['ALL']
    if isinstance(mcc_list, list):
        if len(mcc_list) == 0:
            return ['ALL']
        return mcc_list
    # Handle single values if they exist (though schema says list)
    return [mcc_list]

def main():
    # Define file path
    fees_path = '/output/chunk5/data/context/fees.json'
    
    # Load the fees data
    try:
        df_fees = pd.read_json(fees_path)
    except ValueError:
        with open(fees_path, 'r') as f:
            data = json.load(f)
        df_fees = pd.DataFrame(data)

    # Preprocess the merchant_category_code column
    # Convert None/Empty to ['ALL'] to handle wildcards
    df_fees['merchant_category_code'] = df_fees['merchant_category_code'].apply(process_mcc_column)

    # Explode the merchant_category_code column
    # This creates a separate row for each MCC in the list, inheriting the fee rule
    df_fees_exploded = df_fees.explode('merchant_category_code')

    # Define the transaction amount
    transaction_amount = 500.0

    # Ensure numeric types for calculation
    df_fees_exploded['fixed_amount'] = df_fees_exploded['fixed_amount'].apply(coerce_to_float)
    df_fees_exploded['rate'] = df_fees_exploded['rate'].apply(coerce_to_float)

    # Calculate the fee for each rule
    # Formula: fee = fixed_amount + (rate * transaction_value / 10000)
    df_fees_exploded['calculated_fee'] = (
        df_fees_exploded['fixed_amount'] + 
        (df_fees_exploded['rate'] * transaction_amount / 10000)
    )

    # Find the maximum fee value across all rules
    max_fee = df_fees_exploded['calculated_fee'].max()

    # Filter for rows that result in this maximum fee
    most_expensive_rules = df_fees_exploded[df_fees_exploded['calculated_fee'] == max_fee]

    # Extract the unique MCCs associated with the most expensive rules
    expensive_mccs = most_expensive_rules['merchant_category_code'].unique()

    # Prepare the result list
    # Filter out 'ALL' if specific MCCs are present, unless 'ALL' is the only result
    result_list = [x for x in expensive_mccs if x != 'ALL']
    
    if not result_list and 'ALL' in expensive_mccs:
        # If the only expensive rule is a wildcard, return ALL
        final_output = ['ALL']
    else:
        # Sort and ensure integers for clean output
        final_output = sorted([int(x) if isinstance(x, (int, float)) else x for x in result_list])

    # Output the list as requested
    print(final_output)

if __name__ == "__main__":
    main()
