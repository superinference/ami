# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1294
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3351 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
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
    return float(value) if value is not None else 0.0

def safe_get(df, column, default=None):
    """Safely get column from DataFrame, return default if not exists."""
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

# Main Analysis Script
def calculate_average_nexpay_fee():
    # Define file path
    fees_file_path = '/output/chunk4/data/context/fees.json'

    # Load the fees data
    try:
        with open(fees_file_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {fees_file_path}")
        return

    # Convert to DataFrame
    df_fees = pd.DataFrame(fees_data)

    # Define transaction parameters
    transaction_value = 5000.0
    target_scheme = 'NexPay'

    # Filter logic:
    # 1. Card Scheme must be 'NexPay'
    # 2. Rule must apply to Credit transactions.
    #    - is_credit == True (Explicitly Credit)
    #    - is_credit is Null/NaN (Applies to ALL types, including Credit)
    #    - We exclude is_credit == False (Explicitly Debit)
    
    # Create mask for scheme
    scheme_mask = df_fees['card_scheme'] == target_scheme
    
    # Create mask for credit applicability
    # Note: We handle NaN/None values which imply the rule applies to all transaction types
    credit_mask = (df_fees['is_credit'] == True) | (df_fees['is_credit'].isna())
    
    # Apply filters
    relevant_rules = df_fees[scheme_mask & credit_mask].copy()

    # Calculate fee for each rule
    # Formula: fee = fixed_amount + (rate * transaction_value / 10000)
    # We use apply to handle potential data type issues row-by-row, though vectorization is possible
    def compute_fee(row):
        fixed = coerce_to_float(row['fixed_amount'])
        rate = coerce_to_float(row['rate'])
        return fixed + (rate * transaction_value / 10000)

    relevant_rules['calculated_fee'] = relevant_rules.apply(compute_fee, axis=1)

    # Calculate the average fee
    if not relevant_rules.empty:
        average_fee = relevant_rules['calculated_fee'].mean()
        
        # Output results
        # print(f"Found {len(relevant_rules)} applicable rules for {target_scheme} Credit transactions.")
        # print(f"Average Fee: {average_fee}")
        
        # Final answer format (high precision)
        print(f"{average_fee:.14f}")
    else:
        print("No applicable rules found.")

if __name__ == "__main__":
    calculate_average_nexpay_fee()
