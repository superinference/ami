# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1508
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 2915 characters (FULL CODE)
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

# ═══════════════════════════════════════════════════════════
# Main Analysis Script
# ═══════════════════════════════════════════════════════════

def analyze_fees():
    # Load the fees data
    fees_path = '/output/chunk3/data/context/fees.json'
    try:
        df_fees = pd.read_json(fees_path)
    except ValueError:
        # Fallback if JSON structure is complex, though read_json usually handles list of dicts
        with open(fees_path, 'r') as f:
            data = json.load(f)
        df_fees = pd.DataFrame(data)

    # Define the transaction amount for the scenario
    transaction_amount = 50.0

    # Ensure numeric columns are floats
    df_fees['fixed_amount'] = df_fees['fixed_amount'].apply(coerce_to_float)
    df_fees['rate'] = df_fees['rate'].apply(coerce_to_float)

    # Calculate the fee for each rule in the dataset
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    df_fees['calculated_fee_50eur'] = df_fees['fixed_amount'] + (df_fees['rate'] * transaction_amount / 10000)

    # Calculate the average fee per card scheme ("average scenario")
    # We average the fees resulting from all rules associated with each scheme
    avg_fee_by_scheme = df_fees.groupby('card_scheme')['calculated_fee_50eur'].mean()

    # Identify the most expensive scheme
    most_expensive_scheme = avg_fee_by_scheme.idxmax()
    max_fee = avg_fee_by_scheme.max()

    # Debugging output
    print("Average Fee per Scheme for 50 EUR Transaction:")
    print(avg_fee_by_scheme)
    print(f"\nMost Expensive Scheme: {most_expensive_scheme} ({max_fee:.4f} EUR)")

    # Final Answer
    print(most_expensive_scheme)

if __name__ == "__main__":
    analyze_fees()
