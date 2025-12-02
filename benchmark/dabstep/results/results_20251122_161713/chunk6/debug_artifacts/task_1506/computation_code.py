import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
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
# MAIN ANALYSIS
# ═══════════════════════════════════════════════════════════

def main():
    # 1. Load Data
    fees_path = '/output/chunk6/data/context/fees.json'
    try:
        df_fees = pd.read_json(fees_path)
        print(f"Successfully loaded {len(df_fees)} fee rules.")
    except Exception as e:
        print(f"Error loading fees.json: {e}")
        return

    # 2. Inspect Data Types and Clean if necessary
    # Ensure fixed_amount and rate are numeric
    df_fees['fixed_amount'] = df_fees['fixed_amount'].apply(coerce_to_float)
    df_fees['rate'] = df_fees['rate'].apply(coerce_to_float)

    # 3. Calculate Average Fee Parameters per Card Scheme
    # The "average scenario" implies averaging the fee rules for each scheme
    avg_fees = df_fees.groupby('card_scheme')[['fixed_amount', 'rate']].mean()
    
    print("\nAverage Fee Parameters by Card Scheme:")
    print(avg_fees)

    # 4. Calculate Total Fee for a 10 EUR Transaction
    # Formula: fee = fixed_amount + (rate * transaction_value / 10000)
    transaction_value = 10.0
    
    avg_fees['calculated_fee'] = avg_fees['fixed_amount'] + (avg_fees['rate'] * transaction_value / 10000)
    
    print(f"\nCalculated Fees for {transaction_value} EUR transaction:")
    print(avg_fees['calculated_fee'].sort_values(ascending=False))

    # 5. Identify the Most Expensive Scheme
    most_expensive_scheme = avg_fees['calculated_fee'].idxmax()
    max_fee_value = avg_fees['calculated_fee'].max()

    print("\nMost expensive card scheme:")
    print(most_expensive_scheme)

if __name__ == "__main__":
    main()