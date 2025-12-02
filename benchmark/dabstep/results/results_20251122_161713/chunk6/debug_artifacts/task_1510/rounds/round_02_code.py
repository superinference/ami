# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1510
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 2348 characters (FULL CODE)
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

def calculate_fee(amount, fee_rule):
    """
    Calculates fee based on manual.md formula:
    fee = fixed_amount + rate * transaction_value / 10000
    """
    fixed = coerce_to_float(fee_rule.get('fixed_amount', 0))
    rate = coerce_to_float(fee_rule.get('rate', 0))
    
    # Formula from manual.md
    fee = fixed + (rate * amount / 10000.0)
    return fee

# 1. Load the fees.json file
fees_file_path = '/output/chunk6/data/context/fees.json'
with open(fees_file_path, 'r') as f:
    fees_data = json.load(f)

# 2. Define transaction parameters
transaction_amount = 100.0

# 3. Calculate fee for each rule
calculated_fees = []

for rule in fees_data:
    scheme = rule.get('card_scheme')
    if not scheme:
        continue
        
    fee = calculate_fee(transaction_amount, rule)
    
    calculated_fees.append({
        'card_scheme': scheme,
        'fee': fee
    })

# 4. Convert to DataFrame for aggregation
df_fees_calc = pd.DataFrame(calculated_fees)

# 5. Group by card_scheme and calculate the average (mean) fee
# "In the average scenario" implies averaging the fees defined in the rules
avg_fees_by_scheme = df_fees_calc.groupby('card_scheme')['fee'].mean()

# 6. Identify the most expensive scheme
most_expensive_scheme = avg_fees_by_scheme.idxmax()
highest_avg_fee = avg_fees_by_scheme.max()

# Print intermediate results for verification
print("Average Fee per Card Scheme (for 100 EUR transaction):")
print(avg_fees_by_scheme.sort_values(ascending=False))
print("-" * 30)

# 7. Final Answer
print(most_expensive_scheme)
