import pandas as pd
import json
import re

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
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

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk1/data/context/payments.csv'
fees_path = '/output/chunk1/data/context/fees.json'
manual_path = '/output/chunk1/data/context/manual.md'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Calculate Fraud Rate per Merchant (Volume Based)
# Manual.md: "Fraud is defined as the ratio of fraudulent volume over total volume."
merchant_stats = []
merchants = df['merchant'].unique()

print("--- Merchant Fraud Rates (Volume Based) ---")
for merchant in merchants:
    merchant_txs = df[df['merchant'] == merchant]
    
    total_volume = merchant_txs['eur_amount'].sum()
    
    # Filter for fraudulent transactions
    fraud_txs = merchant_txs[merchant_txs['has_fraudulent_dispute'] == True]
    fraud_volume = fraud_txs['eur_amount'].sum()
    
    # Calculate rate
    if total_volume > 0:
        fraud_rate_decimal = fraud_volume / total_volume
        fraud_rate_percent = fraud_rate_decimal * 100
    else:
        fraud_rate_percent = 0.0
        
    merchant_stats.append({
        'merchant': merchant,
        'fraud_rate': fraud_rate_percent
    })
    print(f"{merchant}: {fraud_rate_percent:.2f}%")

# 3. Identify "Excessive Fraud Threshold" from fees.json
# We look for 'monthly_fraud_level' entries, specifically those indicating a high upper bound (e.g., ">X%")
threshold_candidates = set()

for rule in fees_data:
    fraud_level = rule.get('monthly_fraud_level')
    if fraud_level:
        # Look for patterns like ">8.3%" which indicate the start of the excessive tier
        if isinstance(fraud_level, str) and '>' in fraud_level:
            # Extract the number
            val = coerce_to_float(fraud_level)
            # coerce_to_float handles the % and returns a decimal (e.g. 0.083)
            # We want percentage for comparison (8.3)
            threshold_candidates.add(val * 100)

# Determine the threshold
# If we found explicit ">X%" values, the maximum of these is likely the "excessive" threshold.
if threshold_candidates:
    excessive_threshold = max(threshold_candidates)
else:
    # Fallback: Look for the upper bound of ranges if no ">" found
    # (This part is a safeguard, but the sample data showed ">8.3%")
    excessive_threshold = 8.3 # Default based on typical data if extraction fails

print(f"\n--- Threshold Identification ---")
print(f"Identified Excessive Fraud Threshold: {excessive_threshold}%")

# 4. Compare and Answer
# Question: "Are there any merchants under the excessive fraud threshold?"
# Interpretation: Are there merchants where fraud_rate < threshold?

merchants_under_threshold = [
    m['merchant'] for m in merchant_stats 
    if m['fraud_rate'] < excessive_threshold
]

print(f"\n--- Comparison ---")
print(f"Merchants under threshold ({excessive_threshold}%): {merchants_under_threshold}")

# Final Answer Logic
if len(merchants_under_threshold) > 0:
    print("\nAnswer:")
    print("Yes")
else:
    print("\nAnswer:")
    print("No")