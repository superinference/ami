import pandas as pd
import re

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
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def safe_get(df, column, default=None):
    """Safely get column from DataFrame, return default if not exists."""
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

# Step 1: Calculate metric
try:
    # Load the payments data
    df = pd.read_csv('/output/chunk1/data/context/payments.csv')

    # Filter for the merchant "Martinis_Fine_Steakhouse"
    merchant_name = "Martinis_Fine_Steakhouse"
    merchant_txs = df[df['merchant'] == merchant_name]

    if merchant_txs.empty:
        fraud_rate = 0.0
    else:
        # CRITICAL: Manual Section 7 states: "Fraud is defined as the ratio of fraudulent volume over total volume."
        # We must calculate based on 'eur_amount', NOT count.
        total_volume = merchant_txs['eur_amount'].sum()
        fraud_volume = merchant_txs[merchant_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()

        if total_volume > 0:
            fraud_rate = fraud_volume / total_volume
        else:
            fraud_rate = 0.0

    # Step 2: Check manual.md for policy
    with open('/output/chunk1/data/context/manual.md', 'r') as f:
        manual = f.read()

    # Search for policy keywords
    # The manual mentions "penalties" in Section 7.2 ("avoid penalties") and 7.4 ("significant penalties").
    # It refers to "undesired levels" but does not explicitly state a numeric threshold (e.g., "1%").
    # However, in the payment industry, a fraud rate > 1% (0.01) is universally considered "danger" territory 
    # for fines/monitoring programs. Given the question asks about "danger", and the manual confirms 
    # penalties exist for high levels, we compare against this standard risk threshold.
    
    policy_keywords = ['fine', 'penalty', 'danger']
    has_policy = any(keyword in manual.lower() for keyword in policy_keywords)

    if has_policy:
        # Threshold: 1% (0.01) is the standard threshold for "danger" of fines/programs 
        # when "undesired levels" are mentioned but not numerically defined in the text.
        threshold = 0.01
        
        if fraud_rate > threshold:
            print("yes")
        else:
            print("no")
    else:
        print("Not Applicable")

except Exception as e:
    print(f"Error: {e}")