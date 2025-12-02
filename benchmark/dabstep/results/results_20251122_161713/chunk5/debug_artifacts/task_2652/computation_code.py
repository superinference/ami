import pandas as pd
import json
import numpy as np

# --- Helper Functions ---
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

def parse_range_check(value, range_str):
    """Checks if a value falls within a range string like '100k-1m', '>5', or '8.3%'."""
    if range_str is None:
        return True
    
    # Handle percentages in range string
    is_percent = '%' in str(range_str)
    
    # Clean string
    s = str(range_str).lower().replace(',', '').replace('_', '')
    
    # Helper to parse individual values with suffixes
    def parse_val(x):
        if 'k' in x: return float(x.replace('k', '')) * 1000
        if 'm' in x: return float(x.replace('m', '')) * 1000000
        if '%' in x: return float(x.replace('%', '')) / 100
        return float(x)

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_val(parts[0])
            high = parse_val(parts[1])
            return low <= value <= high
        elif '>' in s:
            limit = parse_val(s.replace('>', '').replace('=', ''))
            return value > limit
        elif '<' in s:
            limit = parse_val(s.replace('<', '').replace('=', ''))
            return value < limit
        else:
            # Exact match attempt
            return value == parse_val(s)
    except:
        return False

def match_fee_rule(ctx, rule):
    """
    Matches a transaction context against a fee rule.
    ctx: dict containing transaction and merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme (Explicit match required for this simulation)
    if rule['card_scheme'] != ctx['target_scheme']:
        return False
        
    # 2. Account Type (List match or Wildcard)
    if rule['account_type'] and ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List match or Wildcard)
    if rule['merchant_category_code'] and ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (String match, Numeric Range, or Wildcard)
    if rule['capture_delay'] is not None:
        # Exact string match (e.g., 'manual' == 'manual')
        if rule['capture_delay'] == ctx['capture_delay']:
            pass 
        # Numeric range check (e.g., merchant '1' vs rule '<3')
        elif any(c in str(rule['capture_delay']) for c in ['<', '>', '-']):
            try:
                merch_delay = float(ctx['capture_delay'])
                if not parse_range_check(merch_delay, rule['capture_delay']):
                    return False
            except ValueError:
                # Merchant delay is non-numeric (e.g. 'manual') but rule is numeric range -> No match
                return False
        else:
            # Rule is string (e.g. 'manual') but didn't match merchant (e.g. 'immediate')
            return False

    # 5. Monthly Volume (Range check)
    if not parse_range_check(ctx['monthly_volume'], rule['monthly_volume']):
        return False
        
    # 6. Monthly Fraud Level (Range check)
    if not parse_range_check(ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
        return False
        
    # 7. Is Credit (Bool match or Wildcard)
    if rule['is_credit'] is not None and rule['is_credit'] != ctx['is_credit']:
        return False
        
    # 8. ACI (List match or Wildcard)
    if rule['aci'] and ctx['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry (Bool match or Wildcard)
    # Note: In Python 0.0 == False and 1.0 == True, so this handles float/bool mismatch
    if rule['intracountry'] is not None and rule['intracountry'] != ctx['intracountry']:
        return False
        
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule['fixed_amount']
    # Rate is typically basis points or similar, divided by 10000 as per manual
    variable = (rule['rate'] * amount) / 10000
    return fixed + variable

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
merchant_path = '/output/chunk5/data/context/merchant_data.json'
fees_path = '/output/chunk5/data/context/fees.json'

print("Loading data...")
df = pd.read_csv(payments_path)
with open(merchant_path) as f:
    merchant_data = json.load(f)
with open(fees_path) as f:
    fees_data = json.load(f)

# 2. Filter for Merchant and August
target_merchant = 'Golfclub_Baron_Friso'
start_day = 213
end_day = 243

# Get Merchant Profile
merchant_profile = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_profile:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

mcc = merchant_profile['merchant_category_code']
account_type = merchant_profile['account_type']
capture_delay = merchant_profile['capture_delay']

print(f"Analyzing for Merchant: {target_merchant}")
print(f"Profile: MCC={mcc}, Account={account_type}, Capture={capture_delay}")

# Filter Transactions for August
august_txs = df[
    (df['merchant'] == target_merchant) & 
    (df['day_of_year'] >= start_day) & 
    (df['day_of_year'] <= end_day)
].copy()

if len(august_txs) == 0:
    print("No transactions found for this merchant in August.")
    exit()

# 3. Calculate Monthly Stats (Volume & Fraud)
# These stats determine the fee tier for ALL transactions in the month
total_volume = august_txs['eur_amount'].sum()

# Fraud calculation
fraud_txs = august_txs[august_txs['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

print(f"August Volume: €{total_volume:,.2f}")
print(f"August Fraud Rate: {fraud_rate:.4%}")

# 4. Evaluate Schemes
# We simulate processing ALL August transactions through each scheme to find the cheapest total cost.
schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
scheme_costs = {}

print("\nCalculating potential fees per scheme...")

for scheme in schemes:
    total_scheme_fee = 0
    possible = True
    
    for _, tx in august_txs.iterrows():
        # Build Context for this specific transaction
        ctx = {
            'target_scheme': scheme,
            'account_type': account_type,
            'mcc': mcc,
            'capture_delay': capture_delay,
            'monthly_volume': total_volume,
            'monthly_fraud_rate': fraud_rate,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': tx['issuing_country'] == tx['acquirer_country']
        }
        
        # Find matching rule
        match = None
        for rule in fees_data:
            if match_fee_rule(ctx, rule):
                match = rule
                break
        
        if match:
            fee = calculate_fee(tx['eur_amount'], match)
            total_scheme_fee += fee
        else:
            # If a scheme has NO rule for a transaction (e.g. MCC not supported), it's not a valid option
            possible = False
            # print(f"  - Scheme {scheme} cannot process transaction {tx['psp_reference']} (No matching rule)")
            break
    
    if possible:
        scheme_costs[scheme] = total_scheme_fee
        print(f"  {scheme}: €{total_scheme_fee:,.2f}")
    else:
        scheme_costs[scheme] = float('inf')
        print(f"  {scheme}: Not viable (rules do not cover all transactions)")

# 5. Find Minimum
best_scheme = min(scheme_costs, key=scheme_costs.get)
min_cost = scheme_costs[best_scheme]

print(f"\nOptimal Scheme: {best_scheme} with total fees of €{min_cost:,.2f}")

# Final Answer Output
print(best_scheme)