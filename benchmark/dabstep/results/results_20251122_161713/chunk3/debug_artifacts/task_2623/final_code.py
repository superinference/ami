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

def parse_range_match(value, rule_string):
    """
    Checks if a numeric value fits within a rule string range.
    Examples of rule_string: '100k-1m', '>8.3%', '<3', '7.7%-8.3%'
    """
    if rule_string is None:
        return True  # Wildcard matches everything
        
    # Normalize rule string
    s = str(rule_string).lower().strip()
    
    # Handle k/m suffixes for volume
    def parse_val(x):
        x = x.replace('%', '')
        mult = 1
        if 'k' in x:
            mult = 1000
            x = x.replace('k', '')
        elif 'm' in x:
            mult = 1000000
            x = x.replace('m', '')
        return float(x) * mult

    try:
        # Percentage adjustment for value if rule has %
        val_to_check = value
        if '%' in s and value < 1.0: # Assuming value is ratio (0.10) and rule is % (10%)
             # If rule is %, we treat value as ratio. 
             # But parse_val strips %. So 8.3% becomes 8.3.
             # We should convert value 0.101 -> 10.1 to match rule scale
             val_to_check = value * 100

        if '-' in s:
            parts = s.split('-')
            low = parse_val(parts[0])
            high = parse_val(parts[1])
            return low <= val_to_check <= high
            
        if s.startswith('>'):
            limit = parse_val(s[1:])
            return val_to_check > limit
            
        if s.startswith('<'):
            limit = parse_val(s[1:])
            return val_to_check < limit
            
        # Exact match (rare for ranges, but possible)
        return val_to_check == parse_val(s)
        
    except Exception as e:
        # Fallback: if parsing fails, assume no match to be safe
        return False

def match_fee_rule(tx_profile, rule):
    """
    Determines if a fee rule applies to a specific transaction/merchant profile.
    """
    # 1. Merchant Category Code (List match)
    if rule.get('merchant_category_code') is not None:
        if tx_profile['mcc'] not in rule['merchant_category_code']:
            return False

    # 2. Account Type (List match)
    if rule.get('account_type'): # Check if not empty/None
        if tx_profile['account_type'] not in rule['account_type']:
            return False

    # 3. Capture Delay (Exact match)
    if rule.get('capture_delay') is not None:
        if rule['capture_delay'] != tx_profile['capture_delay']:
            return False

    # 4. Monthly Volume (Range match)
    if rule.get('monthly_volume') is not None:
        if not parse_range_match(tx_profile['monthly_volume'], rule['monthly_volume']):
            return False

    # 5. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level') is not None:
        if not parse_range_match(tx_profile['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    # 6. Is Credit (Exact match - if rule specifies)
    # Note: For "steering traffic" generally, we might look at the average case.
    # However, if a rule is SPECIFIC to credit=True and our avg tx is generic, 
    # we need to decide. Given the prompt asks for "using average transaction amount",
    # we will calculate the fee for the rule that matches. 
    # If the merchant has mixed traffic, we'll check if the rule applies to the majority or if we average.
    # For this specific problem, we'll assume we check all valid rules and find the max potential fee.
    
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000)

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
df_payments = pd.read_csv('/output/chunk3/data/context/payments.csv')
with open('/output/chunk3/data/context/merchant_data.json', 'r') as f:
    merchant_data = json.load(f)
with open('/output/chunk3/data/context/fees.json', 'r') as f:
    fees_data = json.load(f)

# 2. Define Target
target_merchant = 'Martinis_Fine_Steakhouse'
start_day = 121
end_day = 151

# 3. Calculate May Metrics
# Filter for merchant and date range
may_txs = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['day_of_year'] >= start_day) & 
    (df_payments['day_of_year'] <= end_day)
]

total_volume = may_txs['eur_amount'].sum()
fraud_txs = may_txs[may_txs['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0
avg_amount = may_txs['eur_amount'].mean()

print(f"--- May Metrics for {target_merchant} ---")
print(f"Total Volume: €{total_volume:,.2f}")
print(f"Fraud Volume: €{fraud_volume:,.2f}")
print(f"Fraud Rate: {fraud_rate:.2%}")
print(f"Avg Amount: €{avg_amount:.2f}")

# 4. Get Merchant Static Profile
m_profile = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not m_profile:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# Construct the profile dictionary for matching
profile_for_matching = {
    'mcc': m_profile['merchant_category_code'],
    'account_type': m_profile['account_type'],
    'capture_delay': m_profile['capture_delay'],
    'monthly_volume': total_volume,
    'monthly_fraud_rate': fraud_rate
}

print(f"\n--- Merchant Profile ---")
print(f"MCC: {profile_for_matching['mcc']}")
print(f"Account Type: {profile_for_matching['account_type']}")
print(f"Capture Delay: {profile_for_matching['capture_delay']}")

# 5. Evaluate Fee Rules per Scheme
scheme_fees = {}

# Get unique schemes from payments to ensure we check relevant ones
# (Or check all schemes in fees.json)
schemes = set(r['card_scheme'] for r in fees_data)

print(f"\n--- Evaluating Schemes ---")

for scheme in schemes:
    # Find all rules for this scheme that match the merchant profile
    matching_rules = []
    for rule in fees_data:
        if rule['card_scheme'] == scheme:
            if match_fee_rule(profile_for_matching, rule):
                matching_rules.append(rule)
    
    if not matching_rules:
        continue
        
    # Calculate fee for the average transaction amount
    # If multiple rules match (e.g. one for credit, one for debit), we need a strategy.
    # Since we want to know the MAXIMUM fees to steer traffic to, we will take the 
    # highest fee applicable to this merchant's profile for that scheme.
    max_fee_for_scheme = 0
    best_rule_id = None
    
    for rule in matching_rules:
        fee = calculate_fee(avg_amount, rule)
        if fee > max_fee_for_scheme:
            max_fee_for_scheme = fee
            best_rule_id = rule['ID']
            
    scheme_fees[scheme] = max_fee_for_scheme
    print(f"Scheme: {scheme:<15} | Max Fee: €{max_fee_for_scheme:.4f} (Rule ID: {best_rule_id})")

# 6. Determine Winner
if scheme_fees:
    most_expensive_scheme = max(scheme_fees, key=scheme_fees.get)
    max_fee = scheme_fees[most_expensive_scheme]
    print(f"\nRESULT: To pay the maximum fees, steer traffic to: {most_expensive_scheme}")
else:
    print("\nRESULT: No matching fee rules found.")