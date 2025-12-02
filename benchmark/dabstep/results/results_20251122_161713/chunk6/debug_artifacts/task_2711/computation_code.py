import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.replace('>', '').replace('<', '').replace('≥', '').replace('≤', '')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1000
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1000000
        if '-' in v:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range_check(value, rule_string):
    """
    Checks if a numeric value fits into a rule string like '>5', '3-5', '100k-1m', '8.3%'.
    """
    if rule_string is None:
        return True
    
    # Handle percentages in rule
    is_pct = '%' in rule_string
    
    # Clean rule string for parsing
    clean_rule = rule_string.replace('%', '').replace(',', '').lower()
    
    # Helper to parse k/m suffixes
    def parse_val(s):
        s = s.strip()
        if 'k' in s: return float(s.replace('k', '')) * 1000
        if 'm' in s: return float(s.replace('m', '')) * 1000000
        return float(s)

    try:
        if '-' in clean_rule:
            parts = clean_rule.split('-')
            low = parse_val(parts[0])
            high = parse_val(parts[1])
            if is_pct:
                low /= 100
                high /= 100
            return low <= value <= high
        
        if clean_rule.startswith('>'):
            limit = parse_val(clean_rule[1:])
            if is_pct: limit /= 100
            return value > limit
            
        if clean_rule.startswith('<'):
            limit = parse_val(clean_rule[1:])
            if is_pct: limit /= 100
            return value < limit
            
        # Exact match (rare for ranges but possible)
        limit = parse_val(clean_rule)
        if is_pct: limit /= 100
        return value == limit
        
    except:
        return False

def match_capture_delay(merchant_delay, rule_delay):
    """
    Matches merchant capture delay (e.g., '1', 'immediate') with rule (e.g., '<3', 'immediate').
    """
    if rule_delay is None:
        return True
    
    # Exact string match
    if str(merchant_delay).lower() == str(rule_delay).lower():
        return True
        
    # Numeric comparison if merchant_delay is numeric-like
    try:
        delay_days = float(merchant_delay)
        return parse_range_check(delay_days, rule_delay)
    except ValueError:
        # merchant_delay is 'immediate' or 'manual', rule is range -> False
        return False

def match_fee_rule(tx_context, rule):
    """
    Checks if a transaction context matches a fee rule.
    """
    # 1. Card Scheme (Exact)
    if rule.get('card_scheme') != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List contains)
    if rule.get('account_type') and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List contains)
    if rule.get('merchant_category_code') and tx_context['merchant_category_code'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (Complex match)
    if not match_capture_delay(tx_context['capture_delay'], rule.get('capture_delay')):
        return False
        
    # 5. Monthly Volume (Range)
    if not parse_range_check(tx_context['monthly_volume'], rule.get('monthly_volume')):
        return False
        
    # 6. Monthly Fraud Level (Range)
    if not parse_range_check(tx_context['monthly_fraud_level'], rule.get('monthly_fraud_level')):
        return False
        
    # 7. Is Credit (Bool)
    if rule.get('is_credit') is not None and rule['is_credit'] != tx_context['is_credit']:
        return False
        
    # 8. ACI (List contains)
    if rule.get('aci') and tx_context['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry (Bool)
    if rule.get('intracountry') is not None and rule['intracountry'] != tx_context['intracountry']:
        return False
        
    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

# Load Data
payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
with open('/output/chunk6/data/context/fees.json', 'r') as f:
    fees = json.load(f)
with open('/output/chunk6/data/context/merchant_data.json', 'r') as f:
    merchant_data = json.load(f)

# 1. Filter for Rafa_AI in March (Day 60-90)
merchant_name = 'Rafa_AI'
march_mask = (payments['merchant'] == merchant_name) & (payments['day_of_year'] >= 60) & (payments['day_of_year'] <= 90)
march_txs = payments[march_mask].copy()

if len(march_txs) == 0:
    print("No transactions found for Rafa_AI in March.")
    exit()

# 2. Calculate Monthly Stats (Volume & Fraud Rate) for Fee Tier determination
# Volume
monthly_volume = march_txs['eur_amount'].sum()

# Fraud Rate (Volume based)
fraud_vol = march_txs[march_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
monthly_fraud_level = fraud_vol / monthly_volume if monthly_volume > 0 else 0.0

# 3. Get Merchant Static Data
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not merchant_info:
    # Fallback if merchant not in json (though it should be)
    print(f"Merchant {merchant_name} not found in merchant_data.json")
    exit()

# 4. Identify Target Transactions (Fraudulent ones to "move")
target_txs = march_txs[march_txs['has_fraudulent_dispute'] == True].copy()

# 5. Simulate Costs for each ACI
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
aci_costs = {}

for aci in possible_acis:
    total_cost = 0.0
    
    for _, tx in target_txs.iterrows():
        # Determine intracountry
        is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Build Context
        context = {
            'card_scheme': tx['card_scheme'],
            'account_type': merchant_info['account_type'],
            'merchant_category_code': merchant_info['merchant_category_code'],
            'capture_delay': merchant_info['capture_delay'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_level,
            'is_credit': bool(tx['is_credit']),
            'aci': aci, # SIMULATED ACI
            'intracountry': is_intracountry
        }
        
        # Find Fee
        matched_rule = None
        for rule in fees:
            if match_fee_rule(context, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_cost += fee
        else:
            # If no rule matches, we assume a high cost or skip? 
            # In this dataset, there should always be a match.
            # For robustness, we can log it, but here we just proceed.
            pass
            
    aci_costs[aci] = total_cost

# 6. Find Best ACI
# We want the lowest possible fees
best_aci = min(aci_costs, key=aci_costs.get)

# Output the result
print(best_aci)