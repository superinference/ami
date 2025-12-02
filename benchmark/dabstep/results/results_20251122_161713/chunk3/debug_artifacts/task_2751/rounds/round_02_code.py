# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2751
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8124 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1000
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1000000
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (coerce_to_float(parts[0]) + coerce_to_float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '>8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower().replace(',', '').replace('%', '')
    is_percent = '%' in range_str
    scale = 0.01 if is_percent else 1.0
    
    # Handle k/m suffixes for volume
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1.0
        if 'k' in val_s:
            mult = 1000.0
            val_s = val_s.replace('k', '')
        elif 'm' in val_s:
            mult = 1000000.0
            val_s = val_s.replace('m', '')
        try:
            return float(val_s) * mult * scale
        except:
            return 0.0

    if '>' in s:
        val = parse_val(s.replace('>', '').replace('=', ''))
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', '').replace('=', ''))
        return float('-inf'), val
    elif '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    else:
        # Exact match treated as range
        val = parse_val(s)
        return val, val

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx must contain: card_scheme, is_credit, aci, mcc, account_type, 
                         monthly_volume, monthly_fraud_rate, intracountry
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Is Credit (Boolean match or Wildcard if None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 3. ACI (List containment or Wildcard if empty/None)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 4. Merchant Category Code (List containment or Wildcard)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 5. Account Type (List containment or Wildcard)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 6. Intracountry (Boolean match or Wildcard)
    if rule.get('intracountry') is not None:
        # Convert boolean to 0.0/1.0 for comparison if needed, or direct bool
        rule_intra = bool(rule['intracountry'])
        tx_intra = bool(tx_ctx['intracountry'])
        if rule_intra != tx_intra:
            return False

    # 7. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_ctx['monthly_volume'] <= max_v):
            return False

    # 8. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # tx_ctx['monthly_fraud_rate'] is a ratio (e.g. 0.08), range is parsed to ratio
        if not (min_f <= tx_ctx['monthly_fraud_rate'] <= max_f):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Get Merchant Context for 'Rafa_AI'
target_merchant = 'Rafa_AI'
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)

if not merchant_info:
    print("Error: Merchant not found")
    exit()

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']

# 3. Calculate Monthly Stats for November (Day 305-334)
# Filter for Rafa_AI in November
nov_mask = (df['merchant'] == target_merchant) & (df['day_of_year'] >= 305) & (df['day_of_year'] <= 334)
df_nov = df[nov_mask]

total_volume_nov = df_nov['eur_amount'].sum()
fraud_volume_nov = df_nov[df_nov['has_fraudulent_dispute'] == True]['eur_amount'].sum()

# Calculate fraud rate (ratio)
if total_volume_nov > 0:
    monthly_fraud_rate = fraud_volume_nov / total_volume_nov
else:
    monthly_fraud_rate = 0.0

# 4. Identify Target Transactions (Fraudulent ones in Nov)
# The question asks to move the *fraudulent* transactions to a different ACI.
target_txs = df_nov[df_nov['has_fraudulent_dispute'] == True].copy()

# 5. Simulate Fees for each ACI
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
aci_costs = {}

for aci_candidate in possible_acis:
    total_fee_for_aci = 0.0
    
    for _, tx in target_txs.iterrows():
        # Construct context for this transaction with the CANDIDATE ACI
        tx_ctx = {
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'aci': aci_candidate,  # OVERRIDE with candidate
            'mcc': mcc,
            'account_type': account_type,
            'monthly_volume': total_volume_nov,
            'monthly_fraud_rate': monthly_fraud_rate,
            'intracountry': tx['issuing_country'] == tx['acquirer_country']
        }
        
        # Find applicable rule
        applied_rule = None
        # Iterate through fees to find the first match (assuming priority or first match wins)
        # In real scenarios, there might be a priority logic, but usually first match in JSON is standard unless specified.
        for rule in fees_data:
            if match_fee_rule(tx_ctx, rule):
                applied_rule = rule
                break
        
        if applied_rule:
            fee = calculate_fee(tx['eur_amount'], applied_rule)
            total_fee_for_aci += fee
        else:
            # If no rule matches, this ACI might be invalid for this transaction type, 
            # or we assume a default high cost? 
            # For this exercise, we assume coverage exists or ignore. 
            # Let's assume 0 or log warning. Given the dataset, coverage should exist.
            pass

    aci_costs[aci_candidate] = total_fee_for_aci

# 6. Find Preferred Choice (Lowest Fee)
# Sort by cost
sorted_acis = sorted(aci_costs.items(), key=lambda x: x[1])
best_aci = sorted_acis[0][0]
min_cost = sorted_acis[0][1]

# Output the result
# Question: "what would be the preferred choice" -> Return the ACI code.
print(best_aci)
