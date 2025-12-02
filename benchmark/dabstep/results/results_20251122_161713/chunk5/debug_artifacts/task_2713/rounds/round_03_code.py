# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2713
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 8238 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100.0
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
            return None
    return None

def parse_range_check(value, rule_range_str):
    """
    Checks if value is within the rule_range_str.
    Handles numeric ranges (100k-1m), inequalities (>5), percentages (8.3%), and keywords (manual).
    """
    if rule_range_str is None:
        return True
    
    s_rule = str(rule_range_str).lower().strip()
    s_val = str(value).lower().strip()
    
    # Handle keywords for capture_delay
    keywords = ['manual', 'immediate']
    if s_rule in keywords:
        return s_val == s_rule
    if s_val in keywords:
        # Value is keyword (e.g., 'manual'), but rule is numeric/range (e.g., '>5'). No match.
        return False

    # Helper to parse numeric strings with k/m/% suffixes
    def parse_num(s):
        s = s.replace('%', '')
        factor = 1.0
        if 'k' in s:
            factor = 1000.0
            s = s.replace('k', '')
        elif 'm' in s:
            factor = 1000000.0
            s = s.replace('m', '')
        return float(s) * factor

    try:
        # Determine if rule implies percentage comparison
        is_percent_rule = '%' in str(rule_range_str)
        
        # Parse the value to check
        val_num = float(value)
        
        # Logic for inequalities and ranges
        if '>' in s_rule:
            limit = parse_num(s_rule.replace('>', '').replace('=', ''))
            if is_percent_rule: limit /= 100.0
            return val_num > limit 
        elif '<' in s_rule:
            limit = parse_num(s_rule.replace('<', '').replace('=', ''))
            if is_percent_rule: limit /= 100.0
            return val_num < limit
        elif '-' in s_rule:
            parts = s_rule.split('-')
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            if is_percent_rule:
                low /= 100.0
                high /= 100.0
            return low <= val_num <= high
        else:
            # Exact numeric match
            target = parse_num(s_rule)
            if is_percent_rule: target /= 100.0
            return val_num == target
            
    except (ValueError, TypeError):
        return False

def match_fee_rule(ctx, rule):
    """
    Matches a transaction context against a fee rule.
    Returns True if the rule applies, False otherwise.
    """
    # 1. Card Scheme (Exact match required)
    if rule.get('card_scheme') != ctx.get('card_scheme'):
        return False
        
    # 2. Account Type (List match or wildcard)
    if rule.get('account_type'):
        if ctx.get('account_type') not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match or wildcard)
    if rule.get('merchant_category_code'):
        if ctx.get('mcc') not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Boolean match or wildcard)
    if rule.get('is_credit') is not None:
        # Handle potential string 'true'/'false' in JSON
        r_cred = str(rule['is_credit']).lower() == 'true'
        if r_cred != ctx.get('is_credit'):
            return False
            
    # 5. ACI (List match or wildcard)
    if rule.get('aci'):
        if ctx.get('aci') not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean match or wildcard)
    if rule.get('intracountry') is not None:
        # JSON might have 0.0/1.0 or boolean
        r_intra = bool(float(rule['intracountry']))
        if r_intra != ctx.get('intracountry'):
            return False
            
    # 7. Capture Delay (Range/Keyword match or wildcard)
    if not parse_range_check(ctx.get('capture_delay'), rule.get('capture_delay')):
        return False
        
    # 8. Monthly Volume (Range match or wildcard)
    if not parse_range_check(ctx.get('monthly_volume'), rule.get('monthly_volume')):
        return False
        
    # 9. Monthly Fraud Level (Range match or wildcard)
    if not parse_range_check(ctx.get('monthly_fraud_level'), rule.get('monthly_fraud_level')):
        return False
        
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000.0)

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Setup Merchant Context
merchant_name = 'Crossfit_Hanna'
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)

if not merchant_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

# 3. Calculate Merchant Stats for April (Day 91-120)
# These stats determine the fee tier (Volume and Fraud Level)
april_mask = (
    (df_payments['merchant'] == merchant_name) & 
    (df_payments['day_of_year'] >= 91) & 
    (df_payments['day_of_year'] <= 120)
)
april_txs = df_payments[april_mask]

monthly_volume = april_txs['eur_amount'].sum()
fraud_volume = april_txs[april_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 4. Identify Target Transactions (Fraudulent ones in April)
# We want to optimize fees for THESE specific transactions by changing their ACI
target_txs = april_txs[april_txs['has_fraudulent_dispute'] == True].copy()

# 5. Simulate Costs for each ACI
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
aci_results = {}

for aci in possible_acis:
    total_fee = 0.0
    valid_aci = True
    
    for _, tx in target_txs.iterrows():
        # Construct context for this transaction + hypothetical ACI
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': m_account_type,
            'mcc': m_mcc,
            'capture_delay': m_capture_delay,
            'is_credit': tx['is_credit'],
            'aci': aci, # HYPOTHETICAL ACI
            'intracountry': tx['issuing_country'] == tx['acquirer_country'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_rate
        }
        
        # Find the first matching rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee += fee
        else:
            # If no rule matches for this ACI, it's not a valid option (or infinitely expensive)
            # We penalize it heavily to ensure it's not selected
            total_fee = float('inf')
            valid_aci = False
            break
    
    if valid_aci:
        aci_results[aci] = total_fee

# 6. Determine Preferred Choice
if aci_results:
    best_aci = min(aci_results, key=aci_results.get)
    print(best_aci)
else:
    print("No valid ACI found")
