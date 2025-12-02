# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2708
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8354 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import re

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
        v = v.lstrip('><≤≥')
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except:
                return 0.0
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except:
            return 0.0
    return 0.0

def parse_volume_string(vol_str):
    """Parses strings like '100k-1m' into (min, max)."""
    if not vol_str:
        return None, None
    
    def parse_val(s):
        s = s.lower().strip()
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1000000
            s = s.replace('m', '')
        try:
            return float(s) * mult
        except:
            return 0
            
    if '-' in vol_str:
        parts = vol_str.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif '>' in vol_str:
        return parse_val(vol_str.replace('>', '')), float('inf')
    elif '<' in vol_str:
        return 0, parse_val(vol_str.replace('<', ''))
    return None, None

def parse_fraud_string(fraud_str):
    """Parses strings like '>8.3%' into (min, max)."""
    if not fraud_str:
        return None, None
        
    def parse_pct(s):
        s = s.strip().replace('%', '')
        try:
            return float(s) / 100
        except:
            return 0.0

    if '-' in fraud_str:
        parts = fraud_str.split('-')
        return parse_pct(parts[0]), parse_pct(parts[1])
    elif '>' in fraud_str:
        return parse_pct(fraud_str.replace('>', '').replace('=', '')), float('inf')
    elif '<' in fraud_str:
        return 0.0, parse_pct(fraud_str.replace('<', '').replace('=', ''))
    return None, None

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches rule."""
    if rule_delay is None:
        return True
    
    # Exact string match (e.g., "manual", "immediate")
    if str(merchant_delay).lower() == str(rule_delay).lower():
        return True
        
    # Numeric comparison for ranges
    merch_days = None
    if str(merchant_delay).isdigit():
        merch_days = float(merchant_delay)
    elif merchant_delay == 'immediate':
        merch_days = 0
    
    if merch_days is not None:
        if '>' in rule_delay:
            try:
                limit = float(re.sub(r'[^\d.]', '', rule_delay))
                return merch_days > limit
            except:
                pass
        elif '<' in rule_delay:
            try:
                limit = float(re.sub(r'[^\d.]', '', rule_delay))
                return merch_days < limit
            except:
                pass
        elif '-' in rule_delay:
            try:
                parts = rule_delay.split('-')
                low = float(parts[0])
                high = float(parts[1])
                return low <= merch_days <= high
            except:
                pass
                
    return False

def match_fee_rule(ctx, rule):
    """Matches a transaction context against a fee rule."""
    # 1. Card Scheme
    if rule.get('card_scheme') != ctx.get('card_scheme'):
        return False
        
    # 2. Account Type
    if rule.get('account_type'):
        if ctx.get('account_type') not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code
    if rule.get('merchant_category_code'):
        if ctx.get('mcc') not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx.get('is_credit'):
            return False
            
    # 5. ACI
    if rule.get('aci'):
        if ctx.get('aci') not in rule['aci']:
            return False
            
    # 6. Intracountry
    if rule.get('intracountry') is not None:
        is_intra = ctx.get('issuing_country') == ctx.get('acquirer_country')
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Capture Delay
    if not check_capture_delay(ctx.get('capture_delay'), rule.get('capture_delay')):
        return False
        
    # 8. Monthly Volume
    if rule.get('monthly_volume'):
        min_v, max_v = parse_volume_string(rule['monthly_volume'])
        vol = ctx.get('monthly_volume', 0)
        if min_v is not None and (vol < min_v or vol > max_v):
            return False
            
    # 9. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_fraud_string(rule['monthly_fraud_level'])
        fraud = ctx.get('monthly_fraud_rate', 0)
        if min_f is not None and (fraud < min_f or fraud > max_f):
            return False
            
    return True

def calculate_fee(amount, rule):
    fixed = rule.get('fixed_amount', 0)
    rate = rule.get('rate', 0)
    return fixed + (rate * amount / 10000)

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

# Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# Target Merchant and Period
target_merchant = 'Crossfit_Hanna'
# March: Day 60 to 90 (inclusive)
start_day = 60
end_day = 90

# 1. Get Merchant Info
merch_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merch_info:
    print("Merchant not found")
    exit()

# 2. Calculate Monthly Stats for March (ALL transactions for the merchant)
# These stats determine the fee tier (volume/fraud level)
march_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['day_of_year'] >= start_day) &
    (df_payments['day_of_year'] <= end_day)
]

total_volume = march_txs['eur_amount'].sum()
fraud_volume = march_txs[march_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
# Fraud rate defined as ratio of fraudulent volume over total volume (Manual Sec 7)
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

# 3. Identify Fraudulent Transactions to "Move"
target_txs = march_txs[march_txs['has_fraudulent_dispute'] == True].copy()

# 4. Simulate Fees for each ACI
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
aci_costs = {}

for aci in possible_acis:
    total_fee = 0.0
    
    for _, row in target_txs.iterrows():
        # Build Context with OVERRIDDEN ACI
        ctx = {
            'card_scheme': row['card_scheme'],
            'account_type': merch_info['account_type'],
            'mcc': merch_info['merchant_category_code'],
            'is_credit': row['is_credit'],
            'aci': aci, # Testing this ACI
            'issuing_country': row['issuing_country'],
            'acquirer_country': row['acquirer_country'],
            'capture_delay': merch_info['capture_delay'],
            'monthly_volume': total_volume,
            'monthly_fraud_rate': fraud_rate
        }
        
        # Find Matching Rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break # First match priority
        
        if matched_rule:
            fee = calculate_fee(row['eur_amount'], matched_rule)
            total_fee += fee
            
    aci_costs[aci] = total_fee

# 5. Find Lowest Cost
best_aci = min(aci_costs, key=aci_costs.get)

# Output the preferred ACI
print(best_aci)
