import pandas as pd
import json
import numpy as np

# --- HELPER FUNCTIONS ---
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
            return float(v.replace('%', '')) / 100
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

def is_not_empty(obj):
    """Check if list/array is not empty/None."""
    if obj is None:
        return False
    if isinstance(obj, list):
        return len(obj) > 0
    if hasattr(obj, 'size'):
        return obj.size > 0
    return False

def check_range(value, rule_str):
    """Checks if numeric value fits in rule_str (e.g. '>5', '100k-1m', '8.3%')."""
    if rule_str is None:
        return True
    
    s = str(rule_str).strip().lower().replace(',', '')
    
    # Handle percentages
    is_pct = '%' in s
    if is_pct:
        s = s.replace('%', '')
        
    # Handle k/m suffixes
    mult = 1
    if 'k' in s: mult = 1000; s = s.replace('k', '')
    elif 'm' in s: mult = 1000000; s = s.replace('m', '')
    
    try:
        if '-' in s:
            parts = s.split('-')
            low = float(parts[0]) * mult
            high = float(parts[1]) * mult
            if is_pct: low/=100; high/=100
            return low <= value <= high
        elif s.startswith('>'):
            limit = float(s[1:]) * mult
            if is_pct: limit/=100
            return value > limit
        elif s.startswith('<'):
            limit = float(s[1:]) * mult
            if is_pct: limit/=100
            return value < limit
        else:
            target = float(s) * mult
            if is_pct: target/=100
            return value == target
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx keys: card_scheme, account_type, mcc, capture_delay, monthly_fraud_percent, 
                 monthly_volume, is_credit, aci, intracountry
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False

    # 2. Account Type (List match - wildcard if empty)
    if is_not_empty(rule.get('account_type')):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match - wildcard if empty)
    if is_not_empty(rule.get('merchant_category_code')):
        try:
            if int(tx_ctx.get('mcc')) not in [int(x) for x in rule['merchant_category_code']]:
                return False
        except:
            return False

    # 4. Capture Delay (Mixed match)
    if rule.get('capture_delay'):
        r_cd = str(rule['capture_delay']).lower()
        t_cd = str(tx_ctx.get('capture_delay')).lower()
        
        if any(x in r_cd for x in ['<', '>', '-']):
            try:
                t_val = float(t_cd)
                if not check_range(t_val, r_cd):
                    return False
            except ValueError:
                return False
        else:
            if r_cd != t_cd:
                return False

    # 5. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_ctx.get('monthly_fraud_percent', 0), rule['monthly_fraud_level']):
            return False

    # 6. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range(tx_ctx.get('monthly_volume', 0), rule['monthly_volume']):
            return False

    # 7. Is Credit (Bool match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 8. ACI (List match)
    if is_not_empty(rule.get('aci')):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 9. Intracountry (Bool match)
    if rule.get('intracountry') is not None:
        r_intra = bool(rule['intracountry'])
        if r_intra != tx_ctx.get('intracountry'):
            return False

    return True

# --- MAIN SCRIPT ---
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

# 1. Load Data
try:
    df = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchants = json.load(f)
except Exception as e:
    print(f"Error loading files: {e}")
    exit()

# 2. Filter for Rafa_AI in October 2023 (Day 274-304)
target_merchant = 'Rafa_AI'
df_oct = df[
    (df['merchant'] == target_merchant) &
    (df['year'] == 2023) &
    (df['day_of_year'] >= 274) &
    (df['day_of_year'] <= 304)
].copy()

if df_oct.empty:
    print("0.00000000000000")
    exit()

# 3. Get Merchant Attributes
merch_info = next((m for m in merchants if m['merchant'] == target_merchant), None)
if not merch_info:
    print("Merchant not found")
    exit()

# 4. Calculate Monthly Stats (Volume & Fraud)
# Manual: "monthly_fraud_level... ratio between monthly total volume and monthly volume notified as fraud"
monthly_vol = df_oct['eur_amount'].sum()
fraud_vol = df_oct[df_oct['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_ratio = fraud_vol / monthly_vol if monthly_vol > 0 else 0.0

# 5. Get Target Rule (ID 384)
target_rule_id = 384
target_rule = next((r for r in fees if r['ID'] == target_rule_id), None)
if not target_rule:
    print("Rule 384 not found")
    exit()

old_rate = target_rule['rate']
new_rate = 1

# 6. Iterate transactions to find matches
affected_amount = 0.0

# Pre-compute static context
static_ctx = {
    'account_type': merch_info['account_type'],
    'mcc': merch_info['merchant_category_code'],
    'capture_delay': merch_info['capture_delay'],
    'monthly_volume': monthly_vol,
    'monthly_fraud_percent': monthly_fraud_ratio
}

# Sort fees by ID to ensure correct priority (assuming ID 1 is highest priority)
fees.sort(key=lambda x: x['ID'])

for _, tx in df_oct.iterrows():
    # Dynamic context
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    ctx = static_ctx.copy()
    ctx.update({
        'card_scheme': tx['card_scheme'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': is_intra
    })
    
    # Find first matching rule
    matched_id = None
    for rule in fees:
        if match_fee_rule(ctx, rule):
            matched_id = rule['ID']
            break
            
    if matched_id == target_rule_id:
        affected_amount += tx['eur_amount']

# 7. Calculate Delta
# Delta = (New Rate - Old Rate) * Amount / 10000
delta = (new_rate - old_rate) * affected_amount / 10000

print(f"{delta:.14f}")