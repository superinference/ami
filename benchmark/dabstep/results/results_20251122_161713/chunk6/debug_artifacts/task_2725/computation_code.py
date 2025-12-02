import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
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
        v_lower = v.lower()
        if 'k' in v_lower:
            return float(v_lower.replace('k', '')) * 1000
        if 'm' in v_lower:
            return float(v_lower.replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range(range_str):
    """Parses strings like '100k-1m', '0%-0.8%', '>5', '<3' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower()
    is_pct = '%' in s
    s = s.replace('%', '')
    
    def parse_val(x):
        x = x.strip()
        mult = 1
        if 'k' in x:
            mult = 1000
            x = x.replace('k', '')
        elif 'm' in x:
            mult = 1000000
            x = x.replace('m', '')
        try:
            return float(x) * mult
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        low = parse_val(parts[0])
        high = parse_val(parts[1])
        if is_pct:
            low /= 100
            high /= 100
        return low, high
    elif '>' in s:
        val = parse_val(s.replace('>', ''))
        if is_pct: val /= 100
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        if is_pct: val /= 100
        return float('-inf'), val
    else:
        val = parse_val(s)
        if is_pct: val /= 100
        return val, val

def check_capture_delay(rule_delay, merchant_delay):
    """Checks if merchant delay matches rule delay."""
    if rule_delay is None:
        return True
    
    md_str = str(merchant_delay).lower()
    rd_str = str(rule_delay).lower()
    
    if md_str == rd_str:
        return True
    
    # Convert merchant delay to number if possible (immediate=0)
    md_val = None
    if md_str == 'immediate': md_val = 0.0
    elif md_str == 'manual': md_val = 999.0 # Distinct from small numeric delays
    else:
        try: md_val = float(md_str)
        except: md_val = None
        
    if rd_str == 'manual' or rd_str == 'immediate':
        return False # Already checked equality above
        
    if md_val is None: return False
    
    if rd_str.startswith('>'):
        return md_val > float(rd_str[1:])
    if rd_str.startswith('<'):
        return md_val < float(rd_str[1:])
    if '-' in rd_str:
        try:
            low, high = map(float, rd_str.split('-'))
            return low <= md_val <= high
        except:
            return False
    return False

def match_fee_rule(tx_ctx, rule):
    """Matches a transaction context against a fee rule."""
    # 1. Card Scheme
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type
    if rule['account_type']:
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code
    if rule['merchant_category_code']:
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay
    if not check_capture_delay(rule['capture_delay'], tx_ctx['capture_delay']):
        return False
        
    # 5. Monthly Volume
    if rule['monthly_volume']:
        low, high = parse_range(rule['monthly_volume'])
        if not (low <= tx_ctx['monthly_volume'] <= high):
            return False
            
    # 6. Monthly Fraud Level
    if rule['monthly_fraud_level']:
        low, high = parse_range(rule['monthly_fraud_level'])
        if not (low <= tx_ctx['monthly_fraud_level'] <= high):
            return False
            
    # 7. Is Credit
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 8. ACI (The variable we are testing)
    if rule['aci']:
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry
    if rule['intracountry'] is not None:
        is_intra = (tx_ctx['issuing_country'] == tx_ctx['acquirer_country'])
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000.0)

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data_list = json.load(f)

# 2. Filter for Merchant and Month (June: Day 152-181)
target_merchant = 'Martinis_Fine_Steakhouse'
df_merchant_june = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['day_of_year'] >= 152) & 
    (df_payments['day_of_year'] <= 181)
].copy()

# 3. Calculate Merchant Stats for June (Volume & Fraud)
# These stats determine which fee tier applies to the merchant
total_volume = df_merchant_june['eur_amount'].sum()
fraud_volume = df_merchant_june[df_merchant_june['has_fraudulent_dispute'] == True]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

# Get Merchant Static Data
merchant_info = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found")

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 4. Identify Target Transactions (Fraudulent ones in June)
# We will calculate the cost of THESE transactions under different ACIs
target_txs = df_merchant_june[df_merchant_june['has_fraudulent_dispute'] == True].copy()

# 5. Simulate Fees for each ACI
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
aci_costs = {}

for aci in possible_acis:
    total_cost = 0.0
    valid_aci = True
    
    for _, row in target_txs.iterrows():
        # Build Context for Fee Matching
        tx_ctx = {
            'card_scheme': row['card_scheme'],
            'account_type': account_type,
            'mcc': mcc,
            'capture_delay': capture_delay,
            'monthly_volume': total_volume,
            'monthly_fraud_level': fraud_rate,
            'is_credit': row['is_credit'],
            'aci': aci, # HYPOTHETICAL ACI
            'issuing_country': row['issuing_country'],
            'acquirer_country': row['acquirer_country']
        }
        
        # Find First Matching Rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break 
        
        if matched_rule:
            fee = calculate_fee(row['eur_amount'], matched_rule)
            total_cost += fee
        else:
            # If no rule exists for this ACI/Scheme combo, this ACI is invalid/impossible
            valid_aci = False
            break
            
    if valid_aci:
        aci_costs[aci] = total_cost
    else:
        aci_costs[aci] = float('inf')

# 6. Determine Best ACI
# Filter out infinite costs
valid_costs = {k: v for k, v in aci_costs.items() if v != float('inf')}

if valid_costs:
    best_aci = min(valid_costs, key=valid_costs.get)
    print(best_aci)
else:
    print("No valid ACI found")