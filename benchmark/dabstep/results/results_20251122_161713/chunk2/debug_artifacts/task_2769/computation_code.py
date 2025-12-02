import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None: return 0.0
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
    return float(value)

def parse_volume_range(vol_str):
    """Parses volume strings like '100k-1m', '>10m' into (min, max)."""
    if not vol_str:
        return (0, float('inf'))
    
    s = str(vol_str).lower().replace(',', '').replace('€', '').strip()
    
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
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif '>' in s:
        return (parse_val(s.replace('>', '')), float('inf'))
    elif '<' in s:
        return (0, parse_val(s.replace('<', '')))
    return (0, float('inf'))

def parse_fraud_range(fraud_str):
    """Parses fraud strings like '0.0%-0.8%', '>8.3%' into (min, max)."""
    if not fraud_str:
        return (0.0, 1.0)
    
    s = str(fraud_str).replace('%', '').strip()
    
    def parse_val(x):
        try:
            return float(x) / 100.0
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif '>' in s:
        return (parse_val(s.replace('>', '')), 1.0)
    elif '<' in s:
        return (0.0, parse_val(s.replace('<', '')))
    return (0.0, 1.0)

def get_month_from_doy(doy):
    # 2023 is not a leap year
    limits = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 366]
    for i in range(12):
        if limits[i] < doy <= limits[i+1]:
            return i + 1
    return 1

def match_fee_rule(tx_context, rule):
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List) - Wildcard if empty/None
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List) - Wildcard if empty/None
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay - Wildcard if None
    if rule.get('capture_delay'):
        if rule['capture_delay'] != tx_context['capture_delay']:
            return False
            
    # 5. Is Credit (Bool) - Wildcard if None
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 6. ACI (List) - The hypothetical ACI we are testing
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 7. Intracountry (Bool) - Wildcard if None
    if rule.get('intracountry') is not None:
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range) - Wildcard if None
    if rule.get('monthly_volume'):
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        if not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range) - Wildcard if None
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        # Use epsilon for float comparison safety
        if not (min_f - 1e-9 <= tx_context['monthly_fraud_rate'] <= max_f + 1e-9):
            return False
            
    return True

def calculate_fee(amount, rule):
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000.0)

# --- Main Logic ---

# Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023

# 1. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found")

m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

# 2. Calculate Monthly Stats for the Merchant (using ALL transactions for context)
# Filter for merchant and year first
df_merchant_all = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

df_merchant_all['month'] = df_merchant_all['day_of_year'].apply(get_month_from_doy)

# Group by month to get volume and fraud rate context for fee rules
# NOTE: Manual says "Fraud is defined as the ratio of fraudulent volume over total volume."
monthly_stats = {}
for month in range(1, 13):
    month_txs = df_merchant_all[df_merchant_all['month'] == month]
    if len(month_txs) > 0:
        vol = month_txs['eur_amount'].sum()
        # Fraud volume: sum of eur_amount where has_fraudulent_dispute is True
        fraud_vol = month_txs[month_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
        
        fraud_rate = fraud_vol / vol if vol > 0 else 0.0
    else:
        vol = 0.0
        fraud_rate = 0.0
    
    monthly_stats[month] = {'vol': vol, 'fraud': fraud_rate}

# 3. Identify Fraudulent Transactions to "Move"
# We only want to calculate costs for the fraudulent transactions we are hypothetically moving
fraudulent_txs = df_merchant_all[df_merchant_all['has_fraudulent_dispute'] == True].copy()

# 4. Simulate Costs for each ACI
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
aci_costs = {}

# Pre-process fees to ensure types match (optimization)
# (Not strictly necessary but good for safety)

for aci in possible_acis:
    total_cost = 0.0
    valid_aci_for_all = True
    
    for _, tx in fraudulent_txs.iterrows():
        month = tx['month']
        stats = monthly_stats.get(month, {'vol': 0, 'fraud': 0})
        
        # Build Context with the HYPOTHETICAL ACI
        tx_context = {
            'card_scheme': tx['card_scheme'],
            'account_type': m_account_type,
            'mcc': m_mcc,
            'capture_delay': m_capture_delay,
            'is_credit': bool(tx['is_credit']),
            'aci': aci, # The hypothetical ACI
            'intracountry': (tx['issuing_country'] == tx['acquirer_country']),
            'monthly_volume': stats['vol'],
            'monthly_fraud_rate': stats['fraud']
        }
        
        # Find Fee Rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(tx_context, rule):
                matched_rule = rule
                break # Take first match
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_cost += fee
        else:
            # If no rule matches this ACI for this transaction context, 
            # it implies this ACI might not be valid or is extremely expensive (fallback).
            # We assign a high penalty to discourage this choice.
            total_cost += 1e9 
            
    aci_costs[aci] = total_cost

# 5. Determine Preferred Choice
# Find ACI with minimum cost
best_aci = min(aci_costs, key=aci_costs.get)

# Output the result
print(best_aci)