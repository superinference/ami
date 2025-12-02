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
        except ValueError:
            return 0.0
    return float(value)

def parse_vol_str(s):
    """Parse volume strings like '100k', '1m' to float."""
    if not isinstance(s, str): return float(s)
    s = s.lower().replace('€', '').replace(',', '').strip()
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
        return 0.0

def is_not_empty(val):
    """Check if list/array is not empty/None."""
    if val is None: return False
    if isinstance(val, list): return len(val) > 0
    return False

def match_fee_rule(ctx, rule):
    """
    Check if a fee rule applies to a transaction context.
    ctx: dict containing transaction details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (Wildcard: [] or None)
    if is_not_empty(rule.get('account_type')) and ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. MCC (Wildcard: [] or None)
    if is_not_empty(rule.get('merchant_category_code')) and ctx['merchant_category_code'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (Wildcard: None)
    if rule.get('capture_delay'):
        rd = str(rule['capture_delay'])
        cd = str(ctx['capture_delay'])
        
        if rd in ['manual', 'immediate']:
            if rd != cd: return False
        elif rd.startswith('>'):
            try:
                val = float(rd[1:])
                c_val = float(cd) if cd.replace('.','',1).isdigit() else -1
                if not (c_val > val): return False
            except: return False
        elif rd.startswith('<'):
            try:
                val = float(rd[1:])
                c_val = float(cd) if cd.replace('.','',1).isdigit() else 999
                if not (c_val < val): return False
            except: return False
        elif '-' in rd:
            try:
                low, high = map(float, rd.split('-'))
                c_val = float(cd) if cd.replace('.','',1).isdigit() else -1
                if not (low <= c_val <= high): return False
            except: return False
        else:
            if rd != cd: return False

    # 5. Monthly Fraud Level (Wildcard: None)
    if rule.get('monthly_fraud_level'):
        rf = rule['monthly_fraud_level']
        cf = ctx['monthly_fraud_level'] * 100 # Convert ratio to percentage for comparison
        
        if rf.startswith('>'):
            val = float(rf[1:].replace('%',''))
            if not (cf > val): return False
        elif rf.startswith('<'):
            val = float(rf[1:].replace('%',''))
            if not (cf < val): return False
        elif '-' in rf:
            parts = rf.split('-')
            low = float(parts[0].replace('%',''))
            high = float(parts[1].replace('%',''))
            if not (low <= cf <= high): return False

    # 6. Monthly Volume (Wildcard: None)
    if rule.get('monthly_volume'):
        rv = rule['monthly_volume']
        cv = ctx['monthly_volume']
        
        if '-' in rv:
            parts = rv.split('-')
            low = parse_vol_str(parts[0])
            high = parse_vol_str(parts[1])
            if not (low <= cv <= high): return False
        elif rv.startswith('>'):
            val = parse_vol_str(rv[1:])
            if not (cv > val): return False
        elif rv.startswith('<'):
            val = parse_vol_str(rv[1:])
            if not (cv < val): return False

    # 7. Is Credit (Wildcard: None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False

    # 8. ACI (Wildcard: [] or None)
    if is_not_empty(rule.get('aci')) and ctx['aci'] not in rule['aci']:
        return False

    # 9. Intracountry (Wildcard: None)
    if rule.get('intracountry') is not None:
        r_intra = bool(rule['intracountry'])
        if r_intra != ctx['intracountry']:
            return False

    return True

# --- Main Execution ---

# 1. Load Data
payments = pd.read_csv('/output/chunk3/data/context/payments.csv')
merchant_data = pd.read_json('/output/chunk3/data/context/merchant_data.json')
with open('/output/chunk3/data/context/fees.json', 'r') as f:
    fees = json.load(f)

# 2. Identify Account Type F Merchants
merchants_f = merchant_data[merchant_data['account_type'] == 'F']
target_merchants = set(merchants_f['merchant'].unique())
merchant_attrs = merchants_f.set_index('merchant').to_dict('index')

# 3. Calculate Monthly Stats for Target Merchants
# Filter payments for these merchants to calculate stats
df_stats = payments[payments['merchant'].isin(target_merchants)].copy()
# Convert day_of_year to month (2023)
df_stats['month'] = pd.to_datetime(df_stats['day_of_year'], unit='D', origin='2022-12-31').dt.month

# Calculate Monthly Volume
monthly_vol = df_stats.groupby(['merchant', 'month'])['eur_amount'].sum().reset_index()
monthly_vol.rename(columns={'eur_amount': 'vol_total'}, inplace=True)

# Calculate Monthly Fraud Volume
fraud_txs = df_stats[df_stats['has_fraudulent_dispute'] == True]
monthly_fraud = fraud_txs.groupby(['merchant', 'month'])['eur_amount'].sum().reset_index()
monthly_fraud.rename(columns={'eur_amount': 'vol_fraud'}, inplace=True)

# Merge Stats
stats = pd.merge(monthly_vol, monthly_fraud, on=['merchant', 'month'], how='left')
stats['vol_fraud'] = stats['vol_fraud'].fillna(0)
stats['fraud_ratio'] = stats['vol_fraud'] / stats['vol_total']
stats_map = stats.set_index(['merchant', 'month']).to_dict('index')

# 4. Filter Transactions for Analysis
# We need TransactPlus transactions for Account Type F merchants
df_analysis = payments[
    (payments['merchant'].isin(target_merchants)) & 
    (payments['card_scheme'] == 'TransactPlus')
].copy()

# Add derived columns
df_analysis['intracountry'] = df_analysis['issuing_country'] == df_analysis['acquirer_country']
df_analysis['month'] = pd.to_datetime(df_analysis['day_of_year'], unit='D', origin='2022-12-31').dt.month

# 5. Calculate Fees
calculated_fees = []

# Filter fees for TransactPlus and Account Type F (optimization)
relevant_fees = [
    f for f in fees 
    if f['card_scheme'] == 'TransactPlus' and 
    (not f['account_type'] or 'F' in f['account_type'])
]
# Sort by ID to ensure deterministic matching order (assuming lower ID = higher priority)
relevant_fees.sort(key=lambda x: x['ID'])

for _, tx in df_analysis.iterrows():
    merch = tx['merchant']
    month = tx['month']
    
    # Get Context
    m_info = merchant_attrs.get(merch)
    stat = stats_map.get((merch, month), {'vol_total': 0, 'fraud_ratio': 0})
    
    ctx = {
        'card_scheme': 'TransactPlus',
        'account_type': 'F',
        'merchant_category_code': m_info['merchant_category_code'],
        'capture_delay': m_info['capture_delay'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': tx['intracountry'],
        'monthly_volume': stat['vol_total'],
        'monthly_fraud_level': stat['fraud_ratio']
    }
    
    # Find Rule
    matched_rule = None
    for rule in relevant_fees:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate fee for 1000 EUR
        # fee = fixed + rate * amount / 10000
        fixed = matched_rule['fixed_amount']
        rate = matched_rule['rate']
        fee = fixed + (rate * 1000.0 / 10000.0)
        calculated_fees.append(fee)

# 6. Output Result
if not calculated_fees:
    print("0.000000")
else:
    avg_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"{avg_fee:.6f}")