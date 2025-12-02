import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------

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
        return float(v)
    return float(value)

def parse_range(range_str):
    """Parses a range string (e.g., '100k-1m', '>5', '<3') into (min, max)."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).lower().strip()
    
    def parse_val(x):
        x = x.strip()
        mult = 1
        if x.endswith('k'):
            mult = 1000
            x = x[:-1]
        elif x.endswith('m'):
            mult = 1000000
            x = x[:-1]
        elif x.endswith('%'):
            mult = 0.01
            x = x[:-1]
        return float(x) * mult

    if '>' in s:
        val = parse_val(s.replace('>', ''))
        return (val, float('inf')) # >X means (X, inf)
    if '<' in s:
        val = parse_val(s.replace('<', ''))
        return (-float('inf'), val) # <X means (-inf, X)
    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    
    try:
        val = parse_val(s)
        return (val, val)
    except:
        return (-float('inf'), float('inf'))

def get_month_from_doy(doy):
    """Maps day of year (1-365) to month (1-12) for non-leap year."""
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cum_days = [0] + list(np.cumsum(days_in_months))
    for i in range(12):
        if doy <= cum_days[i+1]:
            return i + 1
    return 12

def match_fee_rule(ctx, rule):
    """Checks if a transaction context matches a fee rule."""
    
    # 1. Account Type (List)
    if rule.get('account_type') and ctx['account_type'] not in rule['account_type']:
        return False
        
    # 2. Merchant Category Code (List)
    if rule.get('merchant_category_code') and ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 3. ACI (List)
    if rule.get('aci') and ctx['aci'] not in rule['aci']:
        return False
        
    # 4. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 5. Intracountry (Bool/Float -> Bool)
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != ctx['intracountry']:
            return False

    # 6. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= ctx['monthly_volume'] <= max_v):
            return False

    # 7. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # Note: ctx['monthly_fraud_rate'] is 0.08 for 8%
        # parse_range handles '%' conversion
        if not (min_f <= ctx['monthly_fraud_rate'] <= max_f):
            return False

    # 8. Capture Delay (String or Range)
    if rule.get('capture_delay'):
        r_delay = str(rule['capture_delay']).lower()
        m_delay = str(ctx['capture_delay']).lower()
        
        # If rule is a range/comparison
        if any(c in r_delay for c in ['<', '>', '-']):
            # Convert merchant delay to float if possible
            val = 0.0
            if m_delay == 'immediate':
                val = 0.0
            elif m_delay == 'manual':
                # Manual usually doesn't match numeric ranges unless specified
                return False 
            else:
                try:
                    val = float(m_delay)
                except:
                    return False
            
            min_d, max_d = parse_range(r_delay)
            if not (min_d <= val <= max_d):
                return False
        else:
            # Exact string match
            if r_delay != m_delay:
                return False

    return True

# ---------------------------------------------------------
# Main Execution
# ---------------------------------------------------------

# 1. Load Data
merchant_data = pd.read_json('/output/chunk6/data/context/merchant_data.json')
fees_data = pd.read_json('/output/chunk6/data/context/fees.json')
payments = pd.read_csv('/output/chunk6/data/context/payments.csv')

# 2. Identify 'H' Merchants
h_merchants_df = merchant_data[merchant_data['account_type'] == 'H']
h_merchant_names = set(h_merchants_df['merchant'].unique())
merchant_info_map = merchant_data.set_index('merchant').to_dict('index')

# 3. Calculate Monthly Stats for ALL merchants
# (Volume and Fraud Rate are calculated per merchant per month across ALL their transactions)
payments['month'] = payments['day_of_year'].apply(get_month_from_doy)

# Calculate fraud volume (sum of eur_amount where has_fraudulent_dispute is True)
# We use a lambda or pre-filter. Pre-calculating fraud column is faster.
payments['fraud_amt'] = np.where(payments['has_fraudulent_dispute'], payments['eur_amount'], 0.0)

monthly_stats = payments.groupby(['merchant', 'month']).agg(
    total_vol=('eur_amount', 'sum'),
    fraud_vol=('fraud_amt', 'sum')
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_vol'] / monthly_stats['total_vol']
monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0.0)

# Create lookup dictionary for fast access
stats_lookup = monthly_stats.set_index(['merchant', 'month']).to_dict('index')

# 4. Filter Target Transactions (NexPay + Account H)
target_txs = payments[
    (payments['merchant'].isin(h_merchant_names)) & 
    (payments['card_scheme'] == 'NexPay')
].copy()

# 5. Filter Fee Rules (NexPay only)
nexpay_fees = fees_data[fees_data['card_scheme'] == 'NexPay'].to_dict('records')

# 6. Calculate Fees for 100 EUR
calculated_fees = []
hypothetical_amount = 100.0

for _, tx in target_txs.iterrows():
    merchant = tx['merchant']
    month = tx['month']
    
    # Retrieve Context
    m_info = merchant_info_map.get(merchant)
    stats = stats_lookup.get((merchant, month), {'total_vol': 0, 'fraud_rate': 0})
    
    ctx = {
        'account_type': m_info['account_type'],
        'mcc': m_info['merchant_category_code'],
        'capture_delay': m_info['capture_delay'],
        'monthly_volume': stats['total_vol'],
        'monthly_fraud_rate': stats['fraud_rate'],
        'aci': tx['aci'],
        'is_credit': tx['is_credit'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country']
    }
    
    # Find Matching Rule
    # We iterate through rules and pick the first one that matches.
    # (Assuming rules are ordered or mutually exclusive enough for this analysis)
    matched_rule = None
    for rule in nexpay_fees:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Fee Formula: Fixed + (Rate * Amount / 10000)
        # Rate is an integer (basis points equivalent? No, usually rate/10000 implies basis points)
        # Manual says: "rate * transaction_value / 10000"
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * hypothetical_amount / 10000.0)
        calculated_fees.append(fee)

# 7. Output Result
if calculated_fees:
    avg_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"{avg_fee:.6f}")
else:
    print("0.000000")