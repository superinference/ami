import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

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
        try:
            return float(v)
        except:
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses strings like '100k-1m', '>8.3%', '<3' into (min, max) tuple."""
    if range_str is None:
        return -float('inf'), float('inf')
    
    s = str(range_str).strip().lower().replace(',', '')
    
    # Handle percentages
    is_pct = '%' in s
    s = s.replace('%', '')
    factor = 0.01 if is_pct else 1.0
    
    # Handle k/m suffixes
    def parse_val(v):
        v = v.strip()
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1000000
            v = v.replace('m', '')
        try:
            return float(v) * mult * factor
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif '>' in s:
        val = parse_val(s.replace('>', '').replace('=', ''))
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', '').replace('=', ''))
        return -float('inf'), val
    else:
        # Exact value treated as range [val, val]
        try:
            val = parse_val(s)
            return val, val
        except:
            return -float('inf'), float('inf')

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    m_str = str(merchant_delay).lower()
    r_str = str(rule_delay).lower()
    
    # Exact match covers 'manual'=='manual', 'immediate'=='immediate'
    if m_str == r_str:
        return True
        
    # If rule is 'manual' or 'immediate' and we didn't match exactly, return False
    if r_str in ['manual', 'immediate']:
        return False
        
    # Now handle numeric ranges (e.g. rule '<3', merchant '1' or 'immediate')
    # Map 'immediate' to 0 for numeric comparison
    m_val = 0.0 if m_str == 'immediate' else None
    if m_val is None:
        try:
            m_val = float(m_str)
        except:
            return False # Merchant delay is non-numeric (e.g. 'manual') and didn't match exact rule
            
    # Parse rule range
    try:
        min_d, max_d = parse_range(rule_delay)
        return min_d <= m_val <= max_d
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Is Credit (Boolean or None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 3. Merchant Category Code (List of ints)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Account Type (List of strings)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 5. ACI (List of strings)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean or None)
    if rule.get('intracountry') is not None:
        rule_intra = rule['intracountry']
        rule_intra_bool = False
        if isinstance(rule_intra, str):
            rule_intra_bool = (rule_intra.lower() == 'true' or rule_intra == '1.0')
        elif isinstance(rule_intra, (int, float)):
            rule_intra_bool = bool(rule_intra)
        else:
            rule_intra_bool = bool(rule_intra)
            
        if rule_intra_bool != tx_ctx['intracountry']:
            return False
            
    # 7. Monthly Volume (Range string)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_ctx['monthly_volume'] <= max_v):
            return False
            
    # 8. Monthly Fraud Level (Range string)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_ctx['monthly_fraud_rate'] <= max_f):
            return False

    # 9. Capture Delay (String or Range)
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_ctx['capture_delay'], rule['capture_delay']):
            return False

    return True

# ==========================================
# MAIN SCRIPT
# ==========================================

# 1. Load Data
df_payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
with open('/output/chunk5/data/context/fees.json', 'r') as f:
    fees_data = json.load(f)
with open('/output/chunk5/data/context/merchant_data.json', 'r') as f:
    merchant_data_list = json.load(f)

# 2. Pre-process Merchant Data
merchant_lookup = {}
for m in merchant_data_list:
    merchant_lookup[m['merchant']] = {
        'mcc': m['merchant_category_code'],
        'account_type': m['account_type'],
        'capture_delay': m['capture_delay']
    }

# 3. Pre-process Payments for Monthly Stats
# Convert year/day_of_year to a month index
df_payments['date'] = pd.to_datetime(df_payments['year'] * 1000 + df_payments['day_of_year'], format='%Y%j')
df_payments['month'] = df_payments['date'].dt.month

# Calculate Monthly Volume and Fraud Rate per Merchant
monthly_stats = df_payments.groupby(['merchant', 'month']).agg(
    total_volume=('eur_amount', 'sum'),
    tx_count=('eur_amount', 'count'),
    fraud_count=('has_fraudulent_dispute', 'sum')
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_count'] / monthly_stats['tx_count']

# Create a lookup for stats: (merchant, month) -> {vol, fraud_rate}
stats_lookup = {}
for _, row in monthly_stats.iterrows():
    stats_lookup[(row['merchant'], row['month'])] = {
        'volume': row['total_volume'],
        'fraud_rate': row['fraud_rate']
    }

# 4. Filter Target Transactions
# "For credit transactions... card scheme NexPay"
target_txs = df_payments[
    (df_payments['card_scheme'] == 'NexPay') & 
    (df_payments['is_credit'] == True)
].copy()

# 5. Calculate Fees for Target Transactions
calculated_fees = []
transaction_value = 1000.0  # Fixed value from question

# Filter fee rules to only NexPay to speed up matching
nexpay_fees = [r for r in fees_data if r['card_scheme'] == 'NexPay']

for _, tx in target_txs.iterrows():
    merchant = tx['merchant']
    month = tx['month']
    
    # Get Merchant Static Data
    m_data = merchant_lookup.get(merchant)
    if not m_data:
        continue 
        
    # Get Merchant Dynamic Data (Monthly Stats)
    m_stats = stats_lookup.get((merchant, month))
    if not m_stats:
        m_stats = {'volume': 0, 'fraud_rate': 0}
        
    # Build Transaction Context
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'is_credit': tx['is_credit'],
        'mcc': m_data['mcc'],
        'account_type': m_data['account_type'],
        'capture_delay': m_data['capture_delay'],
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'monthly_volume': m_stats['volume'],
        'monthly_fraud_rate': m_stats['fraud_rate']
    }
    
    # Find Matching Rule
    matched_rule = None
    for rule in nexpay_fees:
        if match_fee_rule(tx_ctx, rule):
            matched_rule = rule
            break # First match wins
            
    if matched_rule:
        # Calculate Fee
        # fee = fixed_amount + rate * transaction_value / 10000
        fixed = matched_rule['fixed_amount']
        rate = matched_rule['rate']
        fee = fixed + (rate * transaction_value / 10000)
        calculated_fees.append(fee)

# 6. Compute Average
if calculated_fees:
    average_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"{average_fee:.14f}")
else:
    print("0.0")