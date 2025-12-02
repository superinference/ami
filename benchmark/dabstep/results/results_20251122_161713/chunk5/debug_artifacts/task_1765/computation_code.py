import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
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

def parse_value_with_suffix(s):
    """Parses strings like '100k', '1m', '8.3%' into floats."""
    s = str(s).lower().strip()
    multiplier = 1.0
    if s.endswith('k'):
        multiplier = 1000.0
        s = s[:-1]
    elif s.endswith('m'):
        multiplier = 1000000.0
        s = s[:-1]
    elif s.endswith('%'):
        multiplier = 0.01
        s = s[:-1]
    
    try:
        return float(s) * multiplier
    except ValueError:
        return 0.0

def parse_range(range_str):
    """Parses strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if not range_str:
        return -float('inf'), float('inf')
    
    s = str(range_str).strip().lower()
    
    if '>' in s:
        val = parse_value_with_suffix(s.replace('>', ''))
        return val, float('inf')
    elif '<' in s:
        val = parse_value_with_suffix(s.replace('<', ''))
        return -float('inf'), val
    elif '-' in s:
        parts = s.split('-')
        return parse_value_with_suffix(parts[0]), parse_value_with_suffix(parts[1])
    else:
        # Exact match treated as min=max
        val = parse_value_with_suffix(s)
        return val, val

def match_fee_rule(tx_context, rule):
    """
    Checks if a fee rule applies to a specific transaction context.
    tx_context must contain: card_scheme, account_type, capture_delay, mcc, 
                             monthly_volume, monthly_fraud_rate, is_credit, aci, intracountry
    """
    # 1. Card Scheme (Exact Match)
    if rule.get('card_scheme') != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List Match - Merchant Level)
    # Rule has list of types (e.g., ['F', 'H']). Context has single type 'F'.
    if rule.get('account_type') and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. Capture Delay (String/Range Match - Merchant Level)
    if rule.get('capture_delay'):
        rd = str(rule['capture_delay'])
        cd = str(tx_context['capture_delay'])
        # If exact string match (e.g., "manual" == "manual"), it passes
        if rd != cd:
            # If not exact match, check if it's a numeric range condition vs categorical
            if rd in ['manual', 'immediate'] or cd in ['manual', 'immediate']:
                return False # Mismatch on categorical values
            else:
                # Try numeric range comparison
                try:
                    days = float(cd)
                    min_d, max_d = parse_range(rd)
                    if not (min_d <= days <= max_d):
                        return False
                except ValueError:
                    return False # Cannot compare non-numeric strings

    # 4. Merchant Category Code (List Match - Merchant Level)
    if rule.get('merchant_category_code') and tx_context['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 5. Monthly Volume (Range Match - Merchant Level)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False
            
    # 6. Monthly Fraud Level (Range Match - Merchant Level)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_context['monthly_fraud_rate'] <= max_f):
            return False

    # 7. Is Credit (Bool Match - Transaction Level)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 8. ACI (List Match - Transaction Level)
    if rule.get('aci') and tx_context['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry (Bool Match - Transaction Level)
    if rule.get('intracountry') is not None:
        # JSON might have 0.0/1.0, convert to bool
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_context['intracountry']:
            return False
            
    return True

# ==========================================
# MAIN EXECUTION
# ==========================================

# File Paths
payments_path = '/output/chunk5/data/context/payments.csv'
merchant_path = '/output/chunk5/data/context/merchant_data.json'
fees_path = '/output/chunk5/data/context/fees.json'

# Load Data
df = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Configuration
target_merchant = 'Crossfit_Hanna'
target_year = 2023
start_day = 60  # March 1st (approx)
end_day = 90    # March 31st (approx)

# 1. Filter Transactions for Merchant and Timeframe
df_march = df[
    (df['merchant'] == target_merchant) & 
    (df['year'] == target_year) & 
    (df['day_of_year'] >= start_day) & 
    (df['day_of_year'] <= end_day)
].copy()

if df_march.empty:
    print("No transactions found for this merchant in the specified period.")
    exit()

# 2. Get Merchant Static Data
m_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not m_info:
    print(f"Merchant {target_merchant} not found in merchant_data.json")
    exit()

# 3. Calculate Monthly Stats (Volume and Fraud Rate)
# Manual: "Monthly volumes and rates are computed always in natural months"
total_volume = df_march['eur_amount'].sum()
fraud_volume = df_march[df_march['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

print(f"--- Monthly Stats for {target_merchant} (March 2023) ---")
print(f"Total Volume: €{total_volume:,.2f}")
print(f"Fraud Rate: {fraud_rate:.2%}")
print(f"MCC: {m_info['merchant_category_code']}")
print(f"Account Type: {m_info['account_type']}")
print(f"Capture Delay: {m_info['capture_delay']}")
print("-" * 50)

# 4. Identify Unique Transaction Profiles
# We group by the fields that vary per transaction to efficiently match rules
df_march['intracountry'] = df_march['issuing_country'] == df_march['acquirer_country']
unique_txs = df_march[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates()

applicable_ids = set()

# 5. Match Rules
for _, row in unique_txs.iterrows():
    # Build the full context for this transaction profile
    ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': m_info['account_type'],
        'capture_delay': m_info['capture_delay'],
        'mcc': m_info['merchant_category_code'],
        'monthly_volume': total_volume,
        'monthly_fraud_rate': fraud_rate,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry']
    }
    
    # Check against every rule in fees.json
    for rule in fees_data:
        if match_fee_rule(ctx, rule):
            applicable_ids.add(rule['ID'])

# 6. Output Results
result_list = sorted(list(applicable_ids))
print(f"Applicable Fee IDs: {', '.join(map(str, result_list))}")