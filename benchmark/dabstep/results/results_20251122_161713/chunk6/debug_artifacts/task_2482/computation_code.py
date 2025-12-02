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
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
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
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if range_str is None:
        return (float('-inf'), float('inf'))
    
    s = str(range_str).strip().lower().replace(',', '')
    
    # Handle percentages
    is_pct = '%' in s
    if is_pct:
        s = s.replace('%', '')
        
    # Handle k/m suffixes
    def parse_val(v):
        v = v.strip()
        mult = 1
        if v.endswith('k'):
            mult = 1000
            v = v[:-1]
        elif v.endswith('m'):
            mult = 1000000
            v = v[:-1]
        try:
            val = float(v)
            return val / 100 if is_pct else val * mult
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif s.startswith('>'):
        return (parse_val(s[1:]), float('inf'))
    elif s.startswith('<'):
        return (float('-inf'), parse_val(s[1:]))
    else:
        # Exact match treated as range [val, val]
        val = parse_val(s)
        return (val, val)

def match_fee_rule(tx_ctx, rule):
    """
    Checks if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction details + monthly stats
    rule: dict representing a fee rule
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False
        
    # 2. Account Type (List match)
    if rule.get('account_type'):
        # Wildcard: empty list matches all
        if len(rule['account_type']) > 0:
            if tx_ctx.get('account_type') not in rule['account_type']:
                return False
            
    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        # Wildcard: empty list matches all
        if len(rule['merchant_category_code']) > 0:
            # Ensure types match (int vs str)
            rule_mccs = [int(x) for x in rule['merchant_category_code']]
            if int(tx_ctx.get('mcc', 0)) not in rule_mccs:
                return False

    # 4. Capture Delay (Range/Exact)
    if rule.get('capture_delay'):
        r_val = str(rule['capture_delay']).lower()
        t_val = str(tx_ctx.get('capture_delay', '')).lower()
        
        if r_val == t_val:
            pass # Match
        elif any(c in r_val for c in ['<', '>', '-']):
            try:
                t_num = float(t_val)
                min_v, max_v = parse_range(r_val)
                if not (min_v <= t_num <= max_v):
                    return False
            except ValueError:
                # t_val is likely 'manual' or 'immediate' and rule is a range
                # Treat as mismatch
                return False
        else:
            return False

    # 5. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_ctx.get('monthly_fraud_rate', 0) <= max_f):
            return False

    # 6. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_ctx.get('monthly_volume', 0) <= max_v):
            return False

    # 7. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 8. ACI (List)
    if rule.get('aci'):
        # Wildcard: empty list matches all
        if len(rule['aci']) > 0:
            if tx_ctx.get('aci') not in rule['aci']:
                return False

    # 9. Intracountry (Bool)
    if rule.get('intracountry') is not None:
        # Intracountry = (Issuer == Acquirer)
        is_intra = (tx_ctx.get('issuing_country') == tx_ctx.get('acquirer_country'))
        # Rule expects True/False/None.
        # If rule is 1.0/0.0 (float from JSON), convert to bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    return True

def get_month(doy):
    """Maps day_of_year to month (1-12) for non-leap year."""
    if doy <= 31: return 1
    if doy <= 59: return 2
    if doy <= 90: return 3
    if doy <= 120: return 4
    if doy <= 151: return 5
    if doy <= 181: return 6
    if doy <= 212: return 7
    if doy <= 243: return 8
    if doy <= 273: return 9
    if doy <= 304: return 10
    if doy <= 334: return 11
    return 12

# --- Main Execution ---

# File paths
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

# Load data
df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# Filter for Crossfit_Hanna in 2023
merchant_name = 'Crossfit_Hanna'
df_hanna = df[(df['merchant'] == merchant_name) & (df['year'] == 2023)].copy()

# Get Merchant Metadata
m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not m_info:
    print(f"Error: Merchant {merchant_name} not found in merchant_data.json")
    exit()

# Pre-calculate Monthly Stats (Volume and Fraud Rate)
df_hanna['month'] = df_hanna['day_of_year'].apply(get_month)

monthly_stats = {}
for month, group in df_hanna.groupby('month'):
    vol = group['eur_amount'].sum()
    fraud_txs = group['has_fraudulent_dispute'].sum()
    total_txs = len(group)
    fraud_rate = (fraud_txs / total_txs) if total_txs > 0 else 0.0
    monthly_stats[month] = {'vol': vol, 'fraud_rate': fraud_rate}

# Identify Target Fee (ID=384)
target_fee_id = 384
target_fee = next((f for f in fees if f['ID'] == target_fee_id), None)

if not target_fee:
    print(f"Error: Fee ID {target_fee_id} not found in fees.json")
    exit()

old_rate = target_fee['rate']
new_rate = 99

# Optimization: Pre-filter fees that match static merchant properties
# This significantly speeds up the inner loop
candidate_fees = []
for fee in fees:
    # Account Type Check
    if fee.get('account_type') and len(fee['account_type']) > 0:
        if m_info['account_type'] not in fee['account_type']:
            continue
    
    # MCC Check
    if fee.get('merchant_category_code') and len(fee['merchant_category_code']) > 0:
        if int(m_info['merchant_category_code']) not in [int(x) for x in fee['merchant_category_code']]:
            continue
            
    # Capture Delay Check (Static part)
    if fee.get('capture_delay'):
        r_val = str(fee['capture_delay']).lower()
        t_val = str(m_info['capture_delay']).lower()
        # If rule is range and merchant is 'manual'/'immediate', it's a mismatch
        if any(c in r_val for c in ['<', '>', '-']) and t_val in ['manual', 'immediate']:
            continue
        # If exact match required (both strings) and they differ
        if not any(c in r_val for c in ['<', '>', '-']) and r_val != t_val:
            continue
            
    candidate_fees.append(fee)

# Calculate Affected Volume
# We must iterate through transactions and find which ones match Fee 384 as their *primary* fee.
affected_volume = 0.0
count_matches = 0

# Iterate by month for efficiency (stats are constant per month)
for month, group in df_hanna.groupby('month'):
    stats = monthly_stats[month]
    
    # Base context for this merchant in this month
    base_ctx = {
        'account_type': m_info['account_type'],
        'mcc': m_info['merchant_category_code'],
        'capture_delay': m_info['capture_delay'],
        'monthly_volume': stats['vol'],
        'monthly_fraud_rate': stats['fraud_rate']
    }
    
    # Convert to dicts for faster iteration
    txs = group.to_dict('records')
    
    for row in txs:
        # Transaction specific context
        tx_ctx = base_ctx.copy()
        tx_ctx.update({
            'card_scheme': row['card_scheme'],
            'is_credit': row['is_credit'],
            'aci': row['aci'],
            'issuing_country': row['issuing_country'],
            'acquirer_country': row['acquirer_country'],
            'eur_amount': row['eur_amount']
        })
        
        # Find the FIRST applicable fee (Waterfall logic)
        applicable_fee_id = None
        for fee in candidate_fees:
            if match_fee_rule(tx_ctx, fee):
                applicable_fee_id = fee['ID']
                break
        
        # Check if the applicable fee is our target fee
        if applicable_fee_id == target_fee_id:
            affected_volume += row['eur_amount']
            count_matches += 1

# Calculate Delta
# Fee = Fixed + (Rate * Amount / 10000)
# Delta = (NewRate - OldRate) * Amount / 10000
delta = (new_rate - old_rate) * affected_volume / 10000

# Output result with high precision
print(f"{delta:.14f}")