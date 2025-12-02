# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1281
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8257 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v:
            return float(v.replace('k', '')) * 1000
        if 'm' in v:
            return float(v.replace('m', '')) * 1000000
        return float(v)
    return 0.0

def parse_range(range_str):
    """Parses strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip()
    
    # Handle percentage ranges
    is_percent = '%' in s
    
    if '-' in s:
        parts = s.split('-')
        min_val = coerce_to_float(parts[0])
        max_val = coerce_to_float(parts[1])
        return min_val, max_val
    elif s.startswith('>'):
        val = coerce_to_float(s[1:])
        return val, float('inf')
    elif s.startswith('<'):
        val = coerce_to_float(s[1:])
        return float('-inf'), val
    else:
        # Exact match treated as range [val, val]
        val = coerce_to_float(s)
        return val, val

def check_rule_match(context, rule):
    """
    Checks if a fee rule applies to a transaction context.
    Context keys: card_scheme, is_credit, account_type, mcc, aci, 
                  intracountry, capture_delay, monthly_volume, monthly_fraud_level
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != context['card_scheme']:
        return False

    # 2. Is Credit (Exact match, handle null as wildcard if necessary, though usually boolean)
    if rule.get('is_credit') is not None and rule['is_credit'] != context['is_credit']:
        return False

    # 3. Account Type (List match, empty = wildcard)
    if rule.get('account_type') and context['account_type'] not in rule['account_type']:
        return False

    # 4. Merchant Category Code (List match, empty = wildcard)
    if rule.get('merchant_category_code') and context['mcc'] not in rule['merchant_category_code']:
        return False

    # 5. ACI (List match, empty = wildcard)
    if rule.get('aci') and context['aci'] not in rule['aci']:
        return False

    # 6. Intracountry (Boolean match, null = wildcard)
    # Rule has 0.0/1.0, Context has True/False. 1.0 == True.
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != context['intracountry']:
            return False

    # 7. Capture Delay (String match, null = wildcard)
    if rule.get('capture_delay') is not None:
        # Handle range-like strings in capture_delay (e.g., '>5')
        # But merchant_data also has 'manual', 'immediate', '1'.
        # If rule is specific value, check equality. If rule is range, check logic.
        # Based on data, rule capture_delay can be '>5', '<3', '3-5', 'immediate', 'manual'.
        # Merchant data has 'manual', 'immediate', '1', '7'.
        r_delay = rule['capture_delay']
        c_delay = str(context['capture_delay'])
        
        if r_delay in ['manual', 'immediate']:
            if r_delay != c_delay:
                return False
        else:
            # Numeric comparison
            try:
                c_val = float(c_delay)
                min_d, max_d = parse_range(r_delay)
                # Adjust bounds for strict inequalities if needed, but simple range check usually suffices
                # For '>5', min=5, max=inf. 7 is > 5.
                # If range was inclusive/exclusive, we might need more logic. 
                # Assuming standard inclusive for range, exclusive for >/<.
                if '>' in r_delay and c_val <= min_d: return False
                if '<' in r_delay and c_val >= max_d: return False
                if '-' in r_delay and not (min_d <= c_val <= max_d): return False
            except ValueError:
                # If conversion fails (e.g. comparing 'manual' to '>5'), no match
                return False

    # 8. Monthly Volume (Range match, null = wildcard)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= context['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range match, null = wildcard)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # Handle strict inequality for fraud if implied, but usually inclusive ranges
        # For '>8.3%', min=0.083.
        if '>' in rule['monthly_fraud_level'] and context['monthly_fraud_level'] <= min_f: return False
        if '<' in rule['monthly_fraud_level'] and context['monthly_fraud_level'] >= max_f: return False
        if '-' in rule['monthly_fraud_level'] and not (min_f <= context['monthly_fraud_level'] <= max_f): return False

    return True

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Create merchant lookup dict
merchant_lookup = {m['merchant']: m for m in merchant_data}

# 2. Preprocessing: Add Month and Calculate Monthly Stats
# Convert day_of_year to month (2023)
df_payments['date'] = pd.to_datetime(df_payments['year'] * 1000 + df_payments['day_of_year'], format='%Y%j')
df_payments['month'] = df_payments['date'].dt.month

# Calculate monthly volume and fraud rate per merchant
monthly_stats = df_payments.groupby(['merchant', 'month']).agg(
    monthly_vol=('eur_amount', 'sum'),
    tx_count=('psp_reference', 'count'),
    fraud_count=('has_fraudulent_dispute', 'sum')
).reset_index()

monthly_stats['monthly_fraud_rate'] = monthly_stats['fraud_count'] / monthly_stats['tx_count']

# Create stats lookup: (merchant, month) -> {vol, fraud}
stats_map = {}
for _, row in monthly_stats.iterrows():
    stats_map[(row['merchant'], row['month'])] = {
        'vol': row['monthly_vol'],
        'fraud': row['monthly_fraud_rate']
    }

# 3. Filter Target Transactions
# GlobalCard, Credit
target_df = df_payments[
    (df_payments['card_scheme'] == 'GlobalCard') & 
    (df_payments['is_credit'] == True)
].copy()

# 4. Calculate Fees for Hypothetical 100 EUR
calculated_fees = []
hypothetical_amount = 100.0

# Sort fees by ID (assuming lower ID has priority, or simply order in file)
fees_data.sort(key=lambda x: x['ID'])

for _, tx in target_df.iterrows():
    merchant = tx['merchant']
    month = tx['month']
    
    # Retrieve Context Data
    m_info = merchant_lookup.get(merchant)
    m_stats = stats_map.get((merchant, month))
    
    if not m_info or not m_stats:
        continue
        
    # Build Context
    context = {
        'card_scheme': tx['card_scheme'],
        'is_credit': tx['is_credit'],
        'account_type': m_info['account_type'],
        'mcc': m_info['merchant_category_code'],
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'capture_delay': m_info['capture_delay'],
        'monthly_volume': m_stats['vol'],
        'monthly_fraud_level': m_stats['fraud']
    }
    
    # Find First Matching Rule
    matched_rule = None
    for rule in fees_data:
        if check_rule_match(context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate Fee
        # Formula: fixed + (rate * amount / 10000)
        fixed = matched_rule['fixed_amount']
        rate = matched_rule['rate']
        
        fee = fixed + (rate * hypothetical_amount / 10000.0)
        calculated_fees.append(fee)

# 5. Compute Average
if calculated_fees:
    average_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"{average_fee:.14f}")
else:
    print("0.00")
