# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1289
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9503 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np
import re

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', ''))
            
        # Handle k/m suffixes
        lower_v = v.lower()
        if 'k' in lower_v:
            return float(lower_v.replace('k', '')) * 1000
        if 'm' in lower_v:
            return float(lower_v.replace('m', '')) * 1000000
            
        # Handle ranges (return mean, though usually handled by range parsers)
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
    """Parses a range string like '100k-1m', '>5', '<3' into (min, max)."""
    if not range_str:
        return -float('inf'), float('inf')
    
    s = str(range_str).strip()
    
    # Handle percentage ranges specifically if needed, but coerce_to_float handles the number part
    is_percent = '%' in s
    
    if s.startswith('>'):
        val = coerce_to_float(s)
        return val, float('inf')
    elif s.startswith('<'):
        val = coerce_to_float(s)
        return -float('inf'), val
    elif '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            return coerce_to_float(parts[0]), coerce_to_float(parts[1])
            
    # Exact match treated as range [val, val]
    val = coerce_to_float(s)
    return val, val

def check_rule_match(tx_context, rule):
    """
    Checks if a fee rule applies to a transaction context.
    tx_context: dict containing transaction/merchant details
    rule: dict containing fee rule criteria
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Is Credit (Handle boolean/None logic)
    # If rule['is_credit'] is None, it applies to both. If set, must match.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 3. Account Type (List match)
    # If rule list is empty/None, matches all. Else tx value must be in list.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 4. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean/None match)
    # Note: fees.json uses 0.0/1.0 for boolean or null.
    if rule.get('intracountry') is not None:
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_context['is_intracountry']:
            return False

    # 7. Capture Delay (String match)
    if rule.get('capture_delay'):
        # Handle exact string matches or simple logic if needed
        # merchant_data has 'manual', 'immediate', '1', etc.
        # fees.json has 'manual', 'immediate', '>5', etc.
        r_delay = str(rule['capture_delay'])
        t_delay = str(tx_context['capture_delay'])
        
        if r_delay == t_delay:
            pass # Match
        elif r_delay.startswith('>'):
            # Simple numeric check if both are numbers
            try:
                r_val = float(r_delay[1:])
                t_val = float(t_delay)
                if not (t_val > r_val): return False
            except:
                if r_delay != t_delay: return False
        elif r_delay.startswith('<'):
            try:
                r_val = float(r_delay[1:])
                t_val = float(t_delay)
                if not (t_val < r_val): return False
            except:
                if r_delay != t_delay: return False
        elif '-' in r_delay:
             # Range check
             try:
                 min_d, max_d = parse_range(r_delay)
                 t_val = float(t_delay)
                 if not (min_d <= t_val <= max_d): return False
             except:
                 if r_delay != t_delay: return False
        else:
            if r_delay != t_delay: return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        # Rule is likely percentage string e.g. ">8.3%"
        # Context fraud level should be percentage (0-100)
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_context['monthly_fraud_level'] <= max_f):
            return False

    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# Convert merchant data to dict for fast lookup
merchant_lookup = {m['merchant']: m for m in merchant_data}

# 2. Pre-calculate Monthly Stats (Volume & Fraud) for Merchants
# Create month column (2023 is not leap year)
df['date'] = pd.to_datetime(df['year'] * 1000 + df['day_of_year'], format='%Y%j')
df['month'] = df['date'].dt.month

# Group by merchant and month
monthly_stats = df.groupby(['merchant', 'month']).agg(
    total_volume=('eur_amount', 'sum'),
    fraud_count=('has_fraudulent_dispute', 'sum'),
    tx_count=('psp_reference', 'count')
).reset_index()

# Calculate fraud percentage (0-100 scale to match "8.3%" format in fees.json)
monthly_stats['fraud_pct'] = (monthly_stats['fraud_count'] / monthly_stats['tx_count']) * 100

# Create a lookup key for stats
monthly_stats['lookup_key'] = list(zip(monthly_stats['merchant'], monthly_stats['month']))
stats_lookup = monthly_stats.set_index('lookup_key')[['total_volume', 'fraud_pct']].to_dict('index')

# 3. Filter Target Transactions
# Question: "For credit transactions... GlobalCard"
target_df = df[
    (df['card_scheme'] == 'GlobalCard') & 
    (df['is_credit'] == True)
].copy()

# 4. Enrich Target Transactions
target_df['is_intracountry'] = target_df['issuing_country'] == target_df['acquirer_country']

# 5. Calculate Fees for 1000 EUR
calculated_fees = []
hypothetical_amount = 1000.0

# Filter fees to only GlobalCard/Credit candidates to speed up loop
candidate_fees = [
    f for f in fees_data 
    if f['card_scheme'] == 'GlobalCard' 
    and (f['is_credit'] is None or f['is_credit'] is True)
]

# Sort fees by specificity or ID if needed? 
# Usually first match applies. We'll assume list order in JSON is priority.
# (No explicit priority given, but standard rule engine logic is first match)

for _, row in target_df.iterrows():
    merchant_name = row['merchant']
    month = row['month']
    
    # Get Merchant Static Data
    m_data = merchant_lookup.get(merchant_name, {})
    
    # Get Merchant Monthly Stats
    stats = stats_lookup.get((merchant_name, month))
    if not stats:
        # Fallback if stats missing (shouldn't happen for existing txs)
        vol = 0
        fraud = 0
    else:
        vol = stats['total_volume']
        fraud = stats['fraud_pct']
        
    # Build Context
    context = {
        'card_scheme': 'GlobalCard',
        'is_credit': True,
        'account_type': m_data.get('account_type'),
        'merchant_category_code': m_data.get('merchant_category_code'),
        'aci': row['aci'],
        'is_intracountry': row['is_intracountry'],
        'capture_delay': m_data.get('capture_delay'),
        'monthly_volume': vol,
        'monthly_fraud_level': fraud
    }
    
    # Find Matching Rule
    matched_rule = None
    for rule in candidate_fees:
        if check_rule_match(context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate Fee: Fixed + (Rate * Amount / 10000)
        # Rate is integer basis points usually, or per 10k. 
        # Manual says: "rate ... to be multiplied by the transaction value and divided by 10000"
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * hypothetical_amount / 10000)
        calculated_fees.append(fee)
    else:
        # If no rule matches, we skip or log. 
        # For this exercise, we assume coverage.
        pass

# 6. Calculate Average
if calculated_fees:
    average_fee = np.mean(calculated_fees)
    print(f"{average_fee:.14f}")
else:
    print("No matching fees found")
