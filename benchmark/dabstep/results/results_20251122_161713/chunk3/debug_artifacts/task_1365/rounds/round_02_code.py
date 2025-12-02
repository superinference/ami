# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1365
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8235 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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
        except:
            return 0.0
    return 0.0

def parse_range_check(value, rule_range_str):
    """
    Checks if 'value' fits into 'rule_range_str'.
    Handles formats like '100k-1m', '>8.3%', '7.7%-8.3%', '<3'.
    """
    if rule_range_str is None:
        return True
    
    s = str(rule_range_str).strip()
    is_pct = '%' in s
    
    # Helper to parse "100k", "1m", "8.3%"
    def parse_val(x):
        x_clean = x.strip().replace('%', '')
        mult = 1
        if x_clean.lower().endswith('k'):
            mult = 1000
            x_clean = x_clean[:-1]
        elif x_clean.lower().endswith('m'):
            mult = 1000000
            x_clean = x_clean[:-1]
        
        try:
            val = float(x_clean)
            if is_pct:
                return val / 100.0
            return val * mult
        except:
            return 0.0

    try:
        if s.startswith('>'):
            limit = parse_val(s[1:])
            return value > limit
        if s.startswith('<'):
            limit = parse_val(s[1:])
            return value < limit
        if '-' in s:
            parts = s.split('-')
            low = parse_val(parts[0])
            high = parse_val(parts[1])
            return low <= value <= high
        
        # Exact match fallback
        return value == parse_val(s)
    except:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay (e.g., 'manual', '1') against rule (e.g., 'manual', '<3')."""
    if rule_delay is None:
        return True
    
    # Direct string match (e.g., 'manual' == 'manual')
    if str(merchant_delay) == str(rule_delay):
        return True
    
    # Numeric comparison
    try:
        days = float(merchant_delay)
        if rule_delay == '<3':
            return days < 3
        if rule_delay == '>5':
            return days > 5
        if rule_delay == '3-5':
            return 3 <= days <= 5
    except:
        pass
        
    return False

def match_fee_rule(tx_ctx, rule):
    """Determines if a fee rule applies to a specific transaction context."""
    # 1. Card Scheme
    if rule.get('card_scheme') != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List - Empty = Wildcard)
    if rule.get('account_type') and tx_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. MCC (List - Empty = Wildcard)
    if rule.get('merchant_category_code') and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay
    if not check_capture_delay(tx_ctx['capture_delay'], rule.get('capture_delay')):
        return False
        
    # 5. Monthly Fraud Level
    if not parse_range_check(tx_ctx['monthly_fraud'], rule.get('monthly_fraud_level')):
        return False
        
    # 6. Monthly Volume
    if not parse_range_check(tx_ctx['monthly_volume'], rule.get('monthly_volume')):
        return False
        
    # 7. Is Credit (Boolean or None)
    if rule.get('is_credit') is not None and rule['is_credit'] != tx_ctx['is_credit']:
        return False
        
    # 8. ACI (List - Empty = Wildcard)
    if rule.get('aci') and tx_ctx['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry (Boolean or None)
    if rule.get('intracountry') is not None:
        # Convert 0.0/1.0 to bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    return True

# --- Main Analysis ---

# 1. Identify MCC
mcc_df = pd.read_csv('/output/chunk3/data/context/merchant_category_codes.csv')
target_description = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"
mcc_row = mcc_df[mcc_df['description'] == target_description]

if mcc_row.empty:
    print(f"Error: MCC not found for description: {target_description}")
    exit()

target_mcc = int(mcc_row.iloc[0]['mcc'])
# print(f"Target MCC: {target_mcc}")

# 2. Identify Target Merchants (Account Type H + MCC 5813)
with open('/output/chunk3/data/context/merchant_data.json') as f:
    merchant_data = json.load(f)

target_merchants = []
merchant_info_map = {}

for m in merchant_data:
    if m['account_type'] == 'H' and m['merchant_category_code'] == target_mcc:
        target_merchants.append(m['merchant'])
        merchant_info_map[m['merchant']] = {
            'account_type': m['account_type'],
            'capture_delay': m['capture_delay'],
            'mcc': m['merchant_category_code']
        }

if not target_merchants:
    print("No merchants found matching criteria.")
    exit()

# 3. Load and Filter Transactions
df = pd.read_csv('/output/chunk3/data/context/payments.csv')
df = df[df['merchant'].isin(target_merchants)]
df = df[df['card_scheme'] == 'GlobalCard']

if df.empty:
    print("No GlobalCard transactions found for target merchants.")
    exit()

# 4. Calculate Merchant Monthly Stats (Volume & Fraud)
# Helper to map day_of_year to month (1-12)
def get_month(day_of_year):
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cum_days = 0
    for i, d in enumerate(days_in_months):
        if day_of_year <= cum_days + d:
            return i + 1
        cum_days += d
    return 12

df['month'] = df['day_of_year'].apply(get_month)

# Group by merchant and month to get stats
monthly_stats = df.groupby(['merchant', 'month']).agg(
    vol=('eur_amount', 'sum'),
    fraud_count=('has_fraudulent_dispute', 'sum'),
    tx_count=('psp_reference', 'count')
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_count'] / monthly_stats['tx_count']

# Create lookup dictionary: (merchant, month) -> {vol, fraud}
stats_lookup = {}
for _, row in monthly_stats.iterrows():
    stats_lookup[(row['merchant'], row['month'])] = {
        'vol': row['vol'],
        'fraud': row['fraud_rate']
    }

# 5. Load Fee Rules
with open('/output/chunk3/data/context/fees.json') as f:
    fees = json.load(f)

# 6. Calculate Hypothetical Fees
hypothetical_fees = []
target_tx_value = 4321.0

for _, tx in df.iterrows():
    merchant = tx['merchant']
    month = tx['month']
    
    # Get context data
    stats = stats_lookup.get((merchant, month))
    m_info = merchant_info_map.get(merchant)
    
    if not stats or not m_info:
        continue
        
    # Build transaction context for rule matching
    ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': m_info['account_type'],
        'mcc': m_info['mcc'],
        'capture_delay': m_info['capture_delay'],
        'monthly_volume': stats['vol'],
        'monthly_fraud': stats['fraud'],
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': tx['acquirer_country'] == tx['issuing_country']
    }
    
    # Find the first matching rule
    matched_rule = None
    for rule in fees:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate fee for 4321 EUR
        # Fee = Fixed + (Rate * Amount / 10000)
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * target_tx_value / 10000.0)
        hypothetical_fees.append(fee)

# 7. Calculate Average and Output
if not hypothetical_fees:
    print("No applicable fee rules found.")
else:
    avg_fee = np.mean(hypothetical_fees)
    print(f"{avg_fee:.6f} EUR")
