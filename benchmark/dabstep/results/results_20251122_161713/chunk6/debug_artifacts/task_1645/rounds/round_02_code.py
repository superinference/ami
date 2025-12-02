# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1645
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10282 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except:
                return 0.0
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

def parse_range(value_str):
    """Parses a string range (e.g., '100k-1m', '>5', '7.7%-8.3%') into (min, max)."""
    if not isinstance(value_str, str):
        return None, None
    
    s = value_str.lower().strip()
    
    # Handle k/m suffixes
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

    try:
        if '-' in s:
            parts = s.split('-')
            return parse_val(parts[0]), parse_val(parts[1])
        elif s.startswith('>'):
            return parse_val(s[1:]), float('inf')
        elif s.startswith('<'):
            return float('-inf'), parse_val(s[1:])
        elif s == 'immediate':
            return 0, 0
        elif s == 'manual':
            return 999, 999 # Treat as very high delay
        else:
            val = parse_val(s)
            return val, val
    except:
        return None, None

def check_rule_match(tx, rule):
    """
    Checks if a transaction matches a fee rule.
    tx: dict containing transaction details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx.get('card_scheme'):
        return False

    # 2. Account Type (List match, empty = wildcard)
    if rule.get('account_type'):
        if tx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match, empty = wildcard)
    if rule.get('merchant_category_code'):
        if tx.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 4. ACI (List match, empty = wildcard)
    if rule.get('aci'):
        if tx.get('aci') not in rule['aci']:
            return False

    # 5. Is Credit (Boolean match, None = wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx.get('is_credit'):
            return False

    # 6. Intracountry (Boolean match, None = wildcard)
    if rule.get('intracountry') is not None:
        # Note: fees.json uses 0.0/1.0 for boolean often, or actual bools
        rule_intra = bool(rule['intracountry'])
        tx_intra = bool(tx.get('intracountry'))
        if rule_intra != tx_intra:
            return False

    # 7. Capture Delay (Range match, None = wildcard)
    if rule.get('capture_delay'):
        min_val, max_val = parse_range(rule['capture_delay'])
        tx_val = tx.get('capture_delay_days')
        
        # Handle 'manual' and 'immediate' mapping logic
        if rule['capture_delay'] == 'manual':
            if tx.get('capture_delay') != 'manual': return False
        elif rule['capture_delay'] == 'immediate':
            if tx.get('capture_delay') != 'immediate': return False
        elif min_val is not None:
            # If tx has specific string value like 'manual', it might not match numeric range
            # We map tx values to numbers for comparison if possible
            tx_num = 0
            if tx.get('capture_delay') == 'immediate': tx_num = 0
            elif tx.get('capture_delay') == 'manual': tx_num = 999
            else:
                try: tx_num = float(tx.get('capture_delay'))
                except: return False # Mismatch type
            
            if not (min_val <= tx_num <= max_val):
                return False

    # 8. Monthly Volume (Range match, None = wildcard)
    if rule.get('monthly_volume'):
        min_val, max_val = parse_range(rule['monthly_volume'])
        if min_val is not None:
            if not (min_val <= tx.get('monthly_volume', 0) <= max_val):
                return False

    # 9. Monthly Fraud Level (Range match, None = wildcard)
    if rule.get('monthly_fraud_level'):
        min_val, max_val = parse_range(rule['monthly_fraud_level'])
        if min_val is not None:
            # Fraud level in rule is ratio (e.g. 0.083), tx has ratio
            if not (min_val <= tx.get('monthly_fraud_level', 0) <= max_val):
                return False

    return True

# ==========================================
# MAIN EXECUTION
# ==========================================

# 1. Load Data
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'
payments_path = '/output/chunk6/data/context/payments.csv'

with open(fees_path, 'r') as f:
    fees_data = json.load(f)

with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
df_merchants = pd.DataFrame(merchant_data)

df_payments = pd.read_csv(payments_path)

# 2. Identify Account Type F Merchants
merchants_f = df_merchants[df_merchants['account_type'] == 'F']
target_merchants = set(merchants_f['merchant'])
# Create lookup for merchant static data
merchant_info = merchants_f.set_index('merchant').to_dict('index')

# 3. Filter Transactions
# Filter for Account Type F merchants AND GlobalCard
df_filtered = df_payments[
    (df_payments['merchant'].isin(target_merchants)) & 
    (df_payments['card_scheme'] == 'GlobalCard')
].copy()

# 4. Calculate Monthly Stats (Volume and Fraud)
# Group by merchant and month (using day_of_year to approximate month or just use month if available? 
# Dataset has 'year', 'day_of_year'. No explicit month.
# We need to map day_of_year to month.
def get_month(day_of_year):
    # Simple approximation for 2023 (non-leap)
    # Jan: 1-31, Feb: 32-59, etc.
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cumulative = 0
    for i, d in enumerate(days_in_months):
        cumulative += d
        if day_of_year <= cumulative:
            return i + 1
    return 12

df_payments['month'] = df_payments['day_of_year'].apply(get_month)

# Calculate stats on the FULL dataset for these merchants (not just GlobalCard txs)
# because volume/fraud tiers usually apply to the merchant's total processing.
df_merchant_txs = df_payments[df_payments['merchant'].isin(target_merchants)].copy()

# Monthly Volume
monthly_vol = df_merchant_txs.groupby(['merchant', 'month'])['eur_amount'].sum().reset_index()
monthly_vol.rename(columns={'eur_amount': 'monthly_volume'}, inplace=True)

# Monthly Fraud Volume
fraud_txs = df_merchant_txs[df_merchant_txs['has_fraudulent_dispute'] == True]
monthly_fraud_vol = fraud_txs.groupby(['merchant', 'month'])['eur_amount'].sum().reset_index()
monthly_fraud_vol.rename(columns={'eur_amount': 'fraud_volume'}, inplace=True)

# Merge and calculate ratio
monthly_stats = pd.merge(monthly_vol, monthly_fraud_vol, on=['merchant', 'month'], how='left')
monthly_stats['fraud_volume'] = monthly_stats['fraud_volume'].fillna(0)
monthly_stats['monthly_fraud_level'] = monthly_stats['fraud_volume'] / monthly_stats['monthly_volume']

# Create lookup dict for stats: (merchant, month) -> {vol, fraud}
stats_lookup = monthly_stats.set_index(['merchant', 'month']).to_dict('index')

# 5. Prepare Filtered Transactions for Fee Calculation
df_filtered['month'] = df_filtered['day_of_year'].apply(get_month)
df_filtered['intracountry'] = df_filtered['issuing_country'] == df_filtered['acquirer_country']

# 6. Calculate Fees
calculated_fees = []
hypothetical_amount = 4321.0

for idx, row in df_filtered.iterrows():
    merchant = row['merchant']
    month = row['month']
    
    # Get merchant static info
    m_info = merchant_info.get(merchant)
    if not m_info: continue
    
    # Get merchant monthly stats
    stats = stats_lookup.get((merchant, month), {'monthly_volume': 0, 'monthly_fraud_level': 0})
    
    # Build transaction context object
    tx_context = {
        'card_scheme': row['card_scheme'],
        'account_type': m_info['account_type'],
        'merchant_category_code': m_info['merchant_category_code'],
        'capture_delay': m_info['capture_delay'],
        'capture_delay_days': m_info['capture_delay'], # Helper handles string/int conversion
        'monthly_volume': stats['monthly_volume'],
        'monthly_fraud_level': stats['monthly_fraud_level'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry']
    }
    
    # Find matching rule
    matched_rule = None
    # Iterate through fees (assuming order matters or first match is sufficient)
    # In this dataset, usually specific rules override general ones, but often first match works if sorted.
    # We will look for the first valid match.
    for rule in fees_data:
        if check_rule_match(tx_context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate fee: fixed + (rate * amount / 10000)
        # Rate is in basis points (per 10,000)
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * hypothetical_amount / 10000.0)
        calculated_fees.append(fee)
    else:
        # If no rule matches, we skip or log. Ideally shouldn't happen for valid data.
        pass

# 7. Average and Output
if calculated_fees:
    average_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"{average_fee:.6f}")
else:
    print("0.000000")
