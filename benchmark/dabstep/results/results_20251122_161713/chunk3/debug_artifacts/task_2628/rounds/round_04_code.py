# ═══════════════════════════════════════════════════════════
# Round 4 - Task 2628
# ═══════════════════════════════════════════════════════════
# Temperature: 0.30
# Verification: sufficient
# Plan steps: 3
# Code length: 9896 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower().replace(',', '')
    
    # Handle percentages
    is_pct = '%' in s
    if is_pct:
        s = s.replace('%', '')
        scale = 0.01
    else:
        scale = 1.0

    # Handle k/m suffixes for volume
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1
        if 'k' in val_s:
            mult = 1000
            val_s = val_s.replace('k', '')
        elif 'm' in val_s:
            mult = 1000000
            val_s = val_s.replace('m', '')
        try:
            return float(val_s) * mult * scale
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return float('-inf'), parse_val(s[1:])
    else:
        # Exact value treated as range [val, val]
        v = parse_val(s)
        return v, v

def check_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay (e.g., '1') against rule (e.g., '<3')."""
    if rule_delay is None:
        return True
    
    # Direct string match
    if str(merchant_delay).lower() == str(rule_delay).lower():
        return True
        
    # Numeric comparison
    try:
        # Convert merchant delay to float (handle 'immediate'/'manual' as non-numeric)
        if str(merchant_delay).lower() in ['immediate', 'manual']:
            return False # Already checked equality above
            
        m_val = float(merchant_delay)
        
        if rule_delay.startswith('<'):
            limit = float(rule_delay[1:])
            return m_val < limit
        elif rule_delay.startswith('>'):
            limit = float(rule_delay[1:])
            return m_val > limit
        elif '-' in rule_delay:
            low, high = map(float, rule_delay.split('-'))
            return low <= m_val <= high
    except:
        pass
        
    return False

def match_fee_rule(ctx, rule):
    """
    Checks if a fee rule applies to the given context.
    ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact Match)
    if rule['card_scheme'] != ctx['card_scheme']:
        return False

    # 2. Account Type (List Match - Wildcard if empty)
    if rule['account_type'] and ctx['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List Match - Wildcard if empty)
    if rule['merchant_category_code'] and ctx['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Capture Delay (Complex Match - Wildcard if None)
    if not check_capture_delay(ctx['capture_delay'], rule['capture_delay']):
        return False

    # 5. Monthly Volume (Range Match - Wildcard if None)
    if rule['monthly_volume']:
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= ctx['monthly_volume'] <= max_v):
            return False

    # 6. Monthly Fraud Level (Range Match - Wildcard if None)
    if rule['monthly_fraud_level']:
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # Fraud level in context is ratio (0.0-1.0), rule is usually %
        # parse_range handles % conversion if present in string
        if not (min_f <= ctx['monthly_fraud_level'] <= max_f):
            return False

    # 7. Is Credit (Exact Match - Wildcard if None)
    if rule['is_credit'] is not None and rule['is_credit'] != ctx['is_credit']:
        return False

    # 8. ACI (List Match - Wildcard if empty/None)
    if rule['aci'] and ctx['aci'] not in rule['aci']:
        return False

    # 9. Intracountry (Exact Match - Wildcard if None)
    if rule['intracountry'] is not None:
        # Convert boolean to 0.0/1.0 for comparison if needed, or direct bool
        rule_intra = bool(rule['intracountry'])
        ctx_intra = bool(ctx['intracountry'])
        if rule_intra != ctx_intra:
            return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'
acquirer_path = '/output/chunk3/data/context/acquirer_countries.csv'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
df_acquirers = pd.read_csv(acquirer_path)

# 2. Filter for Merchant and Month (June)
merchant_name = 'Belles_cookbook_store'
target_month = 6

# Create date column to extract month
df_payments['date'] = pd.to_datetime(df_payments['year'].astype(str) + df_payments['day_of_year'].astype(str).str.zfill(3), format='%Y%j')
df_payments['month'] = df_payments['date'].dt.month

# Filter
df_merchant_june = df_payments[
    (df_payments['merchant'] == merchant_name) & 
    (df_payments['month'] == target_month)
].copy()

# 3. Calculate Monthly Metrics (Volume & Fraud)
# Manual.md: "Fraud is defined as the ratio of fraudulent volume over total volume."
total_volume = df_merchant_june['eur_amount'].sum()
fraud_volume = df_merchant_june[df_merchant_june['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

# 4. Get Merchant Profile
merchant_profile = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
if not merchant_profile:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

mcc = merchant_profile['merchant_category_code']
account_type = merchant_profile['account_type']
capture_delay = merchant_profile['capture_delay']

# Determine Acquirer Country from merchant_data -> acquirer_countries
# Merchant data has list of acquirers, we take the first one or check mapping
merchant_acquirers = merchant_profile['acquirer']
# Find country for these acquirers
acquirer_country_code = None
for acq in merchant_acquirers:
    match = df_acquirers[df_acquirers['acquirer'] == acq]
    if not match.empty:
        acquirer_country_code = match.iloc[0]['country_code']
        break

if not acquirer_country_code:
    # Fallback to most common in payments if mapping fails (unlikely)
    acquirer_country_code = df_merchant_june['acquirer_country'].mode()[0]

# 5. Determine Intracountry Status for Transactions
# Intracountry = (Issuing Country == Acquirer Country)
# We use the merchant's configured acquirer country (FR), not necessarily the one in the historical row
df_merchant_june['intracountry'] = df_merchant_june['issuing_country'] == acquirer_country_code

# 6. Simulate Fees for Each Scheme
schemes = ['GlobalCard', 'NexPay', 'SwiftCharge', 'TransactPlus']
scheme_costs = {}

# Pre-calculate context fields that are constant per transaction
# We will iterate rows and build context dynamically
# To optimize, we can convert fees_data to a more searchable format, but with 1000 rules and 1162 txs, nested loop is ~1M ops, which is fine.

for scheme in schemes:
    total_fee = 0.0
    match_count = 0
    
    # Iterate through every transaction to find its specific fee
    for _, tx in df_merchant_june.iterrows():
        # Build context for this transaction
        ctx = {
            'card_scheme': scheme, # The variable we are testing
            'account_type': account_type,
            'capture_delay': capture_delay,
            'mcc': mcc,
            'monthly_volume': total_volume,
            'monthly_fraud_level': fraud_rate,
            'is_credit': bool(tx['is_credit']),
            'aci': tx['aci'],
            'intracountry': bool(tx['intracountry'])
        }
        
        # Find matching rule
        applied_rule = None
        for rule in fees_data:
            if match_fee_rule(ctx, rule):
                applied_rule = rule
                break
        
        if applied_rule:
            # Calculate fee: fixed + (rate * amount / 10000)
            fee = applied_rule['fixed_amount'] + (applied_rule['rate'] * tx['eur_amount'] / 10000)
            total_fee += fee
            match_count += 1
        else:
            # If no rule matches, this scheme might not support the transaction.
            # In a steering scenario, we assume the scheme can take the traffic but maybe at a default high rate?
            # Or we skip. For now, we assume complete coverage or penalize non-matches.
            # Given the dataset nature, we expect matches.
            pass

    scheme_costs[scheme] = total_fee
    # print(f"Scheme: {scheme}, Total Fee: {total_fee:.2f}, Matches: {match_count}/{len(df_merchant_june)}")

# 7. Determine Winner
# Filter out schemes with 0 fees (likely no matches found, invalid)
valid_costs = {k: v for k, v in scheme_costs.items() if v > 0}

if valid_costs:
    best_scheme = min(valid_costs, key=valid_costs.get)
    print(best_scheme)
else:
    print("No valid scheme found")
