import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m suffixes to float."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100
            
        # Handle k/m suffixes
        multiplier = 1
        if v.endswith('k'):
            multiplier = 1000
            v = v[:-1]
        elif v.endswith('m'):
            multiplier = 1000000
            v = v[:-1]
            
        # Handle ranges (return mean for simple conversion, but specific logic needed for matching)
        if '-' in v:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2 * multiplier
            except:
                pass
                
        try:
            return float(v) * multiplier
        except ValueError:
            return 0.0
    return 0.0

def parse_range_string(range_str):
    """
    Parses strings like '>5', '<3', '3-5', '100k-1m', '7.7%-8.3%'.
    Returns (min_val, max_val).
    """
    if not isinstance(range_str, str):
        return None, None
        
    s = range_str.strip().lower().replace(',', '').replace('%', '')
    
    # Handle suffixes for volume
    multiplier = 1
    if 'k' in s or 'm' in s:
        # This is a bit tricky if mixed, but usually consistent
        # Let's handle specific cases or strip
        pass 

    def parse_val(val_str):
        m = 1
        if 'k' in val_str:
            m = 1000
            val_str = val_str.replace('k', '')
        elif 'm' in val_str:
            m = 1000000
            val_str = val_str.replace('m', '')
        
        try:
            return float(val_str) * m
        except:
            return 0.0

    # Percentage handling: if input had %, we treat values as 0-100 scale or 0-1?
    # The coerce_to_float handles % by dividing by 100. 
    # Let's stick to raw values if % was present, or handle consistently.
    # Actually, let's use a simpler approach: normalize everything to float first.
    
    is_percent = '%' in range_str
    
    if '>' in s:
        val = parse_val(s.replace('>', '').replace('=', ''))
        if is_percent: val /= 100
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', '').replace('=', ''))
        if is_percent: val /= 100
        return float('-inf'), val
    elif '-' in s:
        parts = s.split('-')
        v1 = parse_val(parts[0])
        v2 = parse_val(parts[1])
        if is_percent:
            v1 /= 100
            v2 /= 100
        return v1, v2
    elif s == 'immediate':
        return 0, 0 # Special case for capture delay
    elif s == 'manual':
        return 999, 999 # Special case
    else:
        # Exact match treated as range [x, x]
        v = parse_val(s)
        if is_percent: v /= 100
        return v, v

def check_rule_match(transaction_context, rule):
    """
    Checks if a fee rule applies to a transaction context.
    transaction_context: dict containing tx details + merchant stats
    rule: dict from fees.json
    """
    
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != transaction_context['card_scheme']:
        return False
        
    # 2. Account Type (List match, empty=wildcard)
    if rule['account_type']:
        if transaction_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match, empty=wildcard)
    if rule['merchant_category_code']:
        if transaction_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (String match/logic, null=wildcard)
    if rule['capture_delay']:
        # Special handling for string descriptors
        rd = rule['capture_delay']
        td = transaction_context['capture_delay']
        
        if rd == 'manual':
            if td != 'manual': return False
        elif rd == 'immediate':
            if td != 'immediate': return False
        elif rd.startswith('>'):
            # e.g. >5. If td is 'manual', is it >5? Usually manual is long delay.
            # If td is numeric string '1', '7'.
            try:
                limit = float(rd.replace('>', ''))
                if td == 'manual': 
                    # Manual is typically considered long delay, so >5 matches
                    pass 
                elif td == 'immediate':
                    return False
                else:
                    if float(td) <= limit: return False
            except:
                pass
        elif rd.startswith('<'):
            try:
                limit = float(rd.replace('<', ''))
                if td == 'manual': return False
                if td == 'immediate': pass
                else:
                    if float(td) >= limit: return False
            except:
                pass
        elif '-' in rd:
            # e.g. 3-5
            try:
                low, high = map(float, rd.split('-'))
                if td in ['manual', 'immediate']: return False
                val = float(td)
                if not (low <= val <= high): return False
            except:
                pass
                
    # 5. Monthly Fraud Level (Range match, null=wildcard)
    if rule['monthly_fraud_level']:
        min_f, max_f = parse_range_string(rule['monthly_fraud_level'])
        actual_f = transaction_context['monthly_fraud_rate']
        if not (min_f <= actual_f <= max_f):
            return False
            
    # 6. Monthly Volume (Range match, null=wildcard)
    if rule['monthly_volume']:
        min_v, max_v = parse_range_string(rule['monthly_volume'])
        actual_v = transaction_context['monthly_volume']
        if not (min_v <= actual_v <= max_v):
            return False
            
    # 7. Is Credit (Bool match, null=wildcard)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != transaction_context['is_credit']:
            return False
            
    # 8. ACI (List match, empty=wildcard)
    if rule['aci']:
        if transaction_context['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Bool match, null=wildcard)
    if rule['intracountry'] is not None:
        is_intra = (transaction_context['issuing_country'] == transaction_context['acquirer_country'])
        # Rule expects boolean or 0.0/1.0
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    # fee = fixed + (rate * amount / 10000)
    # rate is integer basis points? No, manual says "divided by 10000".
    # e.g. rate 19 -> 19/10000 = 0.0019 = 0.19%
    
    fixed = rule['fixed_amount']
    variable = (rule['rate'] * amount) / 10000
    return fixed + variable

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Get Merchant Metadata
target_merchant = 'Crossfit_Hanna'
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)

if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

print(f"Merchant Info: {merchant_info}")

# 3. Calculate Monthly Stats for October 2023
# October is days 274 to 304 (non-leap year 2023)
oct_start = 274
oct_end = 304

df_oct = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] >= oct_start) &
    (df_payments['day_of_year'] <= oct_end)
]

monthly_volume = df_oct['eur_amount'].sum()
monthly_fraud_count = df_oct['has_fraudulent_dispute'].sum()
monthly_tx_count = len(df_oct)
monthly_fraud_rate = (monthly_fraud_count / monthly_volume) if monthly_volume > 0 else 0 
# Wait, manual says: "fraud levels measured as ratio between monthly total volume and monthly volume notified as fraud"
# Actually, manual says: "ratio between monthly total volume and monthly volume notified as fraud"
# Re-reading manual carefully: "monthly_fraud_level... ratio between monthly total volume and monthly volume notified as fraud"
# Usually fraud rate is count/count or value/value. 
# Let's check the manual text again: "ratio between monthly total volume and monthly volume notified as fraud"
# This phrasing is slightly ambiguous. Usually it's (Fraud Volume / Total Volume).
# Let's calculate Fraud Volume.

fraud_volume = df_oct[df_oct['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate_vol = (fraud_volume / monthly_volume) if monthly_volume > 0 else 0

print(f"October Stats for {target_merchant}:")
print(f"  Total Volume: €{monthly_volume:,.2f}")
print(f"  Fraud Volume: €{fraud_volume:,.2f}")
print(f"  Fraud Rate (Vol/Vol): {monthly_fraud_rate_vol:.4%}")

# 4. Filter Transactions for Day 300
target_day = 300
df_target = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] == target_day)
]

print(f"Found {len(df_target)} transactions for Day {target_day}")

# 5. Calculate Fees
total_fees = 0.0
matched_count = 0
unmatched_count = 0

# Pre-process merchant info for speed/convenience
m_context_base = {
    'account_type': merchant_info['account_type'],
    'merchant_category_code': merchant_info['merchant_category_code'],
    'capture_delay': merchant_info['capture_delay'],
    'monthly_volume': monthly_volume,
    'monthly_fraud_rate': monthly_fraud_rate_vol
}

for _, tx in df_target.iterrows():
    # Build full context
    ctx = m_context_base.copy()
    ctx['card_scheme'] = tx['card_scheme']
    ctx['is_credit'] = tx['is_credit']
    ctx['aci'] = tx['aci']
    ctx['issuing_country'] = tx['issuing_country']
    ctx['acquirer_country'] = tx['acquirer_country']
    
    # Find matching rule
    matched_rule = None
    for rule in fees_data:
        if check_rule_match(ctx, rule):
            matched_rule = rule
            break # Use first matching rule
            
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        # print(f"No rule found for tx: {tx['psp_reference']}")
        unmatched_count += 1

print(f"Calculation Complete.")
print(f"Matched Transactions: {matched_count}")
print(f"Unmatched Transactions: {unmatched_count}")
print(f"Total Fees: €{total_fees:.2f}")

# Final Answer Output
print(f"{total_fees:.2f}")