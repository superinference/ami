import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        # Handle percentages
        if '%' in v:
            v = v.replace('%', '')
            return float(v) / 100.0
        # Handle comparisons (simple stripping for now, logic handled in range parsers)
        v_clean = v.lstrip('><≤≥')
        try:
            return float(v_clean)
        except ValueError:
            return 0.0
    return 0.0

def parse_range_string(range_str):
    """
    Parses strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max).
    Returns (min_val, max_val).
    """
    if not range_str:
        return (float('-inf'), float('inf'))
    
    s = range_str.lower().strip()
    
    # Handle k/m suffixes for volume
    def parse_val(x):
        x = x.strip()
        mult = 1
        if 'k' in x:
            mult = 1000
            x = x.replace('k', '')
        elif 'm' in x:
            mult = 1000000
            x = x.replace('m', '')
        
        if '%' in x:
            return float(x.replace('%', '')) / 100.0 * mult
        return float(x) * mult

    try:
        if '-' in s:
            parts = s.split('-')
            return (parse_val(parts[0]), parse_val(parts[1]))
        elif s.startswith('>'):
            val = parse_val(s[1:])
            return (val, float('inf'))
        elif s.startswith('<'):
            val = parse_val(s[1:])
            return (float('-inf'), val)
        else:
            # Exact value treated as point
            val = parse_val(s)
            return (val, val)
    except:
        return (float('-inf'), float('inf'))

def match_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay (e.g., '1') against rule (e.g., '<3')."""
    if rule_delay is None:
        return True
    
    # Exact string matches
    if merchant_delay == rule_delay:
        return True
    
    # Numeric logic
    # Merchant delays: '1', '2', '7', 'immediate', 'manual'
    # Rule delays: '<3', '3-5', '>5', 'immediate', 'manual'
    
    if merchant_delay in ['immediate', 'manual']:
        return merchant_delay == rule_delay
    
    # If merchant delay is numeric string
    try:
        delay_days = float(merchant_delay)
        if rule_delay == '<3':
            return delay_days < 3
        elif rule_delay == '>5':
            return delay_days > 5
        elif rule_delay == '3-5':
            return 3 <= delay_days <= 5
    except ValueError:
        pass
        
    return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain:
        - card_scheme, is_credit, aci, intracountry (from transaction)
        - account_type, mcc, capture_delay (from merchant data)
        - monthly_volume, monthly_fraud_rate (calculated stats)
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay (Custom logic)
    if not match_capture_delay(tx_context['capture_delay'], rule.get('capture_delay')):
        return False

    # 5. Is Credit (Boolean match, None = wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 6. ACI (List match, Empty/None = wildcard)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Boolean match, None = wildcard)
    if rule.get('intracountry') is not None:
        # Intracountry in rule is 0.0 (False) or 1.0 (True) usually, or boolean
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range_string(rule['monthly_volume'])
        # Note: monthly_volume in context should be in Euros
        if not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range_string(rule['monthly_fraud_level'])
        # Note: monthly_fraud_rate in context is a ratio (0.0 to 1.0)
        # Rule parser handles % conversion
        if not (min_f <= tx_context['monthly_fraud_rate'] <= max_f):
            return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Define Target
target_merchant = 'Rafa_AI'
target_year = 2023
target_day = 12

# 3. Get Merchant Profile
merchant_profile = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_profile:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

print(f"Merchant Profile for {target_merchant}:")
print(f"  Account Type: {merchant_profile['account_type']}")
print(f"  MCC: {merchant_profile['merchant_category_code']}")
print(f"  Capture Delay: {merchant_profile['capture_delay']}")

# 4. Calculate Monthly Stats (January 2023)
# Day 12 is in January. We need stats for the full month to determine applicable fees.
# Assuming "monthly" refers to the calendar month of the transaction.
jan_txs = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) & 
    (df_payments['day_of_year'] <= 31) # January
]

monthly_volume = jan_txs['eur_amount'].sum()
fraud_volume = jan_txs[jan_txs['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

print(f"\nMonthly Stats (Jan 2023):")
print(f"  Volume: €{monthly_volume:,.2f}")
print(f"  Fraud Volume: €{fraud_volume:,.2f}")
print(f"  Fraud Rate: {monthly_fraud_rate:.4%}")

# 5. Filter Target Transactions (Day 12)
day_txs = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) & 
    (df_payments['day_of_year'] == target_day)
]

print(f"\nTransactions on Day {target_day}: {len(day_txs)}")

# 6. Find Applicable Fee IDs
applicable_fee_ids = set()

for _, tx in day_txs.iterrows():
    # Build context for this transaction
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    context = {
        'card_scheme': tx['card_scheme'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': is_intracountry,
        'account_type': merchant_profile['account_type'],
        'mcc': merchant_profile['merchant_category_code'],
        'capture_delay': merchant_profile['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Check against all rules
    # Note: In reality, usually the *first* matching rule applies, or specific priority logic exists.
    # However, the question asks "what are the Fee IDs applicable", implying potentially multiple if we consider the set of all transactions.
    # We will collect ALL IDs that match ANY transaction on that day.
    
    for rule in fees_data:
        if match_fee_rule(context, rule):
            applicable_fee_ids.add(rule['ID'])

# 7. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
print("\nApplicable Fee IDs:")
print(sorted_ids)