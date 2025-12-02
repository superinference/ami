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
    return float(value)

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    # Handle k/m suffixes
    def parse_val(v):
        v = v.strip()
        mult = 1
        if v.lower().endswith('k'):
            mult = 1000
            v = v[:-1]
        elif v.lower().endswith('m'):
            mult = 1000000
            v = v[:-1]
        elif '%' in v:
            v = v.replace('%', '')
            mult = 0.01
        return float(v) * mult

    try:
        if '-' in range_str:
            parts = range_str.split('-')
            return parse_val(parts[0]), parse_val(parts[1])
        elif range_str.startswith('>'):
            return parse_val(range_str[1:]), float('inf')
        elif range_str.startswith('<'):
            return float('-inf'), parse_val(range_str[1:])
        else:
            val = parse_val(range_str)
            return val, val
    except:
        return None, None

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    # Direct match
    if str(merchant_delay) == str(rule_delay):
        return True
    
    # Logic for ranges
    try:
        delay_days = float(merchant_delay)
        if rule_delay == '<3':
            return delay_days < 3
        elif rule_delay == '3-5':
            return 3 <= delay_days <= 5
        elif rule_delay == '>5':
            return delay_days > 5
    except ValueError:
        # merchant_delay might be 'immediate' or 'manual'
        pass
        
    return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context must contain:
    - card_scheme
    - account_type
    - mcc
    - capture_delay
    - monthly_volume
    - monthly_fraud_rate
    - is_credit
    - aci
    - intracountry
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match)
    # If rule['account_type'] is empty/null, it applies to all.
    # If not empty, merchant's account_type must be in the list.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay (Logic match)
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False

    # 5. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_vol, max_vol = parse_range(rule['monthly_volume'])
        if min_vol is not None:
            if not (min_vol <= tx_context['monthly_volume'] <= max_vol):
                return False

    # 6. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_fraud, max_fraud = parse_range(rule['monthly_fraud_level'])
        if min_fraud is not None:
            # tx_context['monthly_fraud_rate'] is a ratio (e.g. 0.08 for 8%)
            if not (min_fraud <= tx_context['monthly_fraud_rate'] <= max_fraud):
                return False

    # 7. Is Credit (Boolean match)
    # If rule['is_credit'] is None, applies to both.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Convert rule value to bool just in case (0.0 -> False)
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_data_path = '/output/chunk4/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Define Context
target_merchant = 'Belles_cookbook_store'
target_year = 2023
target_day = 200

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

m_account_type = merchant_info.get('account_type')
m_mcc = merchant_info.get('merchant_category_code')
m_capture_delay = merchant_info.get('capture_delay')

# 4. Calculate Monthly Stats (Volume & Fraud)
# Day 200 is in July (July 1 is day 182, July 31 is day 212)
month_start_day = 182
month_end_day = 212

monthly_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= month_start_day) &
    (df_payments['day_of_year'] <= month_end_day)
]

monthly_volume = monthly_txs['eur_amount'].sum()
fraud_volume = monthly_txs[monthly_txs['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 5. Filter Transactions for Day 200
day_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
]

# 6. Find Applicable Fee IDs
applicable_fee_ids = set()

for _, tx in day_txs.iterrows():
    # Construct transaction context
    tx_context = {
        'card_scheme': tx['card_scheme'],
        'account_type': m_account_type,
        'mcc': m_mcc,
        'capture_delay': m_capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country']
    }
    
    # Check against all fee rules
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            applicable_fee_ids.add(rule['ID'])

# 7. Output Result
# Sort IDs for consistent output
sorted_ids = sorted(list(applicable_fee_ids))
print(", ".join(map(str, sorted_ids)))