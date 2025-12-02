import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

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

def parse_range_string(range_str):
    """Parses strings like '100k-1m', '>5', '<3', '0.0%-0.1%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle percentages
    is_pct = '%' in s
    if is_pct:
        s = s.replace('%', '')
        
    # Handle k/m suffixes
    def parse_val(v):
        v = v.strip()
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1_000_000
            v = v.replace('m', '')
        try:
            val = float(v) * mult
            return val / 100 if is_pct else val
        except:
            return 0.0

    if '>' in s:
        return parse_val(s.replace('>', '')), float('inf')
    if '<' in s:
        return float('-inf'), parse_val(s.replace('<', ''))
    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    
    # Exact match treated as range
    val = parse_val(s)
    return val, val

def check_range(value, range_str):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True
    min_v, max_v = parse_range_string(range_str)
    if min_v is None: 
        return True
    return min_v <= value <= max_v

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != tx_ctx.get('card_scheme'):
        return False

    # 2. Is Credit (Bool match or Wildcard)
    # rule['is_credit'] can be True, False, or None
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 3. Merchant Category Code (List containment or Wildcard)
    # rule['merchant_category_code'] is a list of ints
    if rule.get('merchant_category_code'):
        if tx_ctx.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Account Type (List containment or Wildcard)
    # rule['account_type'] is a list of strings
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 5. ACI (List containment or Wildcard)
    # rule['aci'] is a list of strings
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Bool match or Wildcard)
    # rule['intracountry'] can be 1.0 (True), 0.0 (False), or None
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx.get('intracountry'):
            return False

    # 7. Capture Delay (String match/Range or Wildcard)
    if rule.get('capture_delay'):
        r_delay = rule['capture_delay']
        t_delay = str(tx_ctx.get('capture_delay'))
        
        # If rule is a range (contains <, >, -), try numeric comparison
        if any(c in r_delay for c in ['<', '>', '-']):
            try:
                # Only convert transaction delay to float if it looks numeric
                # 'manual' and 'immediate' will fail float conversion
                delay_val = float(t_delay)
                if not check_range(delay_val, r_delay):
                    return False
            except ValueError:
                # Transaction delay is text (e.g. 'manual'), rule is numeric range
                # They don't match
                return False
        else:
            # Direct string match (e.g. 'manual' == 'manual')
            if r_delay != t_delay:
                return False

    # 8. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        if not check_range(tx_ctx.get('monthly_volume'), rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_ctx.get('monthly_fraud_rate'), rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Rate is typically in basis points or similar, formula: fixed + rate * amount / 10000
    return fixed + (rate * amount / 10000.0)

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
base_path = '/output/chunk5/data/context/'
df_payments = pd.read_csv(base_path + 'payments.csv')
with open(base_path + 'fees.json', 'r') as f:
    fees_data = json.load(f)
with open(base_path + 'merchant_data.json', 'r') as f:
    merchant_data = json.load(f)

# 2. Preprocessing

# Merchant Lookup
merchant_lookup = {m['merchant']: m for m in merchant_data}

# Add Month
# Assuming 2023 non-leap year
df_payments['date'] = pd.to_datetime(df_payments['day_of_year'], unit='D', origin='2022-12-31')
df_payments['month'] = df_payments['date'].dt.month

# 3. Calculate Merchant Monthly Stats
# Calculate fraud amount per transaction
df_payments['fraud_amount'] = df_payments['eur_amount'] * df_payments['has_fraudulent_dispute']

# Group by merchant and month to get total volume and fraud volume
monthly_stats = df_payments.groupby(['merchant', 'month']).agg(
    total_volume=('eur_amount', 'sum'),
    fraud_volume=('fraud_amount', 'sum')
).reset_index()

# Calculate fraud rate (ratio of fraud volume to total volume)
monthly_stats['fraud_rate'] = monthly_stats['fraud_volume'] / monthly_stats['total_volume']
monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0.0)

# Create lookup: (merchant, month) -> stats
stats_lookup = monthly_stats.set_index(['merchant', 'month']).to_dict('index')

# 4. Filter Target Transactions
# "For credit transactions... TransactPlus"
target_txs = df_payments[
    (df_payments['card_scheme'] == 'TransactPlus') & 
    (df_payments['is_credit'] == True)
].copy()

# 5. Simulate Fees
simulated_fees = []
target_amount = 1234.0

# Filter fees to relevant scheme/credit type to speed up
relevant_fees = [
    r for r in fees_data 
    if r['card_scheme'] == 'TransactPlus' 
    and (r['is_credit'] is True or r['is_credit'] is None)
]

count_matched = 0
count_unmatched = 0

for _, tx in target_txs.iterrows():
    merchant = tx['merchant']
    month = tx['month']
    
    m_data = merchant_lookup.get(merchant, {})
    stats = stats_lookup.get((merchant, month), {'total_volume': 0, 'fraud_rate': 0})
    
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    ctx = {
        'card_scheme': 'TransactPlus',
        'is_credit': True,
        'mcc': m_data.get('merchant_category_code'),
        'account_type': m_data.get('account_type'),
        'aci': tx['aci'],
        'intracountry': is_intracountry,
        'capture_delay': m_data.get('capture_delay'),
        'monthly_volume': stats['total_volume'],
        'monthly_fraud_rate': stats['fraud_rate']
    }
    
    matched_rule = None
    # Iterate through fees to find the first match
    for rule in relevant_fees:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        fee = calculate_fee(target_amount, matched_rule)
        simulated_fees.append(fee)
        count_matched += 1
    else:
        count_unmatched += 1

# 6. Calculate Average
if simulated_fees:
    avg_fee = sum(simulated_fees) / len(simulated_fees)
    print(f"{avg_fee:.14f}")
else:
    print("No matching fee rules found.")