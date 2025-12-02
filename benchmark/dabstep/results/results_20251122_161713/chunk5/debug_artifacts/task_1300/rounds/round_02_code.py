# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1300
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9152 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

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
        return float(v)
    return float(value)

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
    tx_ctx: dict containing transaction details (mcc, account_type, volume, etc.)
    rule: dict containing the fee rule from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != tx_ctx.get('card_scheme'):
        return False

    # 2. Is Credit (Bool match or Wildcard)
    # rule['is_credit'] can be True, False, or None (applies to both)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 3. Merchant Category Code (List containment or Wildcard)
    if rule.get('merchant_category_code'):
        if tx_ctx.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Account Type (List containment or Wildcard)
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 5. ACI (List containment or Wildcard)
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
    # In merchant_data, it's 'manual', 'immediate', etc.
    # In fees, it can be 'manual' or ranges like '>5'.
    if rule.get('capture_delay'):
        r_delay = rule['capture_delay']
        t_delay = str(tx_ctx.get('capture_delay'))
        
        # Direct string match
        if r_delay == t_delay:
            pass
        # Numeric range check (if merchant has numeric delay)
        elif any(c in r_delay for c in ['<', '>', '-']):
            # Map text delays to numbers if possible, or skip
            # Assuming 'manual' etc don't match numeric ranges unless specified
            try:
                delay_val = float(t_delay)
                if not check_range(delay_val, r_delay):
                    return False
            except ValueError:
                return False # Text delay didn't match text rule
        else:
            if r_delay != t_delay:
                return False

    # 8. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        if not check_range(tx_ctx.get('monthly_volume'), rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        # Fraud level in rule is volume-based ratio
        if not check_range(tx_ctx.get('monthly_fraud_rate'), rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    # Fee = Fixed + (Rate * Amount / 10000)
    fixed = rule.get('fixed_amount', 0)
    rate = rule.get('rate', 0)
    return fixed + (rate * amount / 10000)

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

# 2. Preprocessing & Enrichment

# Convert merchant_data to dict for fast lookup
# Key: merchant_name -> Value: dict of attributes
merchant_lookup = {m['merchant']: m for m in merchant_data}

# Add Month column to payments (Year is 2023)
# 2023 is not a leap year.
df_payments['date'] = pd.to_datetime(df_payments['day_of_year'], unit='D', origin='2022-12-31')
df_payments['month'] = df_payments['date'].dt.month

# 3. Calculate Merchant Monthly Stats (Volume & Fraud)
# These stats determine which fee tier applies.
# Volume = Sum of eur_amount
# Fraud Rate = Sum of eur_amount (where fraud=True) / Total Volume
monthly_stats = df_payments.groupby(['merchant', 'month']).apply(
    lambda x: pd.Series({
        'total_volume': x['eur_amount'].sum(),
        'fraud_volume': x.loc[x['has_fraudulent_dispute'], 'eur_amount'].sum()
    })
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_volume'] / monthly_stats['total_volume']
monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0.0)

# Create a lookup for stats: (merchant, month) -> {vol, fraud_rate}
stats_lookup = monthly_stats.set_index(['merchant', 'month']).to_dict('index')

# 4. Filter Target Transactions
# Question: "For credit transactions... TransactPlus"
target_txs = df_payments[
    (df_payments['card_scheme'] == 'TransactPlus') & 
    (df_payments['is_credit'] == True)
].copy()

print(f"Found {len(target_txs)} TransactPlus credit transactions.")

# 5. Simulate Fees
simulated_fees = []
target_amount = 1234.0

# Pre-filter fees to optimize loop
# We only care about TransactPlus and Credit (or wildcard credit)
relevant_fees = [
    r for r in fees_data 
    if r['card_scheme'] == 'TransactPlus' 
    and (r['is_credit'] is True or r['is_credit'] is None)
]

# Iterate through transactions
for _, tx in target_txs.iterrows():
    merchant = tx['merchant']
    month = tx['month']
    
    # Get Merchant Static Data
    m_data = merchant_lookup.get(merchant, {})
    
    # Get Merchant Monthly Stats
    stats = stats_lookup.get((merchant, month), {'total_volume': 0, 'fraud_rate': 0})
    
    # Build Context
    # Intracountry: Issuer == Acquirer
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    ctx = {
        'card_scheme': 'TransactPlus',
        'is_credit': True, # We filtered for this
        'mcc': m_data.get('merchant_category_code'),
        'account_type': m_data.get('account_type'),
        'aci': tx['aci'],
        'intracountry': is_intracountry,
        'capture_delay': m_data.get('capture_delay'),
        'monthly_volume': stats['total_volume'],
        'monthly_fraud_rate': stats['fraud_rate']
    }
    
    # Find matching rule
    matched_rule = None
    for rule in relevant_fees:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break # Assume first match wins (standard rule engine logic)
            
    if matched_rule:
        fee = calculate_fee(target_amount, matched_rule)
        simulated_fees.append(fee)
    else:
        # Fallback or error handling? 
        # If no rule matches, we can't calculate a fee. 
        # In a real scenario, we'd flag this. For now, we skip.
        pass

# 6. Calculate Average
if simulated_fees:
    avg_fee = sum(simulated_fees) / len(simulated_fees)
    print(f"Average Fee for 1234 EUR: {avg_fee:.14f}")
else:
    print("No matching fee rules found for the transactions.")
