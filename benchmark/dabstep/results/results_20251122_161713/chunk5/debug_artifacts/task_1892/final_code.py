import pandas as pd
import json
import re

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

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
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean if forced, but usually handled by parsers
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
    """Parses range strings like '100k-1m', '>5', '<3', '7.7%-8.3%'."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle k/m suffixes
    def parse_val(v):
        v = v.strip()
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1000000
            v = v.replace('m', '')
        
        is_pct = '%' in v
        v = v.replace('%', '')
        
        try:
            val = float(v) * mult
            if is_pct:
                val /= 100
            return val
        except:
            return None

    if '>' in s:
        val = parse_val(s.replace('>', ''))
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        return float('-inf'), val
    elif '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            return parse_val(parts[0]), parse_val(parts[1])
            
    # Exact value or fallback
    val = parse_val(s)
    return val, val

def match_fee_rule(tx_ctx, rule):
    """
    Checks if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False

    # 2. Account Type (List match, empty list = wildcard)
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match, empty list = wildcard)
    if rule.get('merchant_category_code'):
        if tx_ctx.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match, None = wildcard)
    if rule.get('is_credit') is not None:
        # Ensure boolean comparison
        rule_credit = bool(rule['is_credit'])
        tx_credit = bool(tx_ctx.get('is_credit'))
        if rule_credit != tx_credit:
            return False

    # 5. ACI (List match, empty list = wildcard)
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match, None = wildcard)
    if rule.get('intracountry') is not None:
        # Intracountry means Issuer Country == Acquirer Country
        is_intra = tx_ctx.get('issuing_country') == tx_ctx.get('acquirer_country')
        # Handle string '0.0'/'1.0' or float 0.0/1.0 in JSON
        rule_intra_val = rule['intracountry']
        if isinstance(rule_intra_val, str):
            rule_intra = (float(rule_intra_val) == 1.0)
        else:
            rule_intra = (float(rule_intra_val) == 1.0)
            
        if rule_intra != is_intra:
            return False

    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        vol = tx_ctx.get('monthly_volume', 0)
        if min_v is not None and vol < min_v:
            return False
        if max_v is not None and vol > max_v:
            return False

    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        fraud = tx_ctx.get('monthly_fraud_level', 0)
        if min_f is not None and fraud < min_f:
            return False
        if max_f is not None and fraud > max_f:
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
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Setup Target Variables
target_merchant = 'Belles_cookbook_store'
target_fee_id = 384
new_rate = 99

# 3. Get Merchant Profile
merchant_profile = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_profile:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

merchant_account_type = merchant_profile.get('account_type')
merchant_mcc = merchant_profile.get('merchant_category_code')

# 4. Get Fee Rule
fee_rule = next((f for f in fees if f['ID'] == target_fee_id), None)
if not fee_rule:
    raise ValueError(f"Fee ID {target_fee_id} not found in fees.json")

old_rate = fee_rule['rate']

# 5. Filter Transactions for March 2023
# March 2023 (non-leap): Jan(31) + Feb(28) = 59. March 1 is Day 60. March 31 is Day 90.
march_txs = df[
    (df['merchant'] == target_merchant) &
    (df['year'] == 2023) &
    (df['day_of_year'] >= 60) &
    (df['day_of_year'] <= 90)
].copy()

# 6. Calculate Monthly Stats for Matching
# Manual: "Monthly volumes and rates are computed always in natural months"
# We use the stats from the filtered month (March) to determine rule applicability.
monthly_volume = march_txs['eur_amount'].sum()
monthly_fraud_count = march_txs['has_fraudulent_dispute'].sum()
monthly_tx_count = len(march_txs)
monthly_fraud_level = (monthly_fraud_count / monthly_tx_count) if monthly_tx_count > 0 else 0.0

# 7. Identify Matching Transactions and Calculate Affected Volume
affected_volume = 0.0
matching_count = 0

for _, tx in march_txs.iterrows():
    # Build context for this specific transaction
    tx_context = {
        'card_scheme': tx['card_scheme'],
        'account_type': merchant_account_type,
        'mcc': merchant_mcc,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'issuing_country': tx['issuing_country'],
        'acquirer_country': tx['acquirer_country'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    if match_fee_rule(tx_context, fee_rule):
        affected_volume += tx['eur_amount']
        matching_count += 1

# 8. Calculate Delta
# Delta = (New Fee - Old Fee)
# Fee = Fixed + (Rate * Amount / 10000)
# Delta = (Fixed + NewRate*Amt/10000) - (Fixed + OldRate*Amt/10000)
# Delta = (NewRate - OldRate) * Amt / 10000
rate_diff = new_rate - old_rate
delta = rate_diff * affected_volume / 10000

# 9. Output
print(f"Merchant: {target_merchant}")
print(f"Month: March 2023")
print(f"Fee ID: {target_fee_id}")
print(f"Old Rate: {old_rate}")
print(f"New Rate: {new_rate}")
print(f"Matching Transactions: {matching_count}")
print(f"Affected Volume: {affected_volume:.2f}")
print(f"Delta: {delta:.14f}")