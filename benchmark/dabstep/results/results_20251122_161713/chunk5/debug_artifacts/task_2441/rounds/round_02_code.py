# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2441
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7901 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- HELPER FUNCTIONS ---
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

def is_not_empty(array):
    """Check if array/list is not empty. Handles numpy arrays safely."""
    if array is None:
        return False
    if hasattr(array, 'size'):  # numpy array
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

def check_range(value, rule_str):
    """Checks if numeric value fits in rule_str (e.g. '>5', '100k-1m', '8.3%')."""
    if rule_str is None:
        return True
    
    s = str(rule_str).strip().lower().replace(',', '')
    
    # Handle percentages
    is_pct = '%' in s
    if is_pct:
        s = s.replace('%', '')
        
    # Handle k/m suffixes
    mult = 1
    if 'k' in s: mult = 1000; s = s.replace('k', '')
    elif 'm' in s: mult = 1000000; s = s.replace('m', '')
    
    try:
        if '-' in s:
            parts = s.split('-')
            low = float(parts[0]) * mult
            high = float(parts[1]) * mult
            if is_pct: low/=100; high/=100
            return low <= value <= high
        elif s.startswith('>'):
            limit = float(s[1:]) * mult
            if is_pct: limit/=100
            return value > limit
        elif s.startswith('<'):
            limit = float(s[1:]) * mult
            if is_pct: limit/=100
            return value < limit
        else:
            target = float(s) * mult
            if is_pct: target/=100
            return value == target
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict representing a fee rule
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False

    # 2. Account Type (List match - wildcard if empty)
    if is_not_empty(rule.get('account_type')):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match - wildcard if empty)
    if is_not_empty(rule.get('merchant_category_code')):
        # Ensure types match (int vs str)
        try:
            mcc_rule = [int(x) for x in rule['merchant_category_code']]
            if int(tx_ctx.get('mcc')) not in mcc_rule:
                return False
        except:
            return False

    # 4. Capture Delay (Mixed match)
    if rule.get('capture_delay'):
        r_cd = str(rule['capture_delay']).lower()
        t_cd = str(tx_ctx.get('capture_delay')).lower()
        
        # Exact string match first (e.g. 'manual', 'immediate')
        if r_cd == t_cd:
            pass 
        # Check if rule is a range/inequality
        elif any(x in r_cd for x in ['<', '>', '-']):
            try:
                t_val = float(t_cd)
                if not check_range(t_val, r_cd):
                    return False
            except ValueError:
                # Tx is 'manual' but rule is numeric range -> No match
                return False
        else:
            # Rule is specific value but didn't match string above
            if r_cd != t_cd:
                return False

    # 5. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_ctx.get('monthly_fraud_percent', 0), rule['monthly_fraud_level']):
            return False

    # 6. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range(tx_ctx.get('monthly_volume', 0), rule['monthly_volume']):
            return False

    # 7. Is Credit (Bool match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 8. ACI (List match)
    if is_not_empty(rule.get('aci')):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 9. Intracountry (Bool match)
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx.get('intracountry'):
            return False

    return True

# --- MAIN SCRIPT ---

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

try:
    payments_df = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchant_data = json.load(f)
except FileNotFoundError as e:
    print(f"Error loading files: {e}")
    exit()

# 2. Filter for Rafa_AI in October 2023
target_merchant = 'Rafa_AI'
target_year = 2023
start_day = 274
end_day = 304

df_oct = payments_df[
    (payments_df['merchant'] == target_merchant) &
    (payments_df['year'] == target_year) &
    (payments_df['day_of_year'] >= start_day) &
    (payments_df['day_of_year'] <= end_day)
].copy()

# 3. Get Merchant Attributes
merch_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merch_info:
    print("Merchant not found")
    exit()

# 4. Calculate Monthly Stats (Volume & Fraud)
# Volume in EUR
monthly_vol = df_oct['eur_amount'].sum()
# Fraud Rate (Ratio)
fraud_count = df_oct['has_fraudulent_dispute'].sum()
total_count = len(df_oct)
monthly_fraud_ratio = fraud_count / total_count if total_count > 0 else 0.0

# 5. Get Target Fee Rule (ID 384)
target_rule_id = 384
target_rule = next((r for r in fees_data if r['ID'] == target_rule_id), None)
if not target_rule:
    print(f"Fee ID {target_rule_id} not found")
    exit()

old_rate = target_rule['rate']
new_rate = 1

# 6. Identify Affected Transactions
# Iterate through transactions and find which ones resolve to ID 384 as the FIRST matching rule.
affected_amount_sum = 0.0
count_affected = 0

# Pre-calculate merchant context parts that don't change per transaction
base_ctx = {
    'account_type': merch_info['account_type'],
    'mcc': merch_info['merchant_category_code'],
    'capture_delay': merch_info['capture_delay'],
    'monthly_volume': monthly_vol,
    'monthly_fraud_percent': monthly_fraud_ratio
}

for _, tx in df_oct.iterrows():
    # Build full context
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    
    ctx = base_ctx.copy()
    ctx.update({
        'card_scheme': tx['card_scheme'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': is_intra
    })
    
    # Find winning rule
    winning_rule_id = None
    for rule in fees_data:
        if match_fee_rule(ctx, rule):
            winning_rule_id = rule['ID']
            break # First match wins
            
    if winning_rule_id == target_rule_id:
        affected_amount_sum += tx['eur_amount']
        count_affected += 1

# 7. Calculate Delta
# Fee = Fixed + (Rate * Amount / 10000)
# Delta = New_Fee - Old_Fee
#       = (Fixed + New_Rate*Amt/10000) - (Fixed + Old_Rate*Amt/10000)
#       = (New_Rate - Old_Rate) * Amt / 10000

delta = (new_rate - old_rate) * affected_amount_sum / 10000

# Output result with high precision
print(f"{delta:.14f}")
