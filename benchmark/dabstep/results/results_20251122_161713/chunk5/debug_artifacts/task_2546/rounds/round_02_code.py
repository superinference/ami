# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2546
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9780 characters (FULL CODE)
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
        return float(v)
    return float(value)

def parse_range(range_str):
    """
    Parses a range string like '100k-1m' or '7.7%-8.3%' into a tuple (min, max).
    Returns (None, None) if parsing fails or input is None.
    """
    if not isinstance(range_str, str):
        return None, None
    
    # Handle k/m suffixes
    def parse_val(v):
        v = v.lower().strip()
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1_000_000
            v = v.replace('m', '')
        
        if '%' in v:
            return float(v.replace('%', '')) / 100
        return float(v) * mult

    try:
        if '-' in range_str:
            parts = range_str.split('-')
            return parse_val(parts[0]), parse_val(parts[1])
        elif '>' in range_str:
            val = parse_val(range_str.replace('>', ''))
            return val, float('inf')
        elif '<' in range_str:
            val = parse_val(range_str.replace('<', ''))
            return float('-inf'), val
        else:
            # Exact value or manual handling
            val = parse_val(range_str)
            return val, val
    except:
        return None, None

def get_month_from_day(day_of_year):
    """Returns month (1-12) from day of year (1-365)."""
    # Days in months for non-leap year (2023)
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cumulative_days = [0] + list(np.cumsum(days_in_months))
    
    for i, cutoff in enumerate(cumulative_days[1:], 1):
        if day_of_year <= cutoff:
            return i
    return 12 # Fallback

def match_fee_rule(tx_ctx, rule):
    """
    Checks if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction details + monthly stats
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List match - wildcard [] matches all)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Capture Delay (Exact match - wildcard null matches all)
    if rule.get('capture_delay') is not None:
        if rule['capture_delay'] != tx_ctx['capture_delay']:
            # Handle special cases like '>5' if necessary, but data seems to have discrete strings
            # If rule is '>5' and tx is '7', we might need logic.
            # Checking unique values: rule values are '3-5', '>5', '<3', 'immediate', 'manual'.
            # Tx values are '1', '2', '7', 'immediate', 'manual'.
            rd = rule['capture_delay']
            td = str(tx_ctx['capture_delay'])
            
            match = False
            if rd == td:
                match = True
            elif rd == '>5':
                try:
                    if float(td) > 5: match = True
                except: pass
            elif rd == '<3':
                try:
                    if float(td) < 3: match = True
                except: pass
            elif rd == '3-5':
                try:
                    if 3 <= float(td) <= 5: match = True
                except: pass
            
            if not match:
                return False

    # 4. Merchant Category Code (List match - wildcard [] matches all)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 5. Is Credit (Exact match - wildcard null matches all)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 6. ACI (List match - wildcard [] matches all)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Exact match - wildcard null matches all)
    if rule.get('intracountry') is not None:
        # Rule uses 0.0/1.0/None or boolean?
        # JSON sample shows 0.0, 1.0.
        # tx_ctx['intracountry'] is boolean.
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False

    # 8. Monthly Volume (Range match - wildcard null matches all)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if min_v is not None:
            vol = tx_ctx['monthly_volume']
            if not (min_v <= vol <= max_v):
                return False

    # 9. Monthly Fraud Level (Range match - wildcard null matches all)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if min_f is not None:
            fraud = tx_ctx['monthly_fraud_rate']
            if not (min_f <= fraud <= max_f):
                return False

    return True

def calculate_fee_amount(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0)
    rate = rule.get('rate', 0)
    # Fee = fixed + (rate * amount / 10000)
    return fixed + (rate * amount / 10000)

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

# 1. Load Data
print("Loading data...")
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

# 2. Filter for Merchant and Year
target_merchant = "Martinis_Fine_Steakhouse"
target_year = 2023
print(f"Filtering for {target_merchant} in {target_year}...")

df_tx = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

if df_tx.empty:
    print("No transactions found.")
    exit()

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
if not merchant_info:
    print("Merchant info not found.")
    exit()

original_mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']
new_mcc = 5999

print(f"Merchant Metadata: Original MCC={original_mcc}, New MCC={new_mcc}, Account Type={account_type}, Capture Delay={capture_delay}")

# 4. Calculate Monthly Stats
# Add month column
df_tx['month'] = df_tx['day_of_year'].apply(get_month_from_day)

# Group by month to get volume and fraud stats
monthly_stats = {}
for month in range(1, 13):
    month_txs = df_tx[df_tx['month'] == month]
    if month_txs.empty:
        monthly_stats[month] = {'volume': 0, 'fraud_rate': 0}
        continue
    
    total_vol = month_txs['eur_amount'].sum()
    
    # Fraud volume: sum of amounts where has_fraudulent_dispute is True
    # Note: Manual says "Fraud is defined as the ratio of fraudulent volume over total volume."
    fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0
    
    monthly_stats[month] = {
        'volume': total_vol,
        'fraud_rate': fraud_rate
    }

# 5. Calculate Fees and Delta
total_delta = 0.0
processed_count = 0

# Pre-process fees to avoid repeated parsing if possible, but for now we use the helper
# Optimization: Sort fees? No, just iterate.

print("Calculating fee delta...")

for idx, row in df_tx.iterrows():
    # Build context
    month = row['month']
    stats = monthly_stats[month]
    
    # Intracountry: Issuer == Acquirer
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    base_ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': account_type,
        'capture_delay': capture_delay,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'monthly_volume': stats['volume'],
        'monthly_fraud_rate': stats['fraud_rate']
    }
    
    # --- Scenario 1: Original MCC ---
    ctx_original = base_ctx.copy()
    ctx_original['mcc'] = original_mcc
    
    fee_original = 0
    found_orig = False
    for rule in fees_data:
        if match_fee_rule(ctx_original, rule):
            fee_original = calculate_fee_amount(row['eur_amount'], rule)
            found_orig = True
            break
            
    # --- Scenario 2: New MCC ---
    ctx_new = base_ctx.copy()
    ctx_new['mcc'] = new_mcc
    
    fee_new = 0
    found_new = False
    for rule in fees_data:
        if match_fee_rule(ctx_new, rule):
            fee_new = calculate_fee_amount(row['eur_amount'], rule)
            found_new = True
            break
    
    # Calculate delta for this transaction
    # If a rule isn't found, fee is 0 (or we could flag it, but assuming coverage)
    delta = fee_new - fee_original
    total_delta += delta
    
    processed_count += 1

print(f"Processed {processed_count} transactions.")
print(f"Total Fee Delta: {total_delta:.14f}")
