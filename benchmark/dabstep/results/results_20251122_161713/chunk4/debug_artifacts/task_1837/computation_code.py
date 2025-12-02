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
    if isinstance(value, (int, float, np.number)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100.0
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range_check(value, rule_range):
    """
    Check if a value falls within a rule range string.
    Handles: "100k-1m", ">5", "<3", "7.7%-8.3%", "immediate", "manual"
    """
    if rule_range is None:
        return True
    
    # Exact string match for non-numeric rules (e.g. "manual")
    if isinstance(rule_range, str) and not any(c.isdigit() for c in rule_range):
        return str(value).lower() == rule_range.lower()

    # Convert value to float for numeric comparison
    try:
        num_val = float(value)
    except (ValueError, TypeError):
        # If value is string (e.g. "manual") but rule is numeric/range, it's a mismatch
        return False

    rr = str(rule_range).lower().strip()
    
    # Helper to parse k/m suffixes
    def parse_num(s):
        s = s.replace('%', '')
        mult = 1.0
        if 'k' in s:
            mult = 1000.0
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1000000.0
            s = s.replace('m', '')
        return float(s) * mult

    is_percent = '%' in rr
    
    try:
        if '-' in rr:
            parts = rr.split('-')
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            if is_percent:
                low /= 100.0
                high /= 100.0
            return low <= num_val <= high + 1e-9 # float tolerance
        elif rr.startswith('>'):
            limit = parse_num(rr[1:])
            if is_percent: limit /= 100.0
            return num_val > limit
        elif rr.startswith('<'):
            limit = parse_num(rr[1:])
            if is_percent: limit /= 100.0
            return num_val < limit
        else:
            # Exact match numeric string
            target = parse_num(rr)
            if is_percent: target /= 100.0
            return abs(num_val - target) < 1e-9
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Check if a transaction context matches a fee rule.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False

    # 2. Account Type (List match - rule has list, merchant has value)
    if rule.get('account_type'): # If rule list is not empty/null
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_ctx.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Rule expects boolean or 0.0/1.0
        rule_intra = bool(rule['intracountry'])
        tx_intra = bool(tx_ctx.get('intracountry'))
        if rule_intra != tx_intra:
            return False

    # 7. Capture Delay (Range/String match)
    if rule.get('capture_delay'):
        if not parse_range_check(tx_ctx.get('capture_delay'), rule['capture_delay']):
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_ctx.get('monthly_volume'), rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_ctx.get('monthly_fraud_level'), rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculate fee based on amount and rule."""
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    # Formula: fixed + (rate * amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_data_path = '/output/chunk4/data/context/merchant_data.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant and Time Period (March 2023)
target_merchant = 'Golfclub_Baron_Friso'
# March 2023: Day 60 to 90 (inclusive)
start_doy = 60
end_doy = 90
year = 2023

# Filter for the specific month
df_month = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == year) &
    (df_payments['day_of_year'] >= start_doy) &
    (df_payments['day_of_year'] <= end_doy)
].copy()

if len(df_month) == 0:
    print("0.0")
    exit()

# 3. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found")

mcc = merchant_info.get('merchant_category_code')
account_type = merchant_info.get('account_type')
capture_delay = merchant_info.get('capture_delay')

# 4. Calculate Monthly Metrics
# These determine the fee tier for ALL transactions in the month
monthly_volume = df_month['eur_amount'].sum()
fraud_txs = df_month[df_month['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs['eur_amount'].sum()
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 5. Calculate Fees
total_fees = 0.0
matched_count = 0
unmatched_count = 0

for _, tx in df_month.iterrows():
    # Determine intracountry status
    # True if issuing_country == acquirer_country
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    # Build context
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'mcc': mcc,
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': is_intracountry,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Find first matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_ctx, rule):
            matched_rule = rule
            break
    
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1

# 6. Output Result
print(f"{total_fees:.14f}")