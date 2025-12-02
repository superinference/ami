import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

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
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value) if value is not None else 0.0

def parse_range_check(value, rule_range_str):
    """
    Checks if a numeric value fits within a rule string range.
    Rule examples: "100k-1m", ">5", "<3", "7.7%-8.3%", ">8.3%"
    """
    if rule_range_str is None:
        return True
        
    # Normalize rule string
    s = str(rule_range_str).strip().lower()
    
    # Handle k/m suffixes
    def parse_val(x):
        x = x.replace('%', '')
        if 'k' in x: return float(x.replace('k', '')) * 1000
        if 'm' in x: return float(x.replace('m', '')) * 1000000
        return float(x)

    try:
        # Percentage handling for value (if rule implies percentage)
        check_value = value
        if '%' in s:
            # If rule is percentage, ensure value is comparable (e.g. 0.08 vs 8.0)
            # Usually value is passed as ratio (0.08), rule is "8%"
            # Let's normalize to ratio
            pass 

        if '-' in s:
            parts = s.split('-')
            low = parse_val(parts[0])
            high = parse_val(parts[1])
            # Adjust for percentage if needed
            if '%' in s:
                low /= 100
                high /= 100
            return low <= check_value <= high
            
        if s.startswith('>'):
            limit = parse_val(s[1:])
            if '%' in s: limit /= 100
            return check_value > limit
            
        if s.startswith('<'):
            limit = parse_val(s[1:])
            if '%' in s: limit /= 100
            return check_value < limit
            
        # Exact match (rare for ranges but possible)
        val = parse_val(s)
        if '%' in s: val /= 100
        return check_value == val
        
    except Exception as e:
        # print(f"Error parsing range '{rule_range_str}' for value {value}: {e}")
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_ctx: Dictionary containing transaction and merchant details.
    rule: Dictionary from fees.json.
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False

    # 2. Account Type (List match - Wildcard if empty)
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match - Wildcard if empty)
    if rule.get('merchant_category_code'):
        # Ensure types match (int vs str)
        mcc = tx_ctx.get('merchant_category_code')
        if mcc not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay (Range/Exact match - Wildcard if None)
    if rule.get('capture_delay'):
        # capture_delay in merchant data is string (e.g., "manual", "immediate", "1")
        # rule in fees.json is string (e.g., "manual", ">5", "<3")
        merchant_delay = str(tx_ctx.get('capture_delay'))
        rule_delay = rule['capture_delay']
        
        if rule_delay in ['manual', 'immediate']:
            if merchant_delay != rule_delay:
                return False
        else:
            # Numeric comparison
            try:
                delay_val = float(merchant_delay)
                if not parse_range_check(delay_val, rule_delay):
                    return False
            except ValueError:
                # Merchant delay is non-numeric (e.g. 'manual') but rule is numeric range
                return False

    # 5. Monthly Fraud Level (Range match - Wildcard if None)
    if rule.get('monthly_fraud_level'):
        # tx_ctx['monthly_fraud_rate'] should be a float ratio (e.g., 0.08 for 8%)
        if not parse_range_check(tx_ctx.get('monthly_fraud_rate', 0), rule['monthly_fraud_level']):
            return False

    # 6. Monthly Volume (Range match - Wildcard if None)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_ctx.get('monthly_volume', 0), rule['monthly_volume']):
            return False

    # 7. Is Credit (Boolean match - Wildcard if None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 8. ACI (List match - Wildcard if empty/None)
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 9. Intracountry (Boolean match - Wildcard if None)
    if rule.get('intracountry') is not None:
        # rule['intracountry'] is 0.0 or 1.0 in JSON, convert to bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx.get('intracountry'):
            return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# File paths
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

# 1. Load Data
print("Loading data...")
df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023
target_day = 12

# 2. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

print(f"Merchant Info: {merchant_info}")

# 3. Calculate Monthly Stats (January 2023)
# Filter for January (days 1-31)
jan_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= 1) &
    (df_payments['day_of_year'] <= 31)
]

monthly_volume = jan_txs['eur_amount'].sum()
fraud_count = jan_txs['has_fraudulent_dispute'].sum()
total_count = len(jan_txs)
monthly_fraud_rate = (fraud_count / total_count) if total_count > 0 else 0.0

print(f"Monthly Volume (Jan): {monthly_volume}")
print(f"Monthly Fraud Rate (Jan): {monthly_fraud_rate} ({monthly_fraud_rate*100:.2f}%)")

# 4. Filter for Target Day (Day 12)
day_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
].copy()

print(f"Transactions on Day {target_day}: {len(day_txs)}")

# 5. Match Fees
applicable_fee_ids = set()

# Pre-process merchant info for speed
m_account_type = merchant_info.get('account_type')
m_mcc = merchant_info.get('merchant_category_code')
m_capture_delay = merchant_info.get('capture_delay')

for idx, tx in day_txs.iterrows():
    # Build Transaction Context
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': m_account_type,
        'merchant_category_code': m_mcc,
        'capture_delay': m_capture_delay,
        'monthly_fraud_rate': monthly_fraud_rate,
        'monthly_volume': monthly_volume,
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country']
    }
    
    # Check against all rules
    for rule in fees_data:
        if match_fee_rule(tx_ctx, rule):
            applicable_fee_ids.add(rule['ID'])

# 6. Output Result
# Sort IDs numerically
sorted_ids = sorted(list(applicable_fee_ids))
print("\nApplicable Fee IDs:")
print(", ".join(map(str, sorted_ids)))