import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        # Handle percentages
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100.0
            except:
                pass
        # Handle k/m suffixes
        if v.lower().endswith('k'):
            try:
                return float(v[:-1]) * 1000
            except:
                pass
        if v.lower().endswith('m'):
            try:
                return float(v[:-1]) * 1000000
            except:
                pass
        # Handle comparison operators for direct conversion if needed (though usually handled in range check)
        v_clean = v.lstrip('><≤≥=')
        try:
            return float(v_clean)
        except:
            pass
    return None

def check_range(rule_value, actual_value):
    """
    Checks if actual_value fits into rule_value range string.
    rule_value examples: '100k-1m', '>5', '<3', '7.7%-8.3%', '>8.3%'
    """
    if rule_value is None:
        return True
    if actual_value is None:
        return False
        
    s = str(rule_value).strip()
    
    # Handle ranges with '-'
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            if min_val is not None and max_val is not None:
                return min_val <= actual_value <= max_val
    
    # Handle inequalities
    if s.startswith('>='):
        val = coerce_to_float(s[2:])
        return actual_value >= val if val is not None else False
    elif s.startswith('>'):
        val = coerce_to_float(s[1:])
        return actual_value > val if val is not None else False
    elif s.startswith('<='):
        val = coerce_to_float(s[2:])
        return actual_value <= val if val is not None else False
    elif s.startswith('<'):
        val = coerce_to_float(s[1:])
        return actual_value < val if val is not None else False
        
    # Handle exact match (rare for these fields but possible)
    val = coerce_to_float(s)
    if val is not None:
        return actual_value == val
        
    return False

def get_month_from_doy(doy, year=2023):
    """Returns month (1-12) from day of year."""
    # 2023 is not a leap year
    cumulative_days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
    for i, d in enumerate(cumulative_days):
        if doy <= d:
            return i
    return 12 # Should not happen for valid doy

def calculate_fee(transaction, fee_rules, merchant_context, monthly_stats):
    """
    Calculates fee for a single transaction based on the first matching rule.
    
    merchant_context: dict containing 'account_type', 'capture_delay', 'mcc'
    monthly_stats: dict containing 'volume' and 'fraud_rate' for the transaction's month
    """
    
    # Extract transaction details
    tx_card_scheme = transaction['card_scheme']
    tx_is_credit = transaction['is_credit']
    tx_aci = transaction['aci']
    tx_amount = transaction['eur_amount']
    
    # Determine intracountry
    # "True if the issuer country and the acquiring country are the same"
    is_intracountry = (transaction['issuing_country'] == transaction['acquirer_country'])
    
    # Extract context
    m_account_type = merchant_context['account_type']
    m_capture_delay = merchant_context['capture_delay']
    m_mcc = merchant_context['mcc']
    
    # Extract monthly stats
    month_vol = monthly_stats['volume']
    month_fraud = monthly_stats['fraud_rate']
    
    for rule in fee_rules:
        # 1. Card Scheme (Exact match required usually, or wildcard if not present)
        if rule.get('card_scheme') and rule['card_scheme'] != tx_card_scheme:
            continue
            
        # 2. Account Type (List match or Wildcard)
        # rule['account_type'] is a list. If not empty, merchant's type must be in it.
        if rule.get('account_type') and m_account_type not in rule['account_type']:
            continue
            
        # 3. Capture Delay (Exact match or Wildcard)
        if rule.get('capture_delay') and rule['capture_delay'] != m_capture_delay:
            continue
            
        # 4. Merchant Category Code (List match or Wildcard)
        # rule['merchant_category_code'] is a list of ints.
        if rule.get('merchant_category_code') and m_mcc not in rule['merchant_category_code']:
            continue
            
        # 5. Is Credit (Bool match or Wildcard)
        if rule.get('is_credit') is not None and rule['is_credit'] != tx_is_credit:
            continue
            
        # 6. ACI (List match or Wildcard)
        if rule.get('aci') and tx_aci not in rule['aci']:
            continue
            
        # 7. Intracountry (Bool match or Wildcard)
        if rule.get('intracountry') is not None:
            # Compare bools. Note: 0.0/1.0 in JSON might be loaded as floats
            rule_intra = bool(rule['intracountry'])
            if rule_intra != is_intracountry:
                continue
                
        # 8. Monthly Volume (Range match or Wildcard)
        if not check_range(rule.get('monthly_volume'), month_vol):
            continue
            
        # 9. Monthly Fraud Level (Range match or Wildcard)
        if not check_range(rule.get('monthly_fraud_level'), month_fraud):
            continue
            
        # MATCH FOUND
        fixed = rule.get('fixed_amount', 0.0)
        rate = rule.get('rate', 0.0)
        
        # Fee formula: fixed + rate * amount / 10000
        fee = fixed + (rate * tx_amount / 10000.0)
        return fee
        
    return 0.0 # Should not happen if there's a default rule, but safe fallback

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk6/data/context/payments.csv'
merchant_path = '/output/chunk6/data/context/merchant_data.json'
fees_path = '/output/chunk6/data/context/fees.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Filter for Target Merchant and Year
target_merchant = 'Crossfit_Hanna'
df_target = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == 2023)
].copy()

# 3. Get Merchant Attributes
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

original_mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
# Add month column
df_target['month'] = df_target['day_of_year'].apply(get_month_from_doy)

monthly_stats = {}
for month in range(1, 13):
    df_month = df_target[df_target['month'] == month]
    if len(df_month) == 0:
        monthly_stats[month] = {'volume': 0.0, 'fraud_rate': 0.0}
        continue
        
    vol = df_month['eur_amount'].sum()
    # Fraud rate is ratio of fraudulent volume over total volume? 
    # Manual says: "Fraud is defined as the ratio of fraudulent volume over total volume." (Section 7)
    # Wait, let's check Section 5: "monthly_fraud_level... ratio between monthly total volume and monthly volume notified as fraud."
    # Actually, usually it's count or volume. Let's re-read carefully.
    # Section 5: "monthly_fraud_level... rule that specifies the fraud levels measured as ratio between monthly total volume and monthly volume notified as fraud."
    # This phrasing is slightly ambiguous ("ratio between A and B" usually means A/B or B/A).
    # "For example '7.7%-8.3%'".
    # Standard industry practice is Fraud Volume / Total Volume.
    # Let's check the data types. 'has_fraudulent_dispute' is boolean.
    # Let's calculate Fraud Volume / Total Volume.
    
    fraud_vol = df_month[df_month['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = fraud_vol / vol if vol > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': vol,
        'fraud_rate': fraud_rate
    }

# 5. Calculate Fees for Both Scenarios
total_fee_original = 0.0
total_fee_new = 0.0

# Contexts
context_original = {
    'account_type': account_type,
    'capture_delay': capture_delay,
    'mcc': original_mcc
}

context_new = {
    'account_type': account_type,
    'capture_delay': capture_delay,
    'mcc': 5411  # The hypothetical new MCC
}

# Iterate transactions
for _, tx in df_target.iterrows():
    month = tx['month']
    stats = monthly_stats[month]
    
    # Calculate Original Fee
    fee_orig = calculate_fee(tx, fees_data, context_original, stats)
    total_fee_original += fee_orig
    
    # Calculate New Fee
    fee_new = calculate_fee(tx, fees_data, context_new, stats)
    total_fee_new += fee_new

# 6. Calculate Delta
delta = total_fee_new - total_fee_original

# Output result
print(f"{delta:.14f}")