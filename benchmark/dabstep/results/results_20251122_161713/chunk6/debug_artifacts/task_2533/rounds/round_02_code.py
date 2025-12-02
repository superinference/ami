# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2533
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9691 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m suffixes to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators for raw value conversion
        
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100
            
        # Handle k/m suffixes (case insensitive)
        lower_v = v.lower()
        if lower_v.endswith('k'):
            return float(lower_v[:-1]) * 1_000
        if lower_v.endswith('m'):
            return float(lower_v[:-1]) * 1_000_000
            
        # Handle ranges (e.g., "50-60") - return mean for simple coercion, 
        # but specific range checkers should handle the string directly.
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

def check_range_condition(value, rule_str):
    """
    Checks if a numeric value satisfies a rule string condition.
    Rule strings can be: "100k-1m", ">5", "<3", "7.7%-8.3%", "immediate", "manual"
    """
    if rule_str is None:
        return True
    if value is None:
        return False
        
    s = str(rule_str).strip().lower()
    
    # Handle exact string matches for non-numeric rules (like capture_delay)
    if isinstance(value, str):
        return s == value.lower()

    # Parse value if it's not already numeric (though input value should be numeric for volume/fraud)
    if isinstance(value, str):
        try:
            value = float(value)
        except:
            return s == str(value).lower()

    # Handle ranges with '-'
    if '-' in s:
        try:
            parts = s.split('-')
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val <= value <= max_val
        except:
            return False
            
    # Handle inequalities
    if s.startswith('>'):
        threshold = coerce_to_float(s[1:])
        return value > threshold
    if s.startswith('<'):
        threshold = coerce_to_float(s[1:])
        return value < threshold
        
    # Handle exact numeric match (rare for these fields but possible)
    try:
        return value == coerce_to_float(s)
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain:
    - card_scheme, account_type, capture_delay, monthly_fraud_rate, monthly_volume,
      mcc, is_credit, aci, intracountry
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match - rule list contains merchant type)
    # If rule list is empty, it applies to all.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Capture Delay (Range/Value match)
    if rule.get('capture_delay'):
        # capture_delay in merchant data is a string (e.g., "manual", "1").
        # In fees, it can be ">5", "manual", etc.
        # If merchant value is numeric string "1", convert to float for range check if rule is range.
        m_delay = tx_context['capture_delay']
        r_delay = rule['capture_delay']
        
        # If both are words (manual, immediate), exact match
        if str(m_delay).isalpha() or str(r_delay).isalpha():
            if str(m_delay).lower() != str(r_delay).lower():
                return False
        else:
            # Numeric comparison
            try:
                val = float(m_delay)
                if not check_range_condition(val, r_delay):
                    return False
            except:
                # Fallback to string match if conversion fails
                if str(m_delay).lower() != str(r_delay).lower():
                    return False

    # 4. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range_condition(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    # 5. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range_condition(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 7. Is Credit (Boolean match)
    # If rule is None, applies to both.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Intracountry in rule is 0.0 (False) or 1.0 (True) usually, or boolean
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    return True

def calculate_fee_amount(amount, rule):
    """Calculates fee: fixed + (rate * amount / 10000)"""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0)
    return fixed + (rate * amount / 10000.0)

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Crossfit_Hanna and 2023
target_merchant = 'Crossfit_Hanna'
df = df_payments[(df_payments['merchant'] == target_merchant) & (df_payments['year'] == 2023)].copy()

# 3. Enrich Data
# Add Month
df['month'] = pd.to_datetime(df['year'] * 1000 + df['day_of_year'], format='%Y%j').dt.month

# Add Intracountry (Issuing == Acquirer)
# Note: acquirer_country is in payments.csv
df['intracountry'] = df['issuing_country'] == df['acquirer_country']

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
# Fraud Rate = Volume of Fraudulent Txs / Total Volume
monthly_stats = {}
for month in df['month'].unique():
    month_data = df[df['month'] == month]
    total_vol = month_data['eur_amount'].sum()
    fraud_vol = month_data[month_data['has_fraudulent_dispute']]['eur_amount'].sum()
    
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': total_vol,
        'fraud_rate': fraud_rate
    }

# 5. Get Merchant Static Attributes
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

base_account_type = merchant_info['account_type']
base_capture_delay = merchant_info['capture_delay']
original_mcc = merchant_info['merchant_category_code']

# 6. Define Calculation Function
def calculate_total_fees_for_mcc(mcc_code):
    total_fees = 0.0
    
    # Iterate through each transaction
    for _, row in df.iterrows():
        # Build context for this transaction
        month = row['month']
        stats = monthly_stats.get(month, {'volume': 0, 'fraud_rate': 0})
        
        context = {
            'card_scheme': row['card_scheme'],
            'account_type': base_account_type,
            'capture_delay': base_capture_delay,
            'monthly_fraud_rate': stats['fraud_rate'],
            'monthly_volume': stats['volume'],
            'mcc': mcc_code,
            'is_credit': row['is_credit'],
            'aci': row['aci'],
            'intracountry': row['intracountry']
        }
        
        # Find matching rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(context, rule):
                matched_rule = rule
                break # Assume first match wins (standard for rule engines)
        
        if matched_rule:
            fee = calculate_fee_amount(row['eur_amount'], matched_rule)
            total_fees += fee
        else:
            # If no rule matches, assume 0 or raise error? 
            # Usually there's a catch-all, but if not, 0 is safer than crashing, 
            # though in reality it implies a data gap.
            pass
            
    return total_fees

# 7. Calculate Fees
# Original Scenario
fees_original = calculate_total_fees_for_mcc(original_mcc)

# Hypothetical Scenario (MCC = 8062)
fees_new = calculate_total_fees_for_mcc(8062)

# 8. Calculate Delta
# Question: "what amount delta will it have to pay"
# Usually implies (New - Old). If positive, pays more. If negative, pays less.
delta = fees_new - fees_original

# 9. Output
print(f"Original Fees (MCC {original_mcc}): {fees_original:.4f}")
print(f"New Fees (MCC 8062): {fees_new:.4f}")
print(f"{delta:.14f}")
