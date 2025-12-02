# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1750
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 8724 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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
        v = v.lstrip('><≤≥')  # Remove comparison operators for simple conversion
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (coerce_to_float_with_suffix(parts[0]) + coerce_to_float_with_suffix(parts[1])) / 2
            except:
                pass
        try:
            return coerce_to_float_with_suffix(v)
        except ValueError:
            return 0.0
    return 0.0

def coerce_to_float_with_suffix(val_str):
    """Helper to handle k/m suffixes."""
    if not isinstance(val_str, str):
        return float(val_str)
    val_str = val_str.strip().lower()
    multiplier = 1
    if val_str.endswith('k'):
        multiplier = 1000
        val_str = val_str[:-1]
    elif val_str.endswith('m'):
        multiplier = 1000000
        val_str = val_str[:-1]
    elif '%' in val_str:
        multiplier = 0.01
        val_str = val_str.replace('%', '')
        
    try:
        return float(val_str) * multiplier
    except ValueError:
        return 0.0

def parse_range_check(value_to_check, rule_string):
    """
    Checks if a numeric value falls within a range string (e.g., '100k-1m', '>5', '<3').
    """
    if rule_string is None:
        return True
    
    s = str(rule_string).lower().strip()
    
    # Handle simple inequalities
    if s.startswith('>'):
        limit = coerce_to_float_with_suffix(s[1:])
        return value_to_check > limit
    if s.startswith('<'):
        limit = coerce_to_float_with_suffix(s[1:])
        return value_to_check < limit
    
    # Handle ranges (e.g., "100k-1m")
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            lower = coerce_to_float_with_suffix(parts[0])
            upper = coerce_to_float_with_suffix(parts[1])
            return lower <= value_to_check <= upper
            
    # Handle exact match
    try:
        val = coerce_to_float_with_suffix(s)
        return value_to_check == val
    except:
        return False

def match_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay against rule."""
    if rule_delay is None:
        return True
    
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    # Exact match
    if m_delay == r_delay:
        return True
        
    # Numeric comparison if merchant delay is numeric (e.g. "1", "2")
    if m_delay.isdigit():
        days = int(m_delay)
        if r_delay.startswith('<'):
            limit = float(r_delay[1:])
            return days < limit
        if r_delay.startswith('>'):
            limit = float(r_delay[1:])
            return days > limit
        if '-' in r_delay:
            parts = r_delay.split('-')
            try:
                lower = float(parts[0])
                upper = float(parts[1])
                return lower <= days <= upper
            except:
                pass
            
    return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match, empty=wildcard)
    if rule.get('account_type') and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List match, empty=wildcard)
    if rule.get('merchant_category_code') and tx_context['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Is Credit (Boolean match, None=wildcard)
    if rule.get('is_credit') is not None and rule['is_credit'] != tx_context['is_credit']:
        return False
        
    # 5. ACI (List match, empty=wildcard)
    if rule.get('aci') and tx_context['aci'] not in rule['aci']:
        return False
        
    # 6. Intracountry (Boolean match, None=wildcard)
    if rule.get('intracountry') is not None:
        # Intracountry in rule is 0.0 or 1.0 (float) or boolean
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
        
    # 7. Monthly Volume (Range match, None=wildcard)
    if rule.get('monthly_volume') is not None:
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 8. Monthly Fraud Level (Range match, None=wildcard)
    if rule.get('monthly_fraud_level') is not None:
        if not parse_range_check(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    # 9. Capture Delay (Complex match, None=wildcard)
    if rule.get('capture_delay') is not None:
        if not match_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False
            
    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Rafa_AI and Year 2023
df_rafa = df_payments[(df_payments['merchant'] == 'Rafa_AI') & (df_payments['year'] == 2023)].copy()

# 3. Get Merchant Metadata
rafa_meta = next((item for item in merchant_data if item["merchant"] == "Rafa_AI"), None)
if not rafa_meta:
    raise ValueError("Rafa_AI not found in merchant_data.json")

rafa_account_type = rafa_meta.get('account_type')
rafa_mcc = rafa_meta.get('merchant_category_code')
rafa_capture_delay = rafa_meta.get('capture_delay')

# 4. Enrich Data
# 4.1 Intracountry
df_rafa['intracountry'] = df_rafa['issuing_country'] == df_rafa['acquirer_country']

# 4.2 Month (from day_of_year)
# 2023 is not a leap year
df_rafa['month'] = pd.to_datetime(df_rafa['year'] * 1000 + df_rafa['day_of_year'], format='%Y%j').dt.month

# 5. Calculate Monthly Aggregates (Volume and Fraud Rate)
# Fraud Rate = (Volume of Fraudulent Txs) / (Total Volume)
monthly_stats = df_rafa.groupby('month').apply(
    lambda x: pd.Series({
        'monthly_volume': x['eur_amount'].sum(),
        'monthly_fraud_vol': x.loc[x['has_fraudulent_dispute'], 'eur_amount'].sum()
    })
).reset_index()

monthly_stats['monthly_fraud_rate'] = monthly_stats['monthly_fraud_vol'] / monthly_stats['monthly_volume']
monthly_stats['monthly_fraud_rate'] = monthly_stats['monthly_fraud_rate'].fillna(0.0)

# Merge stats back to transactions
df_rafa = df_rafa.merge(monthly_stats[['month', 'monthly_volume', 'monthly_fraud_rate']], on='month', how='left')

# 6. Calculate Fees
total_fees = 0.0

# Iterate through transactions
for _, row in df_rafa.iterrows():
    
    # Build context for this transaction
    tx_context = {
        'card_scheme': row['card_scheme'],
        'account_type': rafa_account_type,
        'mcc': rafa_mcc,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry'],
        'monthly_volume': row['monthly_volume'],
        'monthly_fraud_rate': row['monthly_fraud_rate'],
        'capture_delay': rafa_capture_delay
    }
    
    # Find applicable fee
    fee_found = False
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            # Calculate fee
            # fee = fixed_amount + rate * transaction_value / 10000
            fixed = float(rule['fixed_amount'])
            rate = float(rule['rate'])
            amount = row['eur_amount']
            
            fee = fixed + (rate * amount / 10000.0)
            total_fees += fee
            fee_found = True
            break 
            
    if not fee_found:
        # Should not happen with complete rule sets, but safe to ignore or log
        pass

# 7. Output Result
print(f"{total_fees:.2f}")
