# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1641
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 7838 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import numpy as np
import json
import datetime

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None: return 0.0
    if isinstance(value, (int, float)): return float(value)
    s = str(value).strip().replace(',', '').replace('€', '').replace('$', '').replace('%', '')
    # Handle comparison operators
    if s.startswith('>='): return float(s[2:])
    if s.startswith('<='): return float(s[2:])
    if s.startswith('>'): return float(s[1:])
    if s.startswith('<'): return float(s[1:])
    try:
        return float(s)
    except:
        return 0.0

def get_month(doy, year=2023):
    """Returns month (1-12) from day of year."""
    # Simple approximation for non-leap year 2023
    date = datetime.datetime(year, 1, 1) + datetime.timedelta(days=int(doy) - 1)
    return date.month

def parse_range(range_str):
    """Parses range strings like '100k-1m', '>5', '<3', '7.7%-8.3%'."""
    if not isinstance(range_str, str): return None, None
    
    s = range_str.lower().replace('%', '').replace(',', '')
    
    # Handle k/m suffixes
    def parse_val(v):
        try:
            if 'k' in v: return float(v.replace('k', '')) * 1000
            if 'm' in v: return float(v.replace('m', '')) * 1000000
            return float(v)
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif '>' in s:
        val = parse_val(s.replace('>', '').replace('=', ''))
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', '').replace('=', ''))
        return float('-inf'), val
    else:
        try:
            val = parse_val(s)
            return val, val
        except:
            return None, None

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None: return True
    # Exact match (e.g. 'manual' == 'manual')
    if str(merchant_delay).lower() == str(rule_delay).lower(): return True
    
    # Map numeric merchant delays to ranges
    try:
        delay_days = float(merchant_delay)
        if rule_delay == '<3' and delay_days < 3: return True
        if rule_delay == '>5' and delay_days > 5: return True
        if rule_delay == '3-5' and 3 <= delay_days <= 5: return True
    except:
        pass
    return False

def match_fee_rule(tx_context, rule):
    """Determines if a fee rule applies to a specific transaction context."""
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List - Empty means wildcard)
    if rule.get('account_type') and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List - Empty means wildcard)
    if rule.get('merchant_category_code') and tx_context['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. ACI (List - Empty means wildcard)
    if rule.get('aci') and tx_context['aci'] not in rule['aci']:
        return False
        
    # 5. Is Credit (Bool - None means wildcard)
    if rule.get('is_credit') is not None and rule['is_credit'] != tx_context['is_credit']:
        return False
        
    # 6. Intracountry (Bool - None means wildcard)
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 7. Capture Delay
    if not check_capture_delay(tx_context['capture_delay'], rule.get('capture_delay')):
        return False

    # 8. Monthly Volume
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        vol = tx_context['monthly_volume']
        if not (min_v <= vol <= max_v):
            return False

    # 9. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # Fraud level in rule is %, e.g. 8.3. Context is ratio 0.083.
        # We compare percentages: 8.3 vs 8.3
        fraud_pct = tx_context['monthly_fraud_rate'] * 100
        if not (min_f <= fraud_pct <= max_f):
            return False
            
    return True

# --- Main Execution ---
try:
    # 1. Load Data
    payments = pd.read_csv('/output/chunk4/data/context/payments.csv')
    merchant_data = pd.read_json('/output/chunk4/data/context/merchant_data.json')
    with open('/output/chunk4/data/context/fees.json', 'r') as f:
        fees = json.load(f)

    # 2. Merge payments with merchant data
    # We need account_type, mcc, capture_delay from merchant_data
    df = pd.merge(payments, merchant_data, on='merchant', how='left')

    # 3. Calculate Derived Columns
    # Intracountry: Issuer == Acquirer
    df['intracountry'] = df['issuing_country'] == df['acquirer_country']
    
    # Month: From day_of_year
    df['month'] = df['day_of_year'].apply(lambda x: get_month(x))

    # 4. Calculate Monthly Stats per Merchant (Volume and Fraud Rate)
    # Group by merchant, month
    monthly_stats = df.groupby(['merchant', 'month']).agg(
        total_volume=('eur_amount', 'sum'),
        fraud_volume=('eur_amount', lambda x: x[df.loc[x.index, 'has_fraudulent_dispute']].sum())
    ).reset_index()

    # Calculate Fraud Rate (Volume Ratio)
    monthly_stats['fraud_rate'] = monthly_stats['fraud_volume'] / monthly_stats['total_volume']
    monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0.0)

    # Merge stats back to the main dataframe
    df = pd.merge(df, monthly_stats[['merchant', 'month', 'total_volume', 'fraud_rate']], on=['merchant', 'month'], how='left')

    # 5. Filter for Target Transactions
    # Question: "For account type F... card scheme GlobalCard"
    target_df = df[
        (df['account_type'] == 'F') & 
        (df['card_scheme'] == 'GlobalCard')
    ].copy()

    # 6. Calculate Fee for each transaction (Hypothetical 1234 EUR)
    hypothetical_amount = 1234.0
    calculated_fees = []

    # Pre-filter fees to GlobalCard to speed up matching
    global_card_fees = [f for f in fees if f['card_scheme'] == 'GlobalCard']

    for idx, row in target_df.iterrows():
        # Build context for rule matching based on the ACTUAL transaction environment
        ctx = {
            'card_scheme': row['card_scheme'],
            'account_type': row['account_type'],
            'mcc': row['merchant_category_code'],
            'aci': row['aci'],
            'is_credit': row['is_credit'],
            'intracountry': row['intracountry'],
            'capture_delay': row['capture_delay'],
            'monthly_volume': row['total_volume'],
            'monthly_fraud_rate': row['fraud_rate']
        }
        
        # Find the first matching rule
        matched_rule = None
        for rule in global_card_fees:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
                
        if matched_rule:
            # Fee = fixed + rate * amount / 10000
            # Note: rate is an integer (e.g., 19), formula is rate * value / 10000
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * hypothetical_amount / 10000.0)
            calculated_fees.append(fee)
        else:
            # If no rule matches, we skip (or could assume 0, but usually there's a fallback)
            # In this dataset, coverage is usually complete.
            pass

    # 7. Average
    if calculated_fees:
        avg_fee = sum(calculated_fees) / len(calculated_fees)
        print(f"{avg_fee:.6f}")
    else:
        print("0.000000")

except Exception as e:
    print(f"Error: {e}")
