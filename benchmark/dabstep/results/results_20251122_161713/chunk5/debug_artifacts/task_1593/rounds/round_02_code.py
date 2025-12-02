# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1593
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9713 characters (FULL CODE)
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
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except:
            return 0.0
    return float(value) if value is not None else 0.0

def parse_volume_range(vol_str):
    """Parses volume strings like '100k-1m' into (min, max)."""
    if not vol_str: return (0, float('inf'))
    
    def parse_val(s):
        s = s.lower().strip()
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1000000
            s = s.replace('m', '')
        try:
            return float(s) * mult
        except:
            return 0.0

    if '-' in vol_str:
        parts = vol_str.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif '>' in vol_str:
        return (parse_val(vol_str.replace('>', '')), float('inf'))
    elif '<' in vol_str:
        return (0, parse_val(vol_str.replace('<', '')))
    return (0, float('inf'))

def parse_fraud_range(fraud_str):
    """Parses fraud strings like '>8.3%' into (min, max) ratio."""
    if not fraud_str: return (0, float('inf'))
    
    def parse_val(s):
        s = s.strip().replace('%', '')
        try:
            return float(s) / 100.0
        except:
            return 0.0

    if '-' in fraud_str:
        parts = fraud_str.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif '>' in fraud_str:
        return (parse_val(fraud_str.replace('>', '')), float('inf'))
    elif '<' in fraud_str:
        return (0, parse_val(fraud_str.replace('<', '')))
    return (0, float('inf'))

def match_fee_rule(ctx, rule):
    """
    Matches a transaction context against a fee rule.
    Returns True if the rule applies, False otherwise.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List containment or Wildcard)
    if rule.get('account_type'):
        if ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List containment or Wildcard)
    if rule.get('merchant_category_code'):
        if ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Complex logic: string match or numeric range)
    if rule.get('capture_delay'):
        r_cd = rule['capture_delay']
        c_cd = str(ctx['capture_delay'])
        
        # Direct match (e.g., "manual" == "manual")
        if r_cd == c_cd:
            pass 
        # Numeric comparisons (e.g., "1" < "3")
        elif any(x in r_cd for x in ['<', '>', '-']):
            try:
                # Merchant delay must be numeric to match a range rule
                val = float(c_cd)
                if '-' in r_cd:
                    low, high = map(float, r_cd.split('-'))
                    if not (low <= val <= high): return False
                elif '>' in r_cd:
                    limit = float(r_cd.replace('>', ''))
                    if not (val > limit): return False
                elif '<' in r_cd:
                    limit = float(r_cd.replace('<', ''))
                    if not (val < limit): return False
            except ValueError:
                # Merchant has "manual"/"immediate" but rule is numeric -> No match
                return False
        else:
            # Fallback for non-matching strings
            return False

    # 5. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        if not (min_v <= ctx['monthly_volume'] <= max_v):
            return False

    # 6. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        if not (min_f <= ctx['monthly_fraud_level'] <= max_f):
            return False

    # 7. Is Credit (Boolean match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False

    # 8. ACI (List containment or Wildcard)
    if rule.get('aci'):
        if ctx['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean match or Wildcard)
    if rule.get('intracountry') is not None:
        # Handle 0.0/1.0/True/False variations
        r_intra = bool(float(rule['intracountry'])) if isinstance(rule['intracountry'], (int, float, str)) else rule['intracountry']
        if r_intra != ctx['intracountry']:
            return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

def main():
    # 1. Load Data
    try:
        merchants = pd.read_json('/output/chunk5/data/context/merchant_data.json')
        payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
        with open('/output/chunk5/data/context/fees.json') as f:
            fees = json.load(f)
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    # 2. Identify Account Type 'H' Merchants
    h_merchants_df = merchants[merchants['account_type'] == 'H']
    h_merchant_names = h_merchants_df['merchant'].tolist()
    
    # Create lookup for merchant static data
    # Structure: {'MerchantName': {'account_type': 'H', 'mcc': 1234, 'capture_delay': '1'}}
    merchant_lookup = {}
    for _, row in h_merchants_df.iterrows():
        merchant_lookup[row['merchant']] = {
            'account_type': row['account_type'],
            'merchant_category_code': row['merchant_category_code'],
            'capture_delay': row['capture_delay']
        }

    # 3. Calculate Monthly Stats (Volume & Fraud) for these merchants
    # CRITICAL: Must use ALL transactions for these merchants, not just GlobalCard
    df_stats_base = payments[payments['merchant'].isin(h_merchant_names)].copy()
    
    # Convert day_of_year to month (2023)
    df_stats_base['date'] = pd.to_datetime(df_stats_base['year'] * 1000 + df_stats_base['day_of_year'], format='%Y%j')
    df_stats_base['month'] = df_stats_base['date'].dt.month
    
    # Aggregate
    monthly_stats = df_stats_base.groupby(['merchant', 'month']).agg(
        total_volume=('eur_amount', 'sum'),
        fraud_count=('has_fraudulent_dispute', 'sum'),
        tx_count=('has_fraudulent_dispute', 'count')
    ).reset_index()
    
    monthly_stats['fraud_rate'] = monthly_stats['fraud_count'] / monthly_stats['tx_count']
    
    # Create lookup: (merchant, month) -> {volume, fraud_rate}
    stats_lookup = {}
    for _, row in monthly_stats.iterrows():
        stats_lookup[(row['merchant'], row['month'])] = {
            'volume': row['total_volume'],
            'fraud_rate': row['fraud_rate']
        }

    # 4. Filter Transactions for Analysis
    # We only care about GlobalCard transactions for H merchants
    df_analysis = payments[
        (payments['merchant'].isin(h_merchant_names)) & 
        (payments['card_scheme'] == 'GlobalCard')
    ].copy()
    
    # Add month column to analysis dataframe
    df_analysis['date'] = pd.to_datetime(df_analysis['year'] * 1000 + df_analysis['day_of_year'], format='%Y%j')
    df_analysis['month'] = df_analysis['date'].dt.month

    # 5. Calculate Fees for 100 EUR Transaction
    calculated_fees = []
    target_amount = 100.0
    
    for _, tx in df_analysis.iterrows():
        merchant = tx['merchant']
        month = tx['month']
        
        # Retrieve context data
        m_data = merchant_lookup.get(merchant)
        stats = stats_lookup.get((merchant, month))
        
        if not m_data or not stats:
            continue
            
        # Build Context
        context = {
            'card_scheme': tx['card_scheme'],
            'account_type': m_data['account_type'],
            'merchant_category_code': m_data['merchant_category_code'],
            'capture_delay': m_data['capture_delay'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': tx['issuing_country'] == tx['acquirer_country'],
            'monthly_volume': stats['volume'],
            'monthly_fraud_level': stats['fraud_rate']
        }
        
        # Find Matching Rule
        matched_rule = None
        for rule in fees:
            if match_fee_rule(context, rule):
                matched_rule = rule
                break # First match wins
        
        if matched_rule:
            # Calculate Fee: Fixed + (Rate * Amount / 10000)
            # Rate is in basis points (per 10,000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * target_amount / 10000.0)
            calculated_fees.append(fee)

    # 6. Output Result
    if calculated_fees:
        avg_fee = sum(calculated_fees) / len(calculated_fees)
        print(f"{avg_fee:.6f}")
    else:
        print("0.000000")

if __name__ == "__main__":
    main()
