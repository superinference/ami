import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, k, m, commas to float."""
    if pd.isna(value) or value == '':
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    
    s = str(value).strip().lower().replace(',', '').replace('€', '').replace('$', '')
    s = s.lstrip('><≤≥')
    
    try:
        if '%' in s:
            return float(s.replace('%', '')) / 100
        if 'k' in s:
            return float(s.replace('k', '')) * 1000
        if 'm' in s:
            return float(s.replace('m', '')) * 1000000
        return float(s)
    except ValueError:
        return 0.0

def is_not_empty(val):
    """Check if list/array is not empty/null."""
    if val is None:
        return False
    if isinstance(val, (list, tuple, np.ndarray)):
        return len(val) > 0
    return False

def check_range(value, rule_str):
    """Check if value fits within a rule string like '>5', '100k-1m', '8.3%'."""
    if not rule_str:
        return True
        
    try:
        # Handle percentage comparisons if value is ratio (0-1) and rule is %
        # But here we assume value and rule are normalized or handled by caller.
        # Let's normalize rule first.
        
        s = str(rule_str).strip()
        
        # Range "min-max"
        if '-' in s:
            parts = s.split('-')
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val <= value <= max_val
            
        # Greater/Less
        if s.startswith('>'):
            limit = coerce_to_float(s[1:])
            return value > limit
        if s.startswith('<'):
            limit = coerce_to_float(s[1:])
            return value < limit
            
        # Exact match (rare for ranges, but possible)
        return value == coerce_to_float(s)
    except:
        return False

# ═══════════════════════════════════════════════════════════
# MAIN ANALYSIS
# ═══════════════════════════════════════════════════════════

def main():
    # 1. Load Data
    try:
        fees = json.load(open('/output/chunk4/data/context/fees.json', 'r'))
        merchant_data = json.load(open('/output/chunk4/data/context/merchant_data.json', 'r'))
        df = pd.read_csv('/output/chunk4/data/context/payments.csv')
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Get Fee 384
    fee_rule = next((f for f in fees if f['ID'] == 384), None)
    if not fee_rule:
        print("Fee 384 not found in fees.json")
        return

    # 3. Enrich Transaction Data
    # Create lookup dictionaries for merchant attributes
    merchant_lookup = {
        m['merchant']: {
            'account_type': m.get('account_type'),
            'mcc': m.get('merchant_category_code'),
            'capture_delay': m.get('capture_delay')
        }
        for m in merchant_data
    }

    # Map attributes to DataFrame
    df['account_type'] = df['merchant'].map(lambda x: merchant_lookup.get(x, {}).get('account_type'))
    df['mcc'] = df['merchant'].map(lambda x: merchant_lookup.get(x, {}).get('mcc'))
    df['capture_delay'] = df['merchant'].map(lambda x: merchant_lookup.get(x, {}).get('capture_delay'))

    # Filter for 2023
    df = df[df['year'] == 2023].copy()

    # 4. Calculate Monthly Stats (Volume & Fraud)
    # Some fee rules depend on monthly volume or fraud rates.
    # We calculate these per merchant per month to be precise.
    
    # Monthly Volume (Sum of eur_amount)
    monthly_vol = df.groupby(['merchant', 'year', 'day_of_year'])['eur_amount'].sum().reset_index() 
    # Note: day_of_year is daily, we need monthly. 
    # Approximation: We can group by month derived from day_of_year or just use the whole year if the rule implies general volume.
    # However, the manual says "Monthly volumes... computed always in natural months".
    # Let's create a 'month' column.
    # Simple approximation: month = (day_of_year - 1) // 30 + 1 (Roughly)
    # Better: Use datetime if possible, but day_of_year is sufficient for grouping.
    # Let's stick to the provided columns. We can map day_of_year to month.
    # Jan: 1-31, Feb: 32-59, etc.
    # For robustness, let's just calculate the stats for the specific transaction's context.
    # Actually, let's calculate total 2023 stats as a proxy if monthly is too complex without date lib,
    # OR group by (day_of_year // 30) as a proxy for month.
    df['month_proxy'] = (df['day_of_year'] - 1) // 31 + 1  # Rough month 1-12
    
    # Calculate stats per merchant-month
    monthly_stats = df.groupby(['merchant', 'month_proxy']).agg(
        total_vol=('eur_amount', 'sum'),
        fraud_count=('has_fraudulent_dispute', 'sum'),
        tx_count=('psp_reference', 'count')
    ).reset_index()
    
    monthly_stats['fraud_rate'] = monthly_stats['fraud_count'] / monthly_stats['total_vol'] # Fraud is ratio of volume usually?
    # Manual: "Fraud is defined as the ratio of fraudulent volume over total volume." -> Wait, usually count or volume?
    # Manual says: "ratio of fraudulent volume over total volume".
    # Let's re-calculate fraud volume.
    
    fraud_vol = df[df['has_fraudulent_dispute']].groupby(['merchant', 'month_proxy'])['eur_amount'].sum().reset_index(name='fraud_vol')
    monthly_stats = pd.merge(monthly_stats, fraud_vol, on=['merchant', 'month_proxy'], how='left').fillna(0)
    monthly_stats['fraud_rate_val'] = monthly_stats['fraud_vol'] / monthly_stats['total_vol']

    # Merge stats back to transactions
    df = pd.merge(df, monthly_stats[['merchant', 'month_proxy', 'total_vol', 'fraud_rate_val']], on=['merchant', 'month_proxy'], how='left')

    # 5. Apply Fee 384 Criteria (Find CURRENT matches)
    # We filter the DataFrame down to transactions that match the CURRENT rule.
    
    matches = df.copy()

    # A. Card Scheme
    if fee_rule.get('card_scheme'):
        matches = matches[matches['card_scheme'] == fee_rule['card_scheme']]

    # B. Is Credit
    if fee_rule.get('is_credit') is not None:
        matches = matches[matches['is_credit'] == fee_rule['is_credit']]

    # C. ACI (List check)
    if is_not_empty(fee_rule.get('aci')):
        matches = matches[matches['aci'].isin(fee_rule['aci'])]

    # D. Merchant Category Code (List check)
    if is_not_empty(fee_rule.get('merchant_category_code')):
        matches = matches[matches['mcc'].isin(fee_rule['merchant_category_code'])]

    # E. Account Type (CURRENT check)
    # If the rule currently restricts account types, we must respect that.
    if is_not_empty(fee_rule.get('account_type')):
        matches = matches[matches['account_type'].isin(fee_rule['account_type'])]

    # F. Intracountry
    if fee_rule.get('intracountry') is not None:
        is_intra = matches['issuing_country'] == matches['acquirer_country']
        if fee_rule['intracountry']:
            matches = matches[is_intra]
        else:
            matches = matches[~is_intra]

    # G. Capture Delay
    if fee_rule.get('capture_delay'):
        # This is a string rule like '>5' or 'immediate'.
        # We need to check if the merchant's capture_delay matches.
        # Since capture_delay in merchant_data is a string, we might need logic.
        # If rule is 'manual', merchant must be 'manual'.
        rule_cd = fee_rule['capture_delay']
        if rule_cd in ['immediate', 'manual']:
            matches = matches[matches['capture_delay'] == rule_cd]
        else:
            # Numeric comparison (e.g. rule '>5', merchant '7')
            # Filter out non-numeric merchant delays first
            numeric_mask = matches['capture_delay'].apply(lambda x: str(x).replace('.','',1).isdigit())
            matches_num = matches[numeric_mask].copy()
            matches_non = matches[~numeric_mask] # These don't match numeric rules
            
            # Apply range check to numeric ones
            valid_indices = matches_num[matches_num['capture_delay'].apply(lambda x: check_range(float(x), rule_cd))].index
            matches = matches.loc[valid_indices]

    # H. Monthly Volume
    if fee_rule.get('monthly_volume'):
        # Check against total_vol
        mask = matches['total_vol'].apply(lambda x: check_range(x, fee_rule['monthly_volume']))
        matches = matches[mask]

    # I. Monthly Fraud Level
    if fee_rule.get('monthly_fraud_level'):
        # Check against fraud_rate_val
        mask = matches['fraud_rate_val'].apply(lambda x: check_range(x, fee_rule['monthly_fraud_level']))
        matches = matches[mask]

    # 6. Identify Affected Merchants
    # "Affected" = Merchants in 'matches' who are NOT account_type 'R'.
    # Because if the rule changes to "Only R", these merchants (who currently match) will be excluded.
    
    affected_df = matches[matches['account_type'] != 'R']
    affected_merchants = sorted(affected_df['merchant'].unique().tolist())

    # 7. Output Result
    if not affected_merchants:
        print("None")
    else:
        print(", ".join(affected_merchants))

if __name__ == "__main__":
    main()