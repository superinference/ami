import pandas as pd
import json
import numpy as np
import re

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1000
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1000000
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
    return float(value)

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '>5' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle percentages
    is_pct = '%' in s
    scale = 0.01 if is_pct else 1.0
    s = s.replace('%', '')
    
    # Handle k/m suffixes for volume
    def parse_val(v):
        v = v.strip()
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1000000
            v = v.replace('m', '')
        try:
            return float(v) * mult * scale
        except:
            return 0.0

    if '>' in s:
        val = parse_val(s.replace('>', ''))
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        return float('-inf'), val
    elif '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    else:
        # Exact match treated as range [val, val]
        val = parse_val(s)
        return val, val

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    Returns True if the rule applies, False otherwise.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False
        
    # 2. Is Credit
    # If rule['is_credit'] is None, it applies to both. If bool, must match.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False
            
    # 3. Intracountry
    # If rule['intracountry'] is None, applies to both.
    # Note: JSON uses 0.0/1.0 for boolean often, or null.
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        tx_intra = bool(tx_ctx.get('intracountry'))
        if rule_intra != tx_intra:
            return False

    # 4. Merchant Category Code (List)
    # Empty list [] means ALL.
    if rule.get('merchant_category_code'):
        if tx_ctx.get('mcc') not in rule['merchant_category_code']:
            return False

    # 5. Account Type (List)
    # Empty list [] means ALL.
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 6. ACI (List)
    # Empty list [] means ALL.
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False
            
    # 7. Capture Delay (String/Wildcard)
    if rule.get('capture_delay'):
        rule_delay = rule['capture_delay']
        tx_delay = str(tx_ctx.get('capture_delay'))
        
        if rule_delay == 'manual' or rule_delay == 'immediate':
            if rule_delay != tx_delay:
                return False
        else:
            # Handle ranges like ">5", "3-5", "<3"
            # Map tx_delay to numeric if possible, but merchant_data has strings like "manual", "immediate", "1", "7"
            # If tx_delay is numeric string:
            if tx_delay.isdigit():
                delay_days = float(tx_delay)
                min_d, max_d = parse_range(rule_delay)
                if not (min_d <= delay_days <= max_d):
                    return False
            else:
                # tx_delay is 'manual' or 'immediate' but rule is numeric range -> No match usually
                # Unless 'immediate' counts as 0 days? Let's assume strict string match if not digit.
                return False

    # 8. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        tx_vol = tx_ctx.get('monthly_volume', 0)
        if not (min_v <= tx_vol <= max_v):
            return False

    # 9. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        tx_fraud = tx_ctx.get('monthly_fraud_rate', 0)
        if not (min_f <= tx_fraud <= max_f):
            return False

    return True

def calculate_fee_for_amount(amount, rule):
    """Calculates fee for a specific amount based on rule."""
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════

def execute_analysis():
    # File paths
    payments_path = '/output/chunk6/data/context/payments.csv'
    fees_path = '/output/chunk6/data/context/fees.json'
    merchant_path = '/output/chunk6/data/context/merchant_data.json'
    
    print("Loading data...")
    try:
        df_payments = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        with open(merchant_path, 'r') as f:
            merchant_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 1. PREPARE MERCHANT METADATA
    # Create a dictionary for fast lookup: merchant_name -> {mcc, account_type, capture_delay}
    merchant_lookup = {}
    for m in merchant_data:
        merchant_lookup[m['merchant']] = {
            'mcc': m['merchant_category_code'],
            'account_type': m['account_type'],
            'capture_delay': m['capture_delay']
        }

    # 2. CALCULATE MONTHLY STATS (Volume & Fraud)
    # Convert day_of_year to month (2023 is not leap year)
    # Origin 2022-12-31 means day 1 is Jan 1 2023
    df_payments['date'] = pd.to_datetime(df_payments['day_of_year'], unit='D', origin='2022-12-31')
    df_payments['month'] = df_payments['date'].dt.month
    
    # Group by Merchant + Month
    # Calculate Total Volume (sum of eur_amount)
    # Calculate Fraud Volume (sum of eur_amount where has_fraudulent_dispute is True)
    # Note: Manual says fraud level is ratio of fraudulent volume over total volume.
    
    monthly_stats = df_payments.groupby(['merchant', 'month']).agg(
        total_volume=('eur_amount', 'sum'),
        fraud_volume=('eur_amount', lambda x: x[df_payments.loc[x.index, 'has_fraudulent_dispute'] == True].sum())
    ).reset_index()
    
    monthly_stats['fraud_rate'] = monthly_stats['fraud_volume'] / monthly_stats['total_volume']
    monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0)
    
    # Create lookup for stats: (merchant, month) -> {vol, fraud_rate}
    stats_lookup = {}
    for _, row in monthly_stats.iterrows():
        stats_lookup[(row['merchant'], row['month'])] = {
            'volume': row['total_volume'],
            'fraud_rate': row['fraud_rate']
        }

    # 3. FILTER TARGET TRANSACTIONS
    # Question: "For credit transactions... TransactPlus... average fee... for 50 EUR"
    # We use the distribution of these transactions to calculate the weighted average.
    
    # Ensure is_credit is boolean
    if df_payments['is_credit'].dtype == 'object':
        df_payments['is_credit'] = df_payments['is_credit'].map({'True': True, 'False': False, True: True, False: False})
        
    target_txs = df_payments[
        (df_payments['card_scheme'] == 'TransactPlus') & 
        (df_payments['is_credit'] == True)
    ].copy()
    
    print(f"Found {len(target_txs)} TransactPlus Credit transactions.")

    # 4. CALCULATE FEES FOR 50 EUR
    calculated_fees = []
    
    # Optimization: Pre-filter fees for TransactPlus and Credit (or wildcard)
    relevant_fees = [
        f for f in fees_data 
        if f.get('card_scheme') == 'TransactPlus' 
        and (f.get('is_credit') is None or f.get('is_credit') is True)
    ]
    
    print(f"Processing {len(target_txs)} transactions against {len(relevant_fees)} relevant fee rules...")
    
    # Iterate through transactions
    for _, tx in target_txs.iterrows():
        merchant = tx['merchant']
        month = tx['month']
        
        # Get Context Data
        m_meta = merchant_lookup.get(merchant, {})
        m_stats = stats_lookup.get((merchant, month), {'volume': 0, 'fraud_rate': 0})
        
        # Build Context
        tx_ctx = {
            'card_scheme': 'TransactPlus',
            'is_credit': True,
            'intracountry': (tx['issuing_country'] == tx['acquirer_country']),
            'mcc': m_meta.get('mcc'),
            'account_type': m_meta.get('account_type'),
            'capture_delay': m_meta.get('capture_delay'),
            'aci': tx['aci'],
            'monthly_volume': m_stats['volume'],
            'monthly_fraud_rate': m_stats['fraud_rate']
        }
        
        # Find Matching Rule
        # Rules are typically applied in order, or specific matches. 
        # Assuming the first match in the list is the correct one (standard rule engine logic).
        # If multiple match, usually the most specific one applies, but without priority weights, first match is standard.
        # However, let's check if we find *any* match.
        
        matched_rule = None
        for rule in relevant_fees:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Calculate fee for hypothetical 50 EUR
            fee = calculate_fee_for_amount(50.0, matched_rule)
            calculated_fees.append(fee)
        else:
            # If no rule matches, we might skip or log. 
            # For this exercise, we assume coverage.
            pass

    # 5. COMPUTE AVERAGE
    if not calculated_fees:
        print("No fees calculated. Check matching logic.")
        return

    average_fee = np.mean(calculated_fees)
    
    print("-" * 30)
    print(f"Average Fee for 50 EUR (TransactPlus Credit): {average_fee:.14f}")
    print("-" * 30)

if __name__ == "__main__":
    execute_analysis()