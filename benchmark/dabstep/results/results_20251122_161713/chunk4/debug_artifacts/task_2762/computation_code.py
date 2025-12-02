import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float."""
    if pd.isna(value) or value is None: return 0.0
    if isinstance(value, (int, float)): return float(value)
    s = str(value).strip().replace(',', '').replace('€', '').replace('$', '')
    s = s.lstrip('><≤≥')
    if '%' in s: return float(s.replace('%', '')) / 100
    return float(s)

def parse_range_value(val_str):
    """Parse range strings like '100k-1m', '>5', '<3' into (min, max)."""
    if not val_str: return (-float('inf'), float('inf'))
    s = str(val_str).lower().replace(',', '').replace('€', '').replace('%', '')
    
    # Helper for k/m multipliers
    def p(v):
        m = 1
        if 'k' in v: m = 1000; v = v.replace('k', '')
        elif 'm' in v: m = 1000000; v = v.replace('m', '')
        try: return float(v) * m
        except: return 0.0

    if '-' in s:
        parts = s.split('-')
        return (p(parts[0]), p(parts[1]))
    elif '>' in s:
        return (p(s.replace('>', '')) + 1e-9, float('inf')) # +epsilon for strict >
    elif '<' in s:
        return (-float('inf'), p(s.replace('<', '')) - 1e-9) # -epsilon for strict <
    else:
        # Exact match treated as range [x, x]
        val = p(s)
        return (val, val)

def check_rule_match(merchant_val, rule_val, match_type='range'):
    """Check if a merchant value matches a rule constraint."""
    if rule_val is None: return True # Wildcard matches all
    
    if match_type == 'range':
        # Handle percentage comparison (fraud rate) or volume
        # If rule is percentage string, merchant_val should be float (0.083)
        # parse_range_value handles stripping '%'
        min_v, max_v = parse_range_value(rule_val)
        
        # If rule was percentage (e.g. >8.3%), parse_range_value returned 8.3
        # If merchant_val is ratio (0.09), we need to align scales.
        # Heuristic: if rule had %, scale merchant_val up to 100 for comparison OR scale rule down.
        # Let's scale rule down if '%' was in string, but parse_range_value strips it.
        # Better: coerce rule bounds to ratio if '%' was present.
        if isinstance(rule_val, str) and '%' in rule_val:
            min_v /= 100.0
            max_v /= 100.0
            
        return min_v <= merchant_val <= max_v
        
    elif match_type == 'list':
        # rule_val is list, merchant_val is single item
        if not rule_val: return True # Empty list = wildcard
        return merchant_val in rule_val
        
    elif match_type == 'bool':
        return bool(rule_val) == bool(merchant_val)
        
    elif match_type == 'exact_or_range':
        # Special for capture_delay: can be 'manual' (str) or '2' (numeric/range)
        if str(rule_val) in ['manual', 'immediate']:
            return str(merchant_val) == str(rule_val)
        # Try numeric range
        try:
            m_val = float(merchant_val)
            min_v, max_v = parse_range_value(rule_val)
            return min_v <= m_val <= max_v
        except:
            return str(rule_val) == str(merchant_val)
            
    return False

def get_month_from_doy(doy):
    """Map day of year (1-365) to month (1-12)."""
    # Days cumulative: 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365
    days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 366]
    for i in range(12):
        if days[i] < doy <= days[i+1]:
            return i + 1
    return 1

# ═══════════════════════════════════════════════════════════
# Main Analysis
# ═══════════════════════════════════════════════════════════

def execute_analysis():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk4/data/context/payments.csv')
        with open('/output/chunk4/data/context/merchant_data.json') as f: m_data = json.load(f)
        with open('/output/chunk4/data/context/fees.json') as f: fees = json.load(f)
        acquirer_countries = pd.read_csv('/output/chunk4/data/context/acquirer_countries.csv')
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    target_merchant = 'Golfclub_Baron_Friso'
    target_year = 2023
    
    # 2. Get Merchant Metadata
    m_profile = next((m for m in m_data if m['merchant'] == target_merchant), None)
    if not m_profile:
        print(f"Merchant {target_merchant} not found")
        return
        
    mcc = m_profile['merchant_category_code']
    account_type = m_profile['account_type']
    capture_delay = m_profile['capture_delay']
    
    # Determine Acquirer Country
    # Merchant has list of acquirers, usually one primary or we check all.
    # Golfclub_Baron_Friso has ["medici"]
    acquirer_name = m_profile['acquirer'][0]
    acq_row = acquirer_countries[acquirer_countries['acquirer'] == acquirer_name]
    if not acq_row.empty:
        acquirer_country = acq_row['country_code'].iloc[0]
    else:
        # Fallback: check payments for most common acquirer_country
        acquirer_country = payments[payments['merchant'] == target_merchant]['acquirer_country'].mode()[0]

    # 3. Filter Transactions
    df = payments[(payments['merchant'] == target_merchant) & (payments['year'] == target_year)].copy()
    if df.empty:
        print("No transactions found")
        return

    # 4. Add Month Column (for "Natural Month" calculation)
    df['month'] = df['day_of_year'].apply(get_month_from_doy)

    # 5. Simulation Loop
    schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
    scheme_total_costs = {s: 0.0 for s in schemes}
    scheme_validity = {s: True for s in schemes}

    # Iterate through each month (Fee tiers are determined monthly)
    for month in range(1, 13):
        df_month = df[df['month'] == month]
        if df_month.empty: continue
        
        # Calculate Monthly Stats (Volume & Fraud)
        monthly_vol = df_month['eur_amount'].sum()
        fraud_vol = df_month[df_month['has_fraudulent_dispute'] == True]['eur_amount'].sum()
        monthly_fraud_rate = fraud_vol / monthly_vol if monthly_vol > 0 else 0.0
        
        # Group transactions for efficiency
        # Key attributes: is_credit, aci, is_intracountry
        df_month['is_intracountry'] = df_month['issuing_country'] == acquirer_country
        
        grouped = df_month.groupby(['is_credit', 'aci', 'is_intracountry']).agg(
            count=('psp_reference', 'count'),
            sum_amount=('eur_amount', 'sum')
        ).reset_index()
        
        # Calculate cost for each scheme for this month
        for scheme in schemes:
            if not scheme_validity[scheme]: continue # Skip if already invalid
            
            month_cost = 0
            
            # Pre-filter rules for this Scheme + Merchant Profile + Month Stats
            # This optimization reduces inner loop checks
            applicable_rules = []
            for rule in fees:
                if rule['card_scheme'] != scheme: continue
                if not check_rule_match(mcc, rule['merchant_category_code'], 'list'): continue
                if not check_rule_match(account_type, rule['account_type'], 'list'): continue
                if not check_rule_match(capture_delay, rule['capture_delay'], 'exact_or_range'): continue
                if not check_rule_match(monthly_vol, rule['monthly_volume'], 'range'): continue
                if not check_rule_match(monthly_fraud_rate, rule['monthly_fraud_level'], 'range'): continue
                applicable_rules.append(rule)
            
            # Match specific transaction attributes
            for _, row in grouped.iterrows():
                matched_rule = None
                for rule in applicable_rules:
                    # is_credit
                    if rule['is_credit'] is not None and rule['is_credit'] != row['is_credit']: continue
                    # aci
                    if not check_rule_match(row['aci'], rule['aci'], 'list'): continue
                    # intracountry
                    if rule['intracountry'] is not None:
                        # Handle 0.0/1.0/True/False
                        rule_intra = bool(rule['intracountry'])
                        if rule_intra != row['is_intracountry']: continue
                    
                    matched_rule = rule
                    break # Use first matching rule
                
                if matched_rule:
                    fee = (matched_rule['fixed_amount'] * row['count']) + \
                          (matched_rule['rate'] * row['sum_amount'] / 10000.0)
                    month_cost += fee
                else:
                    # If a scheme cannot process a transaction type (no rule), it's invalid
                    scheme_validity[scheme] = False
                    break
            
            if scheme_validity[scheme]:
                scheme_total_costs[scheme] += month_cost

    # 6. Determine Best Scheme
    valid_costs = {k: v for k, v in scheme_total_costs.items() if scheme_validity[k]}
    
    if not valid_costs:
        print("No valid schemes found")
    else:
        best_scheme = min(valid_costs, key=valid_costs.get)
        print(best_scheme)

if __name__ == "__main__":
    execute_analysis()