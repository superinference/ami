# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1604
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10108 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
        return 0.0
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
        # K/M suffix handling
        v_lower = v.lower()
        if 'k' in v_lower:
            return float(v_lower.replace('k', '')) * 1000
        if 'm' in v_lower:
            return float(v_lower.replace('m', '')) * 1000000
            
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def get_month_from_doy(doy, year=2023):
    """Convert day of year to month number (1-12)."""
    return pd.Timestamp(year, 1, 1) + pd.Timedelta(days=doy - 1)

def parse_range_check(value, range_str):
    """
    Check if a value falls within a range string (e.g., '100k-1m', '>5', '<3').
    Returns True/False.
    """
    if range_str is None:
        return True
        
    try:
        # Handle percentages in range string
        is_pct = '%' in range_str
        
        # Clean string for parsing
        s = range_str.lower().replace('%', '').replace(',', '')
        
        # Handle K/M suffixes
        def parse_val(x):
            if 'k' in x: return float(x.replace('k', '')) * 1000
            if 'm' in x: return float(x.replace('m', '')) * 1000000
            return float(x)

        # Handle operators
        if s.startswith('>'):
            limit = parse_val(s[1:])
            if is_pct: limit /= 100
            return value > limit
        if s.startswith('<'):
            limit = parse_val(s[1:])
            if is_pct: limit /= 100
            return value < limit
            
        # Handle ranges (e.g., "100k-1m")
        if '-' in s:
            parts = s.split('-')
            if len(parts) == 2:
                low = parse_val(parts[0])
                high = parse_val(parts[1])
                if is_pct:
                    low /= 100
                    high /= 100
                return low <= value <= high
                
        # Exact match (rare for ranges, but possible)
        val = parse_val(s)
        if is_pct: val /= 100
        return value == val
        
    except Exception as e:
        # If parsing fails, assume no match to be safe
        return False

def match_fee_rule(tx_context, rule):
    """
    Check if a fee rule applies to a specific transaction context.
    tx_context: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme
    if rule.get('card_scheme') != tx_context['card_scheme']:
        return False

    # 2. Account Type (Wildcard supported)
    rule_acct = rule.get('account_type')
    if rule_acct and len(rule_acct) > 0:
        if tx_context['account_type'] not in rule_acct:
            return False

    # 3. Merchant Category Code (Wildcard supported)
    rule_mcc = rule.get('merchant_category_code')
    if rule_mcc and len(rule_mcc) > 0:
        if tx_context['merchant_category_code'] not in rule_mcc:
            return False

    # 4. Capture Delay (Wildcard supported)
    if rule.get('capture_delay') is not None:
        # Exact match or range check could be implemented, but data shows exact strings or ranges
        # merchant_data has 'immediate', 'manual', '1', etc.
        # fees.json has '>5', 'immediate', etc.
        # Simple string match for exacts, range check for others
        r_cd = str(rule['capture_delay'])
        t_cd = str(tx_context['capture_delay'])
        
        if r_cd == t_cd:
            pass # Match
        elif r_cd.startswith('>') or r_cd.startswith('<'):
            # Try to parse numeric delay
            try:
                # Map 'immediate' to 0, 'manual' to 999 (assuming manual is long)
                delay_map = {'immediate': 0, 'manual': 999}
                t_val = delay_map.get(t_cd, float(t_cd) if t_cd.replace('.','').isdigit() else 999)
                if not parse_range_check(t_val, r_cd):
                    return False
            except:
                return False
        else:
            return False

    # 5. Is Credit (Wildcard supported)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 6. ACI (Wildcard supported)
    rule_aci = rule.get('aci')
    if rule_aci and len(rule_aci) > 0:
        if tx_context['aci'] not in rule_aci:
            return False

    # 7. Intracountry (Wildcard supported)
    if rule.get('intracountry') is not None:
        # fees.json uses 0.0/1.0 for boolean false/true or boolean types
        rule_intra = rule['intracountry']
        # Normalize rule value to bool
        if isinstance(rule_intra, (float, int)):
            rule_intra_bool = bool(rule_intra)
        elif isinstance(rule_intra, str):
            rule_intra_bool = rule_intra.lower() == 'true'
        else:
            rule_intra_bool = rule_intra
            
        if rule_intra_bool != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range check)
    if rule.get('monthly_volume') is not None:
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level') is not None:
        if not parse_range_check(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    return True

# ==========================================
# MAIN EXECUTION
# ==========================================

# 1. Load Data
base_path = '/output/chunk5/data/context/'
payments = pd.read_csv(base_path + 'payments.csv')
with open(base_path + 'merchant_data.json', 'r') as f:
    merchant_data = json.load(f)
with open(base_path + 'fees.json', 'r') as f:
    fees_data = json.load(f)

# 2. Identify Account Type 'H' Merchants
h_merchants_info = {m['merchant']: m for m in merchant_data if m['account_type'] == 'H'}
h_merchant_names = list(h_merchants_info.keys())

print(f"Merchants with Account Type 'H': {h_merchant_names}")

# 3. Calculate Monthly Stats for ALL merchants (needed for correct context)
# Add month column
payments['month'] = payments['day_of_year'].apply(lambda x: get_month_from_doy(x).month)

# Group by merchant and month
monthly_stats = payments.groupby(['merchant', 'month']).agg(
    total_volume=('eur_amount', 'sum'),
    fraud_volume=('eur_amount', lambda x: x[payments.loc[x.index, 'has_fraudulent_dispute']].sum())
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_volume'] / monthly_stats['total_volume']

# Create a lookup dictionary for fast access: (merchant, month) -> {vol, fraud}
stats_lookup = {}
for _, row in monthly_stats.iterrows():
    stats_lookup[(row['merchant'], row['month'])] = {
        'volume': row['total_volume'],
        'fraud_rate': row['fraud_rate']
    }

# 4. Filter Transactions for Analysis
# We only care about transactions from H-merchants using TransactPlus
target_txs = payments[
    (payments['merchant'].isin(h_merchant_names)) & 
    (payments['card_scheme'] == 'TransactPlus')
].copy()

print(f"Found {len(target_txs)} TransactPlus transactions for Account Type H.")

# 5. Calculate Fees for Hypothetical 1000 EUR Transaction
calculated_fees = []
hypothetical_amount = 1000.0

# Pre-filter fees to optimize (only TransactPlus)
tp_fees = [r for r in fees_data if r.get('card_scheme') == 'TransactPlus']

for idx, tx in target_txs.iterrows():
    merchant_name = tx['merchant']
    month = tx['month']
    
    # Get Merchant Static Data
    m_info = h_merchants_info.get(merchant_name)
    if not m_info: continue
    
    # Get Merchant Dynamic Data (Monthly Stats)
    stats = stats_lookup.get((merchant_name, month))
    if not stats: continue
    
    # Build Context
    context = {
        'card_scheme': 'TransactPlus',
        'account_type': 'H', # Known from filter
        'merchant_category_code': m_info['merchant_category_code'],
        'capture_delay': m_info['capture_delay'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'monthly_volume': stats['volume'],
        'monthly_fraud_level': stats['fraud_rate']
    }
    
    # Find Matching Rule
    # Note: In reality, multiple rules might match (e.g., specific vs general). 
    # Usually, the most specific one applies, or the first one found if ordered by priority.
    # Assuming the dataset is structured such that the first match is valid or rules are mutually exclusive.
    matched_rule = None
    for rule in tp_fees:
        if match_fee_rule(context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate Fee: Fixed + (Rate * Amount / 10000)
        # Rate is in basis points (per 10,000)
        fixed = float(matched_rule['fixed_amount'])
        rate = float(matched_rule['rate'])
        
        fee = fixed + (rate * hypothetical_amount / 10000.0)
        calculated_fees.append(fee)
    else:
        # If no rule matches, we skip (or could log error). 
        # With correct logic, coverage should be high.
        pass

# 6. Compute Average
if calculated_fees:
    avg_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"Average Fee for 1000 EUR: {avg_fee:.6f} EUR")
else:
    print("No applicable fees calculated.")
