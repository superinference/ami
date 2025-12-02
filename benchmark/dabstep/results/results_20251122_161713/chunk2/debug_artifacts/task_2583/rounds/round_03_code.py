# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2583
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 8013 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float."""
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
        return float(v)
    return float(value)

def parse_volume_check(vol_str, actual_vol):
    """Check if actual volume fits in the rule's volume range string."""
    if not vol_str:
        return True
    
    # Normalize string (100k -> 100000, 1m -> 1000000)
    s = vol_str.lower().replace('€', '').replace(',', '')
    s = s.replace('k', '000').replace('m', '000000')
    
    try:
        if '-' in s:
            low, high = map(float, s.split('-'))
            return low <= actual_vol <= high
        elif '>' in s:
            limit = float(s.replace('>', ''))
            return actual_vol > limit
        elif '<' in s:
            limit = float(s.replace('<', ''))
            return actual_vol < limit
    except:
        return False
    return False

def parse_fraud_check(fraud_str, actual_fraud_rate):
    """Check if actual fraud rate fits in the rule's fraud range string."""
    if not fraud_str:
        return True
        
    # actual_fraud_rate is a ratio (e.g. 0.08), rule is percentage (e.g. "8%")
    # Convert actual to percentage for comparison
    actual_pct = actual_fraud_rate * 100
    
    s = fraud_str.replace('%', '').strip()
    
    try:
        if '-' in s:
            low, high = map(float, s.split('-'))
            return low <= actual_pct <= high
        elif '>' in s:
            limit = float(s.replace('>', ''))
            return actual_pct > limit
        elif '<' in s:
            limit = float(s.replace('<', ''))
            return actual_pct < limit
    except:
        return False
    return False

def match_fee_rule(ctx, rule):
    """
    Match a transaction context against a fee rule.
    ctx: dict containing transaction and merchant details
    rule: dict containing fee rule criteria
    """
    # 1. Card Scheme (Exact match required for the simulation loop)
    if rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match or Wildcard)
    if rule['account_type'] and ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List match or Wildcard)
    if rule['merchant_category_code'] and ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (Exact match or Wildcard)
    if rule['capture_delay'] and rule['capture_delay'] != ctx['capture_delay']:
        return False
        
    # 5. Is Credit (Bool match or Wildcard)
    # Note: rule['is_credit'] can be True, False, or None
    if rule['is_credit'] is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
        
    # 6. ACI (List match or Wildcard)
    if rule['aci'] and ctx['aci'] not in rule['aci']:
        return False
        
    # 7. Intracountry (Float/Bool match or Wildcard)
    if rule['intracountry'] is not None:
        # JSON loads 0.0/1.0, convert to bool for comparison
        rule_intra = bool(rule['intracountry'])
        if rule_intra != ctx['intracountry']:
            return False
            
    # 8. Monthly Volume (Range match)
    if not parse_volume_check(rule['monthly_volume'], ctx['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level (Range match)
    if not parse_fraud_check(rule['monthly_fraud_level'], ctx['monthly_fraud_level']):
        return False
        
    return True

def solve():
    # 1. Load Data
    payments_path = '/output/chunk2/data/context/payments.csv'
    merchant_path = '/output/chunk2/data/context/merchant_data.json'
    fees_path = '/output/chunk2/data/context/fees.json'
    
    df = pd.read_csv(payments_path)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
    with open(fees_path, 'r') as f:
        fees = json.load(f)
        
    # 2. Filter Data
    target_merchant = 'Martinis_Fine_Steakhouse'
    # Filter for January (Day 1-31)
    mask = (df['merchant'] == target_merchant) & (df['day_of_year'] >= 1) & (df['day_of_year'] <= 31)
    jan_txs = df[mask].copy()
    
    if len(jan_txs) == 0:
        print("No transactions found for merchant in January.")
        return

    # 3. Calculate Merchant Profile (Volume & Fraud)
    # These metrics define the merchant's tier and apply regardless of which scheme is tested
    total_volume = jan_txs['eur_amount'].sum()
    fraud_volume = jan_txs[jan_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    # 4. Get Merchant Metadata
    m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return
        
    account_type = m_info['account_type']
    mcc = m_info['merchant_category_code']
    capture_delay = m_info['capture_delay']
    
    # 5. Identify Available Schemes
    # We simulate fees for ALL schemes present in the fee rules to see which is most expensive
    available_schemes = set(r['card_scheme'] for r in fees)
    
    # Optimization: Group rules by scheme to speed up matching
    rules_by_scheme = {s: [] for s in available_schemes}
    for r in fees:
        rules_by_scheme[r['card_scheme']].append(r)
        
    # 6. Calculate Fees for Each Scheme
    scheme_costs = {}
    
    for scheme in available_schemes:
        total_fee = 0.0
        relevant_rules = rules_by_scheme[scheme]
        
        # Iterate through every actual transaction
        for _, tx in jan_txs.iterrows():
            # Determine transaction-specific attributes
            is_credit = tx['is_credit']
            aci = tx['aci']
            # Intracountry: Issuer == Acquirer
            intracountry = (tx['issuing_country'] == tx['acquirer_country'])
            amount = tx['eur_amount']
            
            # Build Context
            ctx = {
                'card_scheme': scheme, # Simulating this scheme
                'account_type': account_type,
                'mcc': mcc,
                'capture_delay': capture_delay,
                'monthly_volume': total_volume,
                'monthly_fraud_level': fraud_rate,
                'is_credit': is_credit,
                'aci': aci,
                'intracountry': intracountry
            }
            
            # Find first matching rule
            matched_rule = None
            for rule in relevant_rules:
                if match_fee_rule(ctx, rule):
                    matched_rule = rule
                    break # Use first match
            
            if matched_rule:
                # Calculate Fee: Fixed + (Rate * Amount / 10000)
                fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * amount / 10000.0)
                total_fee += fee
            else:
                # If no rule matches, fee is 0 (or could be considered infinite/error, but 0 is safe for "max" check if others exist)
                pass
                
        scheme_costs[scheme] = total_fee
        # print(f"Scheme: {scheme}, Total Fee: {total_fee:.2f}") # Debug
        
    # 7. Find Max
    if not scheme_costs:
        print("No applicable schemes found.")
        return
        
    most_expensive_scheme = max(scheme_costs, key=scheme_costs.get)
    
    # 8. Output Result
    print(most_expensive_scheme)

if __name__ == "__main__":
    solve()
