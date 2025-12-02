# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1819
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7957 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# Helper functions for robust data processing
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
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
        try:
            return float(v)
        except:
            return 0.0
    return float(value)

def parse_range_tuple(range_str):
    """Parses a range string into (min, max)."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).strip().lower()
    if s == 'immediate': return ('immediate', 'immediate')
    if s == 'manual': return ('manual', 'manual')
    
    # Clean string
    s = s.replace('%', '').replace(',', '').replace('€', '').replace('$', '')
    
    # Helper to parse number with k/m
    def p(n):
        n = n.strip()
        mult = 1
        if n.endswith('k'): mult = 1000; n = n[:-1]
        elif n.endswith('m'): mult = 1000000; n = n[:-1]
        try: return float(n) * mult
        except: return 0.0

    if '-' in s:
        parts = s.split('-')
        return (p(parts[0]), p(parts[1]))
    elif s.startswith('>'):
        return (p(s[1:]), float('inf'))
    elif s.startswith('<'):
        return (-float('inf'), p(s[1:]))
    else:
        val = p(s)
        return (val, val)

def check_rule_match(tx_context, rule):
    """
    Checks if a fee rule applies to a specific transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List contains)
    # If rule list is empty or None, it matches all.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List contains)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Bool match)
    # Rule is_credit can be None (wildcard), True, False
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 5. ACI (List contains)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Bool/Float match)
    # Rule intracountry: None, 0.0 (False), 1.0 (True)
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['is_intracountry']:
            return False
            
    # 7. Capture Delay (Range match)
    cd_val = tx_context['capture_delay']
    cd_rule = rule.get('capture_delay')
    if cd_rule:
        # If rule is specific string
        if cd_rule in ['immediate', 'manual']:
            if cd_val != cd_rule: return False
        elif cd_val in ['immediate', 'manual']:
             if cd_val != cd_rule: return False
        else:
            # Numeric comparison
            try:
                val_float = float(cd_val)
                low, high = parse_range_tuple(cd_rule)
                if not (low <= val_float <= high):
                    return False
            except:
                pass 
                
    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        vol = tx_context['monthly_volume']
        low, high = parse_range_tuple(rule['monthly_volume'])
        if not (low <= vol <= high):
            return False
            
    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        # Fraud level in rule is usually %, e.g. "8.3%". parse_range_tuple returns 8.3
        # Context fraud is 0.083. Need to scale to percentage (0-100) for comparison
        fraud_pct = tx_context['monthly_fraud_rate'] * 100.0
        low, high = parse_range_tuple(rule['monthly_fraud_level'])
        if not (low <= fraud_pct <= high):
            return False
            
    return True

def calculate_belles_fees():
    # Define file paths
    payments_path = '/output/chunk5/data/context/payments.csv'
    fees_path = '/output/chunk5/data/context/fees.json'
    merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

    try:
        # Load data
        df_pay = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees = json.load(f)
        df_merch = pd.read_json(merchant_data_path)
        
        # Filter for Belles_cookbook_store, September 2023
        # September 2023 is Day 244 to 273 (Non-leap year)
        merchant_name = 'Belles_cookbook_store'
        mask = (
            (df_pay['merchant'] == merchant_name) & 
            (df_pay['year'] == 2023) & 
            (df_pay['day_of_year'] >= 244) & 
            (df_pay['day_of_year'] <= 273)
        )
        df_belles = df_pay[mask].copy()
        
        if df_belles.empty:
            print("No transactions found for Belles_cookbook_store in September 2023.")
            return

        # Get Merchant Static Data
        merch_info = df_merch[df_merch['merchant'] == merchant_name].iloc[0]
        mcc = int(merch_info['merchant_category_code'])
        account_type = merch_info['account_type']
        capture_delay = str(merch_info['capture_delay']) 
        
        # Calculate Monthly Stats (Volume & Fraud) for September 2023
        monthly_volume = df_belles['eur_amount'].sum()
        
        fraud_count = df_belles['has_fraudulent_dispute'].sum()
        total_count = len(df_belles)
        monthly_fraud_rate = fraud_count / total_count if total_count > 0 else 0.0
        
        # Pre-calculate intracountry for all rows
        df_belles['is_intracountry'] = df_belles['issuing_country'] == df_belles['acquirer_country']
        
        total_fees = 0.0
        
        # Iterate transactions to calculate fees
        for idx, row in df_belles.iterrows():
            # Build context for rule matching
            ctx = {
                'card_scheme': row['card_scheme'],
                'is_credit': row['is_credit'],
                'aci': row['aci'],
                'is_intracountry': row['is_intracountry'],
                'eur_amount': row['eur_amount'],
                'mcc': mcc,
                'account_type': account_type,
                'capture_delay': capture_delay,
                'monthly_volume': monthly_volume,
                'monthly_fraud_rate': monthly_fraud_rate
            }
            
            # Find matching rule (First match wins)
            matched_rule = None
            for rule in fees:
                if check_rule_match(ctx, rule):
                    matched_rule = rule
                    break 
            
            if matched_rule:
                # Calculate fee: fixed + (rate * amount / 10000)
                fixed = float(matched_rule['fixed_amount'])
                rate = float(matched_rule['rate'])
                fee = fixed + (rate * row['eur_amount'] / 10000.0)
                total_fees += fee
            else:
                # Fallback or warning if no rule matches (should not happen with complete ruleset)
                pass
                
        # Output the final result
        print(f"{total_fees:.2f}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    calculate_belles_fees()
