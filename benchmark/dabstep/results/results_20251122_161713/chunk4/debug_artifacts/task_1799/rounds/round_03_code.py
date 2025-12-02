# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1799
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 9504 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        
        # Handle percentages
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
            
        # Handle k/m suffixes
        multiplier = 1
        if 'k' in v:
            multiplier = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            multiplier = 1000000
            v = v.replace('m', '')
            
        # Range handling (e.g., "50-60") - return mean for coercion
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2 * multiplier
            except:
                pass
                
        try:
            return float(v) * multiplier
        except ValueError:
            return 0.0
    return float(value)

def parse_range(rule_str):
    """Parses a range string like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if rule_str is None:
        return -float('inf'), float('inf')
    
    s = str(rule_str).strip().lower().replace(',', '')
    
    # Helper to parse value with k/m/%
    def parse_val(val_str):
        val_str = val_str.strip()
        mult = 1
        if 'k' in val_str:
            mult = 1000
            val_str = val_str.replace('k', '')
        elif 'm' in val_str:
            mult = 1000000
            val_str = val_str.replace('m', '')
        elif '%' in val_str:
            mult = 0.01
            val_str = val_str.replace('%', '')
        return float(val_str) * mult

    try:
        if '-' in s:
            parts = s.split('-')
            return parse_val(parts[0]), parse_val(parts[1])
        elif s.startswith('>'):
            return parse_val(s[1:]), float('inf')
        elif s.startswith('<'):
            return -float('inf'), parse_val(s[1:])
        else:
            # Exact match treated as range [val, val]
            val = parse_val(s)
            return val, val
    except:
        return -float('inf'), float('inf')

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    # Direct string match (e.g., "manual", "immediate")
    if m_delay == r_delay:
        return True
        
    # If merchant delay is numeric (e.g. "1"), check against rule range (e.g. "<3")
    try:
        m_val = float(m_delay)
        min_val, max_val = parse_range(r_delay)
        # Handle strict inequalities implied by context if necessary, 
        # but standard inclusive range check is usually sufficient for these bins.
        # Note: parse_range returns (-inf, 3) for "<3". 1 is in range.
        # parse_range returns (5, inf) for ">5". 7 is in range.
        return min_val <= m_val <= max_val
    except ValueError:
        # Merchant delay is non-numeric (e.g. "manual") but rule didn't match string above
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match - empty list in rule means ALL)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match - empty list in rule means ALL)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Complex match)
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False
            
    # 5. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_vol, max_vol = parse_range(rule['monthly_volume'])
        if not (min_vol <= tx_context['monthly_volume'] <= max_vol):
            return False
            
    # 6. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_fraud, max_fraud = parse_range(rule['monthly_fraud_level'])
        # tx_context['monthly_fraud_rate'] is a float (e.g., 0.08)
        if not (min_fraud <= tx_context['monthly_fraud_rate'] <= max_fraud):
            return False
            
    # 7. Is Credit (Bool match - null in rule means ALL)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 8. ACI (List match - empty list in rule means ALL)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Derived Bool match - null in rule means ALL)
    if rule.get('intracountry') is not None:
        is_intra = (tx_context['issuing_country'] == tx_context['acquirer_country'])
        # Handle potential string/float representations in JSON
        rule_intra_val = rule['intracountry']
        if isinstance(rule_intra_val, str):
            rule_intra = rule_intra_val.lower() == 'true'
        else:
            rule_intra = bool(rule_intra_val)
            
        if rule_intra != is_intra:
            return False
            
    return True

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
merchant_path = '/output/chunk4/data/context/merchant_data.json'
fees_path = '/output/chunk4/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Get Merchant Attributes for 'Rafa_AI'
target_merchant = 'Rafa_AI'
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)

if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
else:
    # 3. Calculate Monthly Metrics for Jan 2023
    # Filter for Rafa_AI, Year 2023, Jan (day <= 31)
    jan_2023_mask = (
        (df_payments['merchant'] == target_merchant) & 
        (df_payments['year'] == 2023) & 
        (df_payments['day_of_year'] <= 31)
    )
    
    df_jan = df_payments[jan_2023_mask]
    
    if df_jan.empty:
        print("No transactions found for Rafa_AI in Jan 2023.")
    else:
        # Calculate Monthly Volume (Sum of eur_amount)
        monthly_volume = df_jan['eur_amount'].sum()
        
        # Calculate Monthly Fraud Volume (Sum of eur_amount where has_fraudulent_dispute is True)
        # Note: Manual defines fraud level as "ratio between monthly total volume and monthly volume notified as fraud"
        # Usually this means Fraud Volume / Total Volume.
        fraud_volume = df_jan[df_jan['has_fraudulent_dispute']]['eur_amount'].sum()
        
        # Calculate Fraud Rate
        monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0
        
        # 4. Identify Applicable Fee IDs
        applicable_fee_ids = set()
        
        # Optimization: Group by unique transaction characteristics
        # Relevant columns for fee matching:
        fee_cols = ['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']
        unique_tx_types = df_jan[fee_cols].drop_duplicates()
        
        for _, tx in unique_tx_types.iterrows():
            # Build context
            context = {
                # Transaction specific
                'card_scheme': tx['card_scheme'],
                'is_credit': tx['is_credit'],
                'aci': tx['aci'],
                'issuing_country': tx['issuing_country'],
                'acquirer_country': tx['acquirer_country'],
                
                # Merchant specific
                'account_type': merchant_info['account_type'],
                'merchant_category_code': merchant_info['merchant_category_code'],
                'capture_delay': merchant_info['capture_delay'],
                
                # Monthly metrics
                'monthly_volume': monthly_volume,
                'monthly_fraud_rate': monthly_fraud_rate
            }
            
            # Check against all fee rules
            for rule in fees_data:
                if match_fee_rule(context, rule):
                    applicable_fee_ids.add(rule['ID'])
        
        # 5. Output Result
        # Sort IDs for consistent output
        sorted_ids = sorted(list(applicable_fee_ids))
        
        # Print as comma-separated string
        print(", ".join(map(str, sorted_ids)))
