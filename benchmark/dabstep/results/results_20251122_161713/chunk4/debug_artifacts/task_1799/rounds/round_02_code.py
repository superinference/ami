# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1799
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10753 characters (FULL CODE)
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
            return float(v.replace('%', '')) / 100
            
        # Handle k/m suffixes
        multiplier = 1
        if 'k' in v:
            multiplier = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            multiplier = 1000000
            v = v.replace('m', '')
            
        # Range handling (e.g., "50-60") - return mean for coercion, but range checks handle this differently
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
    if not rule_str:
        return -float('inf'), float('inf')
    
    s = str(rule_str).strip().lower().replace(',', '')
    
    # Handle percentages for fraud
    is_percent = '%' in s
    
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
        
    # Numeric comparison
    # Map merchant numeric strings to floats
    try:
        m_val = float(m_delay)
    except ValueError:
        return False # Merchant delay is non-numeric (e.g. 'manual') but rule is numeric-ish
        
    # Parse rule range
    min_val, max_val = parse_range(r_delay)
    
    # Handle specific logic for capture delay ranges if needed, 
    # but parse_range handles <3, >5, 3-5 well.
    # Note: "3-5" means >=3 and <=5.
    
    # Edge case: parse_range treats "3-5" as inclusive.
    # If rule is "<3", min=-inf, max=3.
    # If rule is ">5", min=5, max=inf.
    
    # Adjust for strict inequalities if implied by context, but usually inclusive is safe for these bins
    # Let's assume standard interval logic:
    # <3 means strictly less than 3? Or <=? Usually bins are exclusive.
    # Given values: 1, 2, 7.
    # 1 < 3 (True). 7 > 5 (True).
    
    # Refinement for strictness based on common business logic:
    # <3 usually means 0, 1, 2.
    # >5 usually means 6, 7, ...
    
    # Let's use the parsed values.
    # If rule is <3, max is 3. 1 <= 3 is True.
    # If rule is >5, min is 5. 7 >= 5 is True.
    return min_val <= m_val <= max_val

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context must contain:
        - card_scheme (str)
        - is_credit (bool)
        - aci (str)
        - issuing_country (str)
        - acquirer_country (str)
        - account_type (str)
        - merchant_category_code (int)
        - capture_delay (str)
        - monthly_volume (float)
        - monthly_fraud_rate (float)
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
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
        # parse_range handles % conversion (e.g., "8.3%" -> 0.083)
        if not (min_fraud <= tx_context['monthly_fraud_rate'] <= max_fraud):
            return False
            
    # 7. Is Credit (Bool match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 8. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Derived Bool match)
    if rule.get('intracountry') is not None:
        is_intra = (tx_context['issuing_country'] == tx_context['acquirer_country'])
        # JSON might have 0.0/1.0 or False/True. Handle safely.
        rule_intra = bool(rule['intracountry'])
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
    # Note: Manual says monthly volumes are computed in natural months.
    # We need metrics for January to determine fees for January.
    
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
        fraud_volume = df_jan[df_jan['has_fraudulent_dispute']]['eur_amount'].sum()
        
        # Calculate Fraud Rate (Volume based)
        monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0
        
        # print(f"Metrics for {target_merchant} Jan 2023:")
        # print(f"  Volume: €{monthly_volume:,.2f}")
        # print(f"  Fraud Rate: {monthly_fraud_rate:.4%}")
        # print(f"  Account Type: {merchant_info['account_type']}")
        # print(f"  MCC: {merchant_info['merchant_category_code']}")
        # print(f"  Capture Delay: {merchant_info['capture_delay']}")
        
        # 4. Identify Applicable Fee IDs
        applicable_fee_ids = set()
        
        # We need to check each transaction because some rules depend on transaction-specific fields
        # (card_scheme, is_credit, aci, intracountry)
        
        # Optimization: Group by unique transaction characteristics to avoid iterating 138k rows if many are identical
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
