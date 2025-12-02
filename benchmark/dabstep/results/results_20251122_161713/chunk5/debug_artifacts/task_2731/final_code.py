import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_volume_range(range_str):
    """Parses volume strings like '100k-1m', '>10m' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().replace(',', '').replace('€', '').strip()
    
    def parse_val(val_str):
        m = 1
        if 'k' in val_str:
            m = 1000
            val_str = val_str.replace('k', '')
        elif 'm' in val_str:
            m = 1000000
            val_str = val_str.replace('m', '')
        try:
            return float(val_str) * m
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif '>' in s:
        return parse_val(s.replace('>', '')), float('inf')
    elif '<' in s:
        return 0.0, parse_val(s.replace('<', ''))
    return None, None

def parse_fraud_range(range_str):
    """Parses fraud strings like '0.0%-1.5%', '>8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.replace('%', '').strip()
    
    if '-' in s:
        parts = s.split('-')
        try:
            return float(parts[0])/100, float(parts[1])/100
        except:
            return 0.0, 1.0
    elif '>' in s:
        try:
            return float(s.replace('>', ''))/100, float('inf')
        except:
            return 0.0, 1.0
    elif '<' in s:
        try:
            return 0.0, float(s.replace('<', ''))/100
        except:
            return 0.0, 1.0
    return None, None

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match, empty=any)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match, empty=any)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay (Exact match, null=any)
    if rule.get('capture_delay'):
        rd = str(rule['capture_delay'])
        td = str(tx_context['capture_delay'])
        if rd != td and rd not in ['manual', 'immediate']: # Simplified for this dataset
             # Check for inequalities if needed, but dataset mostly uses categories
             if rd.startswith('>'):
                 try:
                     if not (float(td) > float(rd[1:])): return False
                 except: return False
             elif rd.startswith('<'):
                 try:
                     if not (float(td) < float(rd[1:])): return False
                 except: return False
             else:
                 return False

    # 5. Monthly Volume (Range match, null=any)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        if min_v is not None:
            vol = tx_context['monthly_volume']
            if not (min_v <= vol <= max_v):
                return False

    # 6. Monthly Fraud Level (Range match, null=any)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        if min_f is not None:
            fr = tx_context['monthly_fraud_rate']
            if not (min_f <= fr <= max_f):
                return False

    # 7. Is Credit (Bool match, null=any)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List match, empty/null=any)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Bool match, null=any)
    if rule.get('intracountry') is not None:
        rule_intra = rule['intracountry']
        if isinstance(rule_intra, (str, float, int)):
            rule_intra = bool(float(rule_intra))
        if rule_intra != tx_context['intracountry']:
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000)

def execute_analysis():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
        with open('/output/chunk5/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
        with open('/output/chunk5/data/context/fees.json', 'r') as f:
            fees = json.load(f)
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Rafa_AI in July
    merchant_name = 'Rafa_AI'
    # July is days 182 to 212 (inclusive)
    july_mask = (payments['day_of_year'] >= 182) & (payments['day_of_year'] <= 212)
    merchant_mask = payments['merchant'] == merchant_name
    
    # All July transactions (needed for volume/fraud stats)
    july_txs = payments[merchant_mask & july_mask].copy()
    
    if july_txs.empty:
        print("No transactions found for Rafa_AI in July.")
        return

    # 3. Calculate Merchant Stats for July
    total_volume = july_txs['eur_amount'].sum()
    fraud_volume = july_txs[july_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    fraud_rate = (fraud_volume / total_volume) if total_volume > 0 else 0.0
    
    # Get Merchant Static Data
    merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
    if not merchant_info:
        print(f"Merchant info not found for {merchant_name}")
        return
        
    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay = merchant_info['capture_delay']

    # 4. Identify Target Transactions (Fraudulent ones in July)
    target_txs = july_txs[july_txs['has_fraudulent_dispute'] == True].copy()
    
    if target_txs.empty:
        print("No fraudulent transactions to analyze.")
        return

    # Analyze Current State
    current_aci_counts = target_txs['aci'].value_counts()
    dominant_aci = current_aci_counts.idxmax()
    
    print(f"Analyzing {len(target_txs)} fraudulent transactions.")
    print(f"Merchant Stats - Vol: €{total_volume:,.2f}, Fraud Rate: {fraud_rate:.2%}")
    print(f"Current ACI Distribution: {current_aci_counts.to_dict()}")

    # 5. Simulate Costs for each ACI
    possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    aci_costs = {}

    for test_aci in possible_acis:
        total_fee_for_aci = 0.0
        valid_aci = True
        
        for _, tx in target_txs.iterrows():
            is_intra = (tx['issuing_country'] == tx['acquirer_country'])
            
            ctx = {
                'card_scheme': tx['card_scheme'],
                'account_type': account_type,
                'merchant_category_code': mcc,
                'monthly_volume': total_volume,
                'monthly_fraud_rate': fraud_rate,
                'is_credit': bool(tx['is_credit']),
                'aci': test_aci,
                'intracountry': is_intra,
                'capture_delay': capture_delay
            }
            
            # Find matching fee rule
            matched_rule = None
            for rule in fees:
                if match_fee_rule(ctx, rule):
                    matched_rule = rule
                    break
            
            if matched_rule:
                fee = calculate_fee(tx['eur_amount'], matched_rule)
                total_fee_for_aci += fee
            else:
                # If no rule matches, this ACI is invalid for this merchant/transaction profile
                valid_aci = False
                break
        
        if valid_aci:
            aci_costs[test_aci] = total_fee_for_aci
        else:
            aci_costs[test_aci] = float('inf')

    # 6. Determine Preferred Choice
    print("\nResults per ACI:")
    sorted_costs = sorted(aci_costs.items(), key=lambda x: x[1])
    for aci, cost in sorted_costs:
        if cost != float('inf'):
            print(f"ACI {aci}: €{cost:.2f}")
        else:
            print(f"ACI {aci}: Invalid (No matching rules)")

    # Select best valid ACI that is DIFFERENT from current dominant
    # The question asks "if we were to move... what would be the preferred choice"
    # This implies the choice must be a destination (different from origin).
    
    best_aci = None
    for aci, cost in sorted_costs:
        if cost == float('inf'):
            continue
        if aci != dominant_aci:
            best_aci = aci
            break
            
    # Fallback: if all valid ACIs are the same as dominant (unlikely), or only dominant is valid
    if best_aci is None:
        best_aci = sorted_costs[0][0] # Stick with current if no other option

    print(f"\nPreferred ACI (Different from {dominant_aci}): {best_aci}")
    
    # Final Answer Output
    print(best_aci)

if __name__ == "__main__":
    execute_analysis()