# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1805
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8386 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- HELPER FUNCTIONS ---
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
        # Handle 'k' and 'm' for volumes
        if v.lower().endswith('k'):
            return float(v[:-1]) * 1000
        if v.lower().endswith('m'):
            return float(v[:-1]) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value) if value is not None else 0.0

def parse_volume_range(range_str):
    """Parses volume range strings like '100k-1m', '<10k', '>1m'."""
    if not range_str:
        return (float('-inf'), float('inf'))
    
    s = range_str.lower().strip()
    
    def parse_val(val_str):
        val_str = val_str.strip()
        mult = 1
        if val_str.endswith('k'):
            mult = 1000
            val_str = val_str[:-1]
        elif val_str.endswith('m'):
            mult = 1000000
            val_str = val_str[:-1]
        return float(val_str) * mult

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif s.startswith('>'):
        return (parse_val(s[1:]), float('inf'))
    elif s.startswith('<'):
        return (float('-inf'), parse_val(s[1:]))
    else:
        val = parse_val(s)
        return (val, val)

def parse_fraud_range(range_str):
    """Parses fraud range strings like '0%-1%', '>5%', '<2.5%'."""
    if not range_str:
        return (float('-inf'), float('inf'))
    
    s = range_str.strip()
    
    def parse_pct(val_str):
        val_str = val_str.strip().replace('%', '')
        return float(val_str) / 100

    if '-' in s:
        parts = s.split('-')
        return (parse_pct(parts[0]), parse_pct(parts[1]))
    elif s.startswith('>'):
        return (parse_pct(s[1:]), float('inf'))
    elif s.startswith('<'):
        return (float('-inf'), parse_pct(s[1:]))
    else:
        val = parse_pct(s)
        return (val, val)

def match_fee_rule(tx_context, rule):
    """
    Checks if a fee rule applies to a transaction context.
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
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay (Complex match)
    if rule.get('capture_delay'):
        rd = str(rule['capture_delay'])
        md = str(tx_context['capture_delay'])
        
        if rd == md:
            pass # Exact match
        elif md == 'manual':
            return False # Manual only matches manual/null
        elif rd == 'manual':
            return False # Rule manual only matches merchant manual
        else:
            # Numeric comparison
            try:
                md_val = 0.0 if md == 'immediate' else float(md)
                if rd.startswith('>'):
                    if not (md_val > float(rd[1:])): return False
                elif rd.startswith('<'):
                    if not (md_val < float(rd[1:])): return False
                elif '-' in rd:
                    low, high = map(float, rd.split('-'))
                    if not (low <= md_val <= high): return False
                elif rd == 'immediate':
                    if md_val != 0.0: return False
                else:
                    if md_val != float(rd): return False
            except ValueError:
                return False

    # 5. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_vol, max_vol = parse_volume_range(rule['monthly_volume'])
        if not (min_vol <= tx_context['monthly_volume'] <= max_vol):
            return False

    # 6. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_fraud, max_fraud = parse_fraud_range(rule['monthly_fraud_level'])
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

    # 9. Intracountry (Bool match)
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    return True

def main():
    # Paths
    payments_path = '/output/chunk6/data/context/payments.csv'
    fees_path = '/output/chunk6/data/context/fees.json'
    merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

    # 1. Load Data
    print("Loading data...")
    try:
        df_payments = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        with open(merchant_data_path, 'r') as f:
            merchant_data = json.load(f)
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Rafa_AI in July 2023
    merchant_name = "Rafa_AI"
    target_year = 2023
    start_day = 182
    end_day = 212

    print(f"Filtering for {merchant_name} in July 2023 (Days {start_day}-{end_day})...")
    df_filtered = df_payments[
        (df_payments['merchant'] == merchant_name) &
        (df_payments['year'] == target_year) &
        (df_payments['day_of_year'] >= start_day) &
        (df_payments['day_of_year'] <= end_day)
    ].copy()

    if df_filtered.empty:
        print("No transactions found.")
        return

    # 3. Calculate Monthly Stats
    total_volume = df_filtered['eur_amount'].sum()
    fraud_volume = df_filtered[df_filtered['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    print(f"Total Volume: {total_volume:.2f}")
    print(f"Fraud Rate: {fraud_rate:.4%}")

    # 4. Get Merchant Attributes
    merchant_info = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
    if not merchant_info:
        print(f"Merchant {merchant_name} not found in merchant_data.json")
        return

    account_type = merchant_info['account_type']
    mcc = merchant_info['merchant_category_code']
    capture_delay = merchant_info['capture_delay']
    
    print(f"Merchant Attributes: Account={account_type}, MCC={mcc}, Delay={capture_delay}")

    # 5. Identify Unique Transaction Profiles
    # Create intracountry column
    df_filtered['intracountry'] = df_filtered['issuing_country'] == df_filtered['acquirer_country']

    # Get unique profiles
    unique_profiles = df_filtered[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates()
    print(f"Unique transaction profiles: {len(unique_profiles)}")

    applicable_fee_ids = set()

    # 6. Match Fees
    for _, row in unique_profiles.iterrows():
        tx_context = {
            'card_scheme': row['card_scheme'],
            'account_type': account_type,
            'mcc': mcc,
            'is_credit': bool(row['is_credit']),
            'aci': row['aci'],
            'intracountry': bool(row['intracountry']),
            'monthly_volume': total_volume,
            'monthly_fraud_rate': fraud_rate,
            'capture_delay': capture_delay
        }

        for rule in fees_data:
            if match_fee_rule(tx_context, rule):
                applicable_fee_ids.add(rule['ID'])

    # 7. Output
    sorted_ids = sorted(list(applicable_fee_ids))
    print("\nApplicable Fee IDs:")
    print(sorted_ids)

if __name__ == "__main__":
    main()
