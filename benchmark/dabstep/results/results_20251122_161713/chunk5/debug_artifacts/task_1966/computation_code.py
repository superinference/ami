import pandas as pd
import json

# --- Helper Functions ---
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
        return float(v)
    return float(value)

def parse_range_check(value, rule_str):
    """Checks if value is within the rule_str range (e.g. '100k-1m', '>5', '8.3%')."""
    if rule_str is None:
        return True
    
    s = str(rule_str).strip()
    
    # Volume suffixes
    if 'k' in s.lower() or 'm' in s.lower():
        s = s.lower().replace('k', '000').replace('m', '000000')
    
    # Percentage
    is_pct = '%' in s
    s = s.replace('%', '')
    
    # Scale adjustment: if rule was %, convert to ratio (0.083)
    scale = 0.01 if is_pct else 1.0
    
    try:
        if '-' in s:
            low, high = s.split('-')
            l = float(low) * scale
            h = float(high) * scale
            return l <= value <= h
        elif s.startswith('>'):
            limit = float(s[1:]) * scale
            return value > limit
        elif s.startswith('<'):
            limit = float(s[1:]) * scale
            return value < limit
        else:
            return value == float(s) * scale
    except:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches rule requirement."""
    if rule_delay is None:
        return True
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    if r_delay == m_delay:
        return True
        
    if m_delay.isdigit():
        days = int(m_delay)
        if r_delay == '<3':
            return days < 3
        if r_delay == '>5':
            return days > 5
        if r_delay == '3-5':
            return 3 <= days <= 5
            
    return False

# --- Main Logic ---
def main():
    # Load Data
    payments_path = '/output/chunk5/data/context/payments.csv'
    fees_path = '/output/chunk5/data/context/fees.json'
    merchant_path = '/output/chunk5/data/context/merchant_data.json'

    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)

    # Parameters
    target_merchant = 'Belles_cookbook_store'
    target_fee_id = 398
    new_rate = 99

    # 1. Get Merchant Metadata
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print("Merchant not found")
        return

    m_account_type = merchant_info.get('account_type')
    m_mcc = merchant_info.get('merchant_category_code')
    m_capture_delay = merchant_info.get('capture_delay')

    # 2. Get Fee Rule
    fee_rule = next((f for f in fees_data if f['ID'] == target_fee_id), None)
    if not fee_rule:
        print("Fee ID not found")
        return

    old_rate = fee_rule['rate']

    # 3. Filter Transactions (October 2023)
    # October is Day 274 to 304 (inclusive)
    df_oct = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['day_of_year'] >= 274) &
        (df_payments['day_of_year'] <= 304)
    ].copy()

    # 4. Calculate Monthly Stats (Volume & Fraud)
    # Manual: "fraud levels measured as ratio between monthly total volume and monthly volume notified as fraud."
    monthly_volume = df_oct['eur_amount'].sum()
    fraud_volume = df_oct[df_oct['has_fraudulent_dispute']]['eur_amount'].sum()
    monthly_fraud_ratio = (fraud_volume / monthly_volume) if monthly_volume > 0 else 0.0

    # 5. Identify Matching Transactions
    affected_volume = 0.0

    for _, tx in df_oct.iterrows():
        # Check 1: Card Scheme
        if fee_rule['card_scheme'] != tx['card_scheme']:
            continue
            
        # Check 2: Account Type (Merchant)
        # If rule list is not empty, merchant type must be in it
        if fee_rule['account_type'] and m_account_type not in fee_rule['account_type']:
            continue
            
        # Check 3: MCC (Merchant)
        if fee_rule['merchant_category_code'] and m_mcc not in fee_rule['merchant_category_code']:
            continue
            
        # Check 4: ACI (Transaction)
        if fee_rule['aci'] and tx['aci'] not in fee_rule['aci']:
            continue
            
        # Check 5: Is Credit (Transaction)
        if fee_rule['is_credit'] is not None:
            if fee_rule['is_credit'] != tx['is_credit']:
                continue
                
        # Check 6: Intracountry (Transaction)
        # Logic: Issuer == Acquirer
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        if fee_rule['intracountry'] is not None:
            r_intra = fee_rule['intracountry']
            r_intra_bool = False
            if isinstance(r_intra, bool): r_intra_bool = r_intra
            elif isinstance(r_intra, (int, float)): r_intra_bool = (r_intra != 0)
            
            if r_intra_bool != is_intra:
                continue
                
        # Check 7: Capture Delay (Merchant)
        if not check_capture_delay(m_capture_delay, fee_rule['capture_delay']):
            continue
            
        # Check 8: Monthly Volume (Merchant)
        if not parse_range_check(monthly_volume, fee_rule['monthly_volume']):
            continue
            
        # Check 9: Monthly Fraud Level (Merchant)
        if not parse_range_check(monthly_fraud_ratio, fee_rule['monthly_fraud_level']):
            continue
            
        # Match found
        affected_volume += tx['eur_amount']

    # 6. Calculate Delta
    # Fee = Fixed + (Rate * Amount / 10000)
    # Delta = (New Rate - Old Rate) * Amount / 10000
    delta = (new_rate - old_rate) * affected_volume / 10000

    # Print with high precision as requested for delta questions
    print(f"{delta:.14f}")

if __name__ == "__main__":
    main()