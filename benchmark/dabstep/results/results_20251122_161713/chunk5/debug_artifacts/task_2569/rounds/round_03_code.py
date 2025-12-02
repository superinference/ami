# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2569
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 5070 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value) if value is not None else 0.0

def is_not_empty(array):
    """Check if array/list is not empty."""
    if array is None:
        return False
    if hasattr(array, 'size'):
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """
    Checks if a merchant's capture delay matches the rule.
    Rule examples: '>5', '<3', '3-5', 'immediate', 'manual', or None.
    Merchant examples: '1', '7', 'immediate', 'manual'.
    """
    if rule_delay is None:
        return True
    
    # Direct string match (case-insensitive)
    if str(merchant_delay).lower() == str(rule_delay).lower():
        return True
        
    # Numeric comparison
    try:
        m_val = float(merchant_delay)
        if '>' in rule_delay:
            threshold = float(rule_delay.replace('>', ''))
            return m_val > threshold
        if '<' in rule_delay:
            threshold = float(rule_delay.replace('<', ''))
            return m_val < threshold
        if '-' in rule_delay:
            parts = rule_delay.split('-')
            if len(parts) == 2:
                low, high = map(float, parts)
                return low <= m_val <= high
    except ValueError:
        # merchant_delay might be 'manual' or 'immediate' which fails float conversion
        pass
        
    return False

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------
def analyze_fee_impact():
    # 1. Load Data
    try:
        fees = json.load(open('/output/chunk5/data/context/fees.json'))
        merchant_data = json.load(open('/output/chunk5/data/context/merchant_data.json'))
        payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Get Fee 17 Configuration
    fee_17 = next((f for f in fees if f['ID'] == 17), None)
    if not fee_17:
        print("Fee with ID 17 not found.")
        return

    # Fee 17 Criteria (from context/previous knowledge):
    # card_scheme: SwiftCharge
    # is_credit: True
    # aci: ['A']
    # capture_delay: '>5'
    # account_type: [] (Originally applies to all)
    
    # 3. Identify Merchants matching Fee 17's TRANSACTION criteria
    # Filter payments for transactions that trigger this fee
    mask = (payments['card_scheme'] == fee_17['card_scheme'])
    
    if fee_17.get('is_credit') is not None:
        mask &= (payments['is_credit'] == fee_17['is_credit'])
        
    if is_not_empty(fee_17.get('aci')):
        mask &= (payments['aci'].isin(fee_17['aci']))
        
    matching_txs = payments[mask]
    potential_merchants = matching_txs['merchant'].unique()
    
    # 4. Identify Merchants matching Fee 17's MERCHANT criteria (capture_delay)
    # And determine if they are affected by the change (account_type != 'S')
    
    affected_merchants = []
    
    for m_name in potential_merchants:
        m_info = next((m for m in merchant_data if m['merchant'] == m_name), None)
        if not m_info:
            continue
            
        # Check Capture Delay
        # Fee 17 requires capture_delay > 5. We check if merchant matches this.
        if not check_capture_delay(m_info.get('capture_delay'), fee_17.get('capture_delay')):
            continue
            
        # Check MCC (if fee has it)
        if is_not_empty(fee_17.get('merchant_category_code')):
            if m_info['merchant_category_code'] not in fee_17['merchant_category_code']:
                continue
                
        # If we are here, the merchant CURRENTLY pays Fee 17 (matches all original criteria).
        # Now check the proposed change: Restrict to account_type 'S'.
        
        m_account_type = m_info.get('account_type')
        
        # Affected Logic:
        # If merchant is type 'S', they continue to pay the fee (No change/Not affected).
        # If merchant is NOT type 'S', they stop paying the fee (Affected by the restriction).
        
        if m_account_type != 'S':
            affected_merchants.append(m_name)
            
    # 5. Output
    if affected_merchants:
        print(", ".join(sorted(affected_merchants)))
    else:
        print("No merchants affected.")

if __name__ == "__main__":
    analyze_fee_impact()
