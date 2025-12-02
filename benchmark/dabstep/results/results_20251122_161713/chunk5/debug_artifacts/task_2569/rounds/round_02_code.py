# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2569
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 5060 characters (FULL CODE)
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
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
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

    print(f"Analyzing Fee 17: {json.dumps(fee_17, indent=2)}")

    # 3. Identify Merchants matching Fee 17's Transaction-Level Criteria
    # Criteria: card_scheme, is_credit, aci (from payments.csv)
    
    # Start with all payments
    mask = pd.Series(True, index=payments.index)

    # Filter by Card Scheme
    if fee_17.get('card_scheme'):
        mask &= (payments['card_scheme'] == fee_17['card_scheme'])

    # Filter by Credit/Debit
    if fee_17.get('is_credit') is not None:
        mask &= (payments['is_credit'] == fee_17['is_credit'])

    # Filter by ACI (Fee has list, Payment has single value)
    if is_not_empty(fee_17.get('aci')):
        mask &= (payments['aci'].isin(fee_17['aci']))

    # Get potential merchants from matching transactions
    matching_txs = payments[mask]
    potential_merchants = matching_txs['merchant'].unique()
    
    print(f"\nMerchants with transactions matching Fee 17 criteria: {list(potential_merchants)}")

    # 4. Filter by Merchant-Level Criteria and Check Account Type
    # Criteria: merchant_category_code (from merchant_data.json)
    # Also check the ORIGINAL account_type rule to see who it applied to before.
    
    affected_merchants = []
    
    print("\n--- Impact Analysis ---")
    print(f"Proposed Change: Restrict Fee 17 to Account Type 'S' only.")
    
    for m_name in potential_merchants:
        # Get merchant info
        m_info = next((m for m in merchant_data if m['merchant'] == m_name), None)
        if not m_info:
            continue

        # Check MCC match (Fee 17 criteria)
        fee_mccs = fee_17.get('merchant_category_code')
        if is_not_empty(fee_mccs):
            if m_info['merchant_category_code'] not in fee_mccs:
                continue # Merchant doesn't match basic fee criteria

        # Check Capture Delay match (if applicable)
        # (Simplified check: if fee requires specific delay, check it. 
        # Assuming for this question we focus on the categorical match first)
        
        # At this point, the merchant MATCHES the original Fee 17 criteria 
        # (assuming original account_type was [] or included them).
        
        # Check Account Type
        m_account_type = m_info.get('account_type')
        
        # Determine if they are affected
        # If they are NOT type 'S', they lose the fee (Affected)
        # If they ARE type 'S', they keep the fee (Not Affected / Status Quo)
        
        if m_account_type == 'S':
            status = "Matches New Rule (Not Affected)"
        else:
            status = "DOES NOT Match New Rule (Affected - Excluded)"
            affected_merchants.append(m_name)
            
        print(f"Merchant: {m_name:<25} | Type: {m_account_type} | Status: {status}")

    # 5. Final Output
    print("\n" + "="*30)
    print("ANSWER")
    print("="*30)
    if affected_merchants:
        # The question asks "which merchants would have been affected".
        # These are the merchants who matched the fee originally but are excluded by the 'S' restriction.
        print(", ".join(sorted(affected_merchants)))
    else:
        print("No merchants affected.")

if __name__ == "__main__":
    analyze_fee_impact()
