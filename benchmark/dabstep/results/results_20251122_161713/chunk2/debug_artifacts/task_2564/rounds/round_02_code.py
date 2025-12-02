# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2564
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 5096 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import json
import pandas as pd

# ═══════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════
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
        return float(v)
    return float(value)

def is_not_empty(array):
    """Check if array/list is not empty. Handles numpy arrays safely."""
    if array is None:
        return False
    if hasattr(array, 'size'):  # numpy array
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

# ═══════════════════════════════════════════════════════════
# Main Analysis Script
# ═══════════════════════════════════════════════════════════

# File paths
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'
payments_path = '/output/chunk2/data/context/payments.csv'

# 1. Load Data
try:
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
    df_payments = pd.read_csv(payments_path)
except Exception as e:
    print(f"Error loading files: {e}")
    exit()

# 2. Get Fee Rule 384 Details
fee_rule_384 = next((item for item in fees_data if item["ID"] == 384), None)

if not fee_rule_384:
    print("Fee rule with ID 384 not found.")
    exit()

# Extract criteria from the fee rule
rule_scheme = fee_rule_384.get("card_scheme")
rule_aci = fee_rule_384.get("aci")
rule_is_credit = fee_rule_384.get("is_credit")
rule_mcc = fee_rule_384.get("merchant_category_code")
rule_account_type = fee_rule_384.get("account_type")
rule_intracountry = fee_rule_384.get("intracountry")

# 3. Prepare Merchant Lookup
# Map merchant name to {mcc, account_type} for fast checking
merchant_info = {}
for m in merchant_data:
    merchant_info[m['merchant']] = {
        'mcc': m['merchant_category_code'],
        'account_type': m['account_type']
    }

# 4. Filter Payments for Transaction-Level Matches
# We first isolate transactions that match the immutable parts of the rule 
# (scheme, aci, credit, intracountry). These are the "candidates" for the fee.

# Filter by Card Scheme
if rule_scheme:
    df_payments = df_payments[df_payments['card_scheme'] == rule_scheme]

# Filter by is_credit (if specified in rule)
if rule_is_credit is not None:
    df_payments = df_payments[df_payments['is_credit'] == rule_is_credit]

# Filter by ACI (if specified in rule)
if is_not_empty(rule_aci):
    df_payments = df_payments[df_payments['aci'].isin(rule_aci)]

# Filter by Intracountry (if specified in rule)
if rule_intracountry is not None:
    # Determine if transaction is intracountry (Issuer == Acquirer)
    is_intra_tx = df_payments['issuing_country'] == df_payments['acquirer_country']
    
    if rule_intracountry: # Rule requires Intracountry (True/1.0)
        df_payments = df_payments[is_intra_tx]
    else: # Rule requires Intercountry (False/0.0)
        df_payments = df_payments[~is_intra_tx]

# 5. Identify Affected Merchants
# Iterate through unique merchants found in the matching transactions
affected_merchants = set()
candidate_merchants = df_payments['merchant'].unique()

for merchant in candidate_merchants:
    if merchant not in merchant_info:
        continue
        
    info = merchant_info[merchant]
    m_mcc = info['mcc']
    m_type = info['account_type']
    
    # --- Check 1: Did the merchant match the ORIGINAL rule? ---
    matches_old = True
    # Check MCC (if rule has restrictions)
    if is_not_empty(rule_mcc) and m_mcc not in rule_mcc:
        matches_old = False
    # Check Account Type (Original rule criteria)
    if is_not_empty(rule_account_type) and m_type not in rule_account_type:
        matches_old = False
        
    # --- Check 2: Does the merchant match the NEW rule? ---
    # New Rule: Same MCC criteria, but Account Type MUST be 'O'
    matches_new = True
    # Check MCC (Same as old)
    if is_not_empty(rule_mcc) and m_mcc not in rule_mcc:
        matches_new = False
    # Check Account Type (New Requirement: Must be 'O')
    if m_type != 'O':
        matches_new = False
        
    # --- Determine if Affected ---
    # A merchant is affected if their fee applicability status changes.
    # (e.g., Applied -> Not Applied, or Not Applied -> Applied)
    if matches_old != matches_new:
        affected_merchants.add(merchant)

# 6. Output Results
if affected_merchants:
    print(", ".join(sorted(list(affected_merchants))))
else:
    print("No merchants affected")
