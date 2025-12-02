import pandas as pd
import json

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
        return float(v)
    return float(value)

def check_capture_delay(merchant_delay, rule_delay):
    """
    Check if a merchant's capture delay matches the rule's requirement.
    merchant_delay: specific value (e.g., '1', 'manual', 'immediate')
    rule_delay: requirement (e.g., '3-5', '>5', '<3', 'immediate', 'manual', None)
    """
    if rule_delay is None:
        return True # Wildcard matches everything
        
    # Exact match (handles 'manual', 'immediate', specific numbers as strings)
    if str(rule_delay) == str(merchant_delay):
        return True
    
    # Numeric comparisons for ranges
    # Only proceed if merchant_delay is a number (e.g., '1', '7')
    if str(merchant_delay).isdigit():
        days = int(merchant_delay)
        if rule_delay == '>5':
            return days > 5
        if rule_delay == '<3':
            return days < 3
        if rule_delay == '3-5':
            return 3 <= days <= 5
            
    return False

# Define file paths
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

# Load data
with open(fees_path, 'r') as f:
    fees = json.load(f)

with open(merchant_data_path, 'r') as f:
    merchants = json.load(f)

# 1. Identify Merchants with Account Type 'S'
s_merchants = [m for m in merchants if m.get('account_type') == 'S']

# 2. Identify SwiftCharge Rules
swift_fees = [f for f in fees if f.get('card_scheme') == 'SwiftCharge']

# 3. Match Rules to Merchants and Calculate Fees
transaction_value = 4321.0
applicable_fees = []

for m in s_merchants:
    m_name = m.get('merchant')
    m_mcc = m.get('merchant_category_code')
    m_delay = m.get('capture_delay')
    m_account = m.get('account_type') # Should be 'S'
    
    for rule in swift_fees:
        # Check Account Type (Rule must include 'S' or be wildcard/empty list)
        if rule.get('account_type') and m_account not in rule['account_type']:
            continue
            
        # Check MCC (Rule must include merchant's MCC or be wildcard/empty list)
        if rule.get('merchant_category_code') and m_mcc not in rule['merchant_category_code']:
            continue
            
        # Check Capture Delay
        if not check_capture_delay(m_delay, rule.get('capture_delay')):
            continue
            
        # If we reach here, the rule is applicable based on the merchant's static profile.
        # We include all matching rules (covering potential variations in dynamic fields like 
        # credit/debit, fraud levels, etc.) to calculate the theoretical average.
        
        fixed = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        # Fee Calculation: fixed + (rate * amount / 10000)
        # Note: rate is an integer to be divided by 10000
        fee = fixed + (rate * transaction_value / 10000)
        applicable_fees.append(fee)

# 4. Calculate Average
if not applicable_fees:
    print("No applicable fees found.")
else:
    avg_fee = sum(applicable_fees) / len(applicable_fees)
    # Output formatted to 6 decimals as requested
    print(f"{avg_fee:.6f} EUR")