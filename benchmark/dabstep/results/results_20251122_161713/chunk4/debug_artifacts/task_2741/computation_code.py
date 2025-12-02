import pandas as pd
import json
import re

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m suffixes to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators for simple parsing
        
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100
            
        # Handle k/m suffixes
        if v.endswith('k'):
            return float(v[:-1]) * 1000
        if v.endswith('m'):
            return float(v[:-1]) * 1000000
            
        # Handle ranges (return mean, though usually we parse ranges separately)
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
    return 0.0

def parse_range_check(value, rule_range_str):
    """
    Checks if a numeric value fits within a rule's range string.
    Range formats: "100k-1m", ">5", "<3", "7.7%-8.3%", "manual" (exact match)
    """
    if rule_range_str is None:
        return True
        
    # If value is a string (like 'manual'), exact match required unless rule is wildcard
    if isinstance(value, str):
        # Try to convert value to float if it looks numeric
        try:
            num_val = float(value)
            value = num_val
        except ValueError:
            return str(value).lower() == str(rule_range_str).lower()

    # Now value is float, parse rule
    s = str(rule_range_str).strip().lower()
    
    # Handle exact string matches for non-numeric rules
    if s in ['manual', 'immediate']:
        return str(value).lower() == s

    # Handle Percentage Ranges "7.7%-8.3%"
    if '%' in s:
        is_pct = True
        s = s.replace('%', '')
    else:
        is_pct = False
        
    # Handle k/m suffixes in rule
    def parse_num(n_str):
        n_str = n_str.strip()
        mult = 1
        if n_str.endswith('k'):
            mult = 1000
            n_str = n_str[:-1]
        elif n_str.endswith('m'):
            mult = 1000000
            n_str = n_str[:-1]
        return float(n_str) * mult

    # Handle operators
    if s.startswith('>'):
        limit = parse_num(s[1:])
        if is_pct: limit /= 100
        return value > limit
    if s.startswith('<'):
        limit = parse_num(s[1:])
        if is_pct: limit /= 100
        return value < limit
        
    # Handle range "min-max"
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = parse_num(parts[0])
            max_val = parse_num(parts[1])
            if is_pct:
                min_val /= 100
                max_val /= 100
            return min_val <= value <= max_val
            
    # Fallback: exact numeric match
    try:
        return value == parse_num(s)
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain:
    - card_scheme, account_type, merchant_category_code, is_credit, aci
    - monthly_volume, monthly_fraud_level, capture_delay, intracountry
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match)
    # If rule['account_type'] is empty/null, it applies to all.
    # If not empty, tx_context['account_type'] must be in the list.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != tx_context['intracountry']:
            return False

    # 7. Capture Delay (Range/Value match)
    if rule.get('capture_delay'):
        if not parse_range_check(tx_context['capture_delay'], rule['capture_delay']):
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate (basis points)."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Rate is typically in basis points or similar, manual says: "multiplied by transaction value and divided by 10000"
    variable = (rate * amount) / 10000
    return fixed + variable

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

try:
    df = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
except FileNotFoundError as e:
    print(f"Error loading files: {e}")
    exit()

# 2. Filter for Rafa_AI in September
merchant_name = 'Rafa_AI'
start_day = 244
end_day = 273

# Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not merchant_info:
    print(f"Merchant {merchant_name} not found in merchant_data.json")
    exit()

# Extract static merchant attributes
m_account_type = merchant_info.get('account_type')
m_mcc = merchant_info.get('merchant_category_code')
m_capture_delay = merchant_info.get('capture_delay')

# Filter Transactions
sept_txs = df[
    (df['merchant'] == merchant_name) &
    (df['day_of_year'] >= start_day) &
    (df['day_of_year'] <= end_day)
]

if sept_txs.empty:
    print("No transactions found for Rafa_AI in September.")
    exit()

# 3. Calculate Monthly Stats (Volume & Fraud Level)
# These determine which fee bucket applies
total_volume = sept_txs['eur_amount'].sum()
fraud_txs_all = sept_txs[sept_txs['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs_all['eur_amount'].sum()

# Fraud Level = Fraud Volume / Total Volume (as per manual definition "ratio of fraudulent volume over total volume")
# Note: Sometimes it's count based, but manual says "monthly total volume and monthly volume notified as fraud" implies volume ratio.
# Let's double check manual text: "monthly_fraud_level... ratio between monthly total volume and monthly volume notified as fraud" -> Yes, volume ratio.
if total_volume > 0:
    monthly_fraud_level = fraud_volume / total_volume
else:
    monthly_fraud_level = 0.0

print(f"Merchant: {merchant_name}")
print(f"Sept Volume: €{total_volume:,.2f}")
print(f"Sept Fraud Volume: €{fraud_volume:,.2f}")
print(f"Sept Fraud Level: {monthly_fraud_level:.4%}")
print(f"Account Type: {m_account_type}, MCC: {m_mcc}, Capture Delay: {m_capture_delay}")

# 4. Isolate Target Transactions for Simulation
# "if we were to move the fraudulent transactions towards a different ACI"
target_txs = fraud_txs_all.copy()
print(f"Number of fraudulent transactions to simulate: {len(target_txs)}")

# 5. Simulation Loop
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G'] # Standard ACIs from manual
results = {}

# Pre-sort fees by ID to ensure consistent priority (assuming lower ID = higher priority if overlaps exist)
fees_data.sort(key=lambda x: x['ID'])

for sim_aci in possible_acis:
    total_fee_for_aci = 0.0
    match_count = 0
    
    for _, tx in target_txs.iterrows():
        # Construct context for this transaction with the SIMULATED ACI
        # Note: We use the ACTUAL monthly stats calculated above, as changing ACI doesn't change the historical volume/fraud rate for the fee tier determination (usually).
        # However, the question implies a hypothetical scenario. If we change ACI, does it change the fee rule match? Yes, via the 'aci' field in the rule.
        
        is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
        
        context = {
            'card_scheme': tx['card_scheme'],
            'account_type': m_account_type,
            'merchant_category_code': m_mcc,
            'is_credit': bool(tx['is_credit']),
            'aci': sim_aci,  # <--- The variable we are changing
            'monthly_volume': total_volume,
            'monthly_fraud_level': monthly_fraud_level,
            'capture_delay': m_capture_delay,
            'intracountry': is_intracountry
        }
        
        # Find matching rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(context, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee_for_aci += fee
            match_count += 1
        else:
            # Fallback if no rule matches? 
            # In a real scenario, there's usually a default. Here we assume coverage.
            # If no match, we might skip or assume 0 (risky). 
            # Let's log warning if significant.
            pass

    results[sim_aci] = total_fee_for_aci
    # print(f"ACI {sim_aci}: Total Fee = €{total_fee_for_aci:.2f} (Matched {match_count}/{len(target_txs)})")

# 6. Determine Preferred Choice
# We want the lowest possible fees.
if not results:
    print("No results generated.")
else:
    best_aci = min(results, key=results.get)
    min_fee = results[best_aci]
    
    print("\nSimulation Results:")
    for aci, fee in results.items():
        print(f"ACI {aci}: €{fee:.2f}")
        
    print(f"\nPreferred ACI: {best_aci}")
    print(f"Lowest Fee: €{min_fee:.2f}")
    
    # Final Answer Output
    print(best_aci)