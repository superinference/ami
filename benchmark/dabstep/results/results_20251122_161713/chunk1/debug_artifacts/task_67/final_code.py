import pandas as pd
import json
import numpy as np

# --- Helper Functions ---
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
            return None
    return float(value)

def parse_volume_to_numeric(vol_str):
    """Parses monthly_volume strings (e.g., '100k-1m') to a numeric proxy."""
    if pd.isna(vol_str):
        return None
    s = str(vol_str).lower().replace('€', '').replace(',', '').strip()
    
    # Handle suffixes
    if 'k' in s: s = s.replace('k', '000')
    if 'm' in s: s = s.replace('m', '000000')
    
    # Handle ranges and operators
    if '-' in s:
        parts = s.split('-')
        try:
            return (float(parts[0]) + float(parts[1])) / 2
        except:
            return None
    if '<' in s:
        try:
            return float(s.replace('<', '')) * 0.5 # Proxy for lower
        except: return None
    if '>' in s:
        try:
            return float(s.replace('>', '')) * 1.5 # Proxy for higher
        except: return None
        
    try:
        return float(s)
    except:
        return None

def parse_fraud_to_numeric(fraud_str):
    """Parses monthly_fraud_level strings (e.g., '>8.3%') to a numeric proxy."""
    if pd.isna(fraud_str):
        return None
    s = str(fraud_str).replace('%', '').strip()
    
    if '-' in s:
        parts = s.split('-')
        try:
            return (float(parts[0]) + float(parts[1])) / 2
        except: return None
    if '<' in s:
        try:
            return float(s.replace('<', '')) * 0.5
        except: return None
    if '>' in s:
        try:
            return float(s.replace('>', '')) * 1.1
        except: return None
        
    try:
        return float(s)
    except:
        return None

def parse_delay_to_numeric(delay_str):
    """Maps capture_delay strings to a numeric 'days' proxy."""
    if pd.isna(delay_str):
        return None
    # Mapping based on logical delay time
    mapping = {
        'immediate': 0,
        '<3': 1.5,   # Avg of 0-3
        '3-5': 4,    # Avg of 3-5
        '>5': 6,     # More than 5
        'manual': 10 # Assume manual is the slowest/longest delay
    }
    return mapping.get(delay_str, None)

# --- Main Analysis ---
try:
    # 1. Load Data
    fees_path = '/output/chunk1/data/context/fees.json'
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    df_fees = pd.DataFrame(fees_data)

    # 2. Analyze Monthly Volume
    # Hypothesis: Higher Volume -> Lower Rate (Negative Correlation)
    df_vol = df_fees.dropna(subset=['monthly_volume']).copy()
    df_vol['vol_numeric'] = df_vol['monthly_volume'].apply(parse_volume_to_numeric)
    # Group by unique values to see the trend clearly
    vol_stats = df_vol.groupby('vol_numeric')['rate'].mean().sort_index()
    corr_vol = df_vol['vol_numeric'].corr(df_vol['rate'])
    
    print(f"--- Monthly Volume Analysis ---")
    print(f"Correlation: {corr_vol:.4f}")
    print("Average Rate by Volume (Low to High):")
    print(vol_stats)

    # 3. Analyze Monthly Fraud Level
    # Hypothesis: Higher Fraud -> Higher Rate (Positive Correlation)
    df_fraud = df_fees.dropna(subset=['monthly_fraud_level']).copy()
    df_fraud['fraud_numeric'] = df_fraud['monthly_fraud_level'].apply(parse_fraud_to_numeric)
    fraud_stats = df_fraud.groupby('fraud_numeric')['rate'].mean().sort_index()
    corr_fraud = df_fraud['fraud_numeric'].corr(df_fraud['rate'])
    
    print(f"\n--- Monthly Fraud Level Analysis ---")
    print(f"Correlation: {corr_fraud:.4f}")
    print("Average Rate by Fraud Level (Low to High):")
    print(fraud_stats)

    # 4. Analyze Capture Delay
    # Hypothesis: Higher Delay (Slower) -> Lower Rate (Negative Correlation)
    df_delay = df_fees.dropna(subset=['capture_delay']).copy()
    df_delay['delay_numeric'] = df_delay['capture_delay'].apply(parse_delay_to_numeric)
    delay_stats = df_delay.groupby('delay_numeric')['rate'].mean().sort_index()
    corr_delay = df_delay['delay_numeric'].corr(df_delay['rate'])
    
    print(f"\n--- Capture Delay Analysis ---")
    print(f"Correlation: {corr_delay:.4f}")
    print("Average Rate by Delay (Short/0 to Long/10):")
    print(delay_stats)

    # 5. Determine Factors
    # We are looking for factors where "value is increased" -> "cheaper fee rate".
    # This means a NEGATIVE correlation (As X goes up, Y goes down).
    
    contributing_factors = []
    
    # Threshold for significance (simple negative slope check)
    if corr_vol < -0.1:
        contributing_factors.append("monthly_volume")
    
    if corr_fraud < -0.1:
        contributing_factors.append("monthly_fraud_level")
        
    if corr_delay < -0.1:
        contributing_factors.append("capture_delay")

    print("\n--- Conclusion ---")
    print("Factors that contribute to a cheaper fee rate if increased:")
    print(contributing_factors)

except Exception as e:
    print(f"An error occurred: {e}")