# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1512
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2794 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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

def safe_get(df, column, default=None):
    """Safely get column from DataFrame, return default if not exists."""
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

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

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default


import pandas as pd

def get_average_scenario_parameters():
    # File path
    payments_path = '/output/chunk6/data/context/payments.csv'
    
    # Load data
    # Using low_memory=False to ensure mixed types (if any) are handled, though schema suggests consistent types
    df = pd.read_csv(payments_path)
    
    # 1. Calculate Mode for ACI
    # The mode() method returns a Series, we take the first element [0]
    aci_mode = df['aci'].mode()[0]
    
    # 2. Calculate Mode for is_credit
    is_credit_mode = df['is_credit'].mode()[0]
    
    # 3. Calculate Mode for intracountry
    # Intracountry is defined as issuing_country == acquirer_country
    # We create a temporary Series for this calculation
    intracountry_series = df['issuing_country'] == df['acquirer_country']
    intracountry_mode = intracountry_series.mode()[0]
    
    # Print results in a structured format for the next step
    print("Average Scenario Parameters Calculated:")
    print(f"ACI: {aci_mode}")
    print(f"is_credit: {is_credit_mode}")
    print(f"intracountry: {intracountry_mode}")

if __name__ == "__main__":
    get_average_scenario_parameters()
