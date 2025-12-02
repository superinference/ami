# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1513
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3080 characters (FULL CODE)
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

def calculate_average_scenario_parameters():
    """
    Loads payments.csv and calculates the mode for merchant, is_credit, aci,
    and intracountry status to define the average scenario parameters.
    """
    # Load the payments dataset
    payments_path = '/output/chunk4/data/context/payments.csv'
    df = pd.read_csv(payments_path)

    # 1. Calculate the mode for 'merchant'
    # The mode() function returns a Series, we take the first element.
    merchant_mode = df['merchant'].mode()[0]

    # 2. Calculate the mode for 'is_credit'
    is_credit_mode = df['is_credit'].mode()[0]

    # 3. Calculate the mode for 'aci' (Authorization Characteristics Indicator)
    aci_mode = df['aci'].mode()[0]

    # 4. Determine the most common 'intracountry' status
    # Intracountry is defined as True if issuing_country is the same as acquirer_country, else False.
    # We create a temporary boolean Series for this comparison.
    intracountry_series = df['issuing_country'] == df['acquirer_country']
    intracountry_mode = intracountry_series.mode()[0]

    # Print the results to be used in the next steps of the analysis
    print("Average Scenario Parameters (Modes):")
    print(f"Merchant: {merchant_mode}")
    print(f"Is Credit: {is_credit_mode}")
    print(f"ACI: {aci_mode}")
    print(f"Intracountry: {intracountry_mode}")

if __name__ == "__main__":
    calculate_average_scenario_parameters()
