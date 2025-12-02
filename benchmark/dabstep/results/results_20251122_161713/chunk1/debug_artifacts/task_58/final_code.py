import pandas as pd

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

# Main Execution
try:
    # Load the payments.csv file
    file_path = '/output/chunk1/data/context/payments.csv'
    df = pd.read_csv(file_path)

    # Filter for merchant 'Crossfit_Hanna'
    merchant_filter = df['merchant'] == 'Crossfit_Hanna'

    # Filter for Q3 2023 (day_of_year between 182 and 273 inclusive)
    # Q3 is July 1 to Sept 30. In non-leap year:
    # Jan: 31, Feb: 28, Mar: 31, Apr: 30, May: 31, Jun: 30 = 181 days.
    # July 1 is day 182.
    # July (31) + Aug (31) + Sept (30) = 92 days.
    # 181 + 92 = 273.
    # So range 182-273 is correct.
    q3_filter = (df['day_of_year'] >= 182) & (df['day_of_year'] <= 273)

    # Apply both filters
    filtered_df = df[merchant_filter & q3_filter].copy()

    # Calculate Total Volume
    total_volume = filtered_df['eur_amount'].sum()

    # Calculate Fraud Volume
    # Fraud is indicated by 'has_fraudulent_dispute' == True
    fraud_volume = filtered_df[filtered_df['has_fraudulent_dispute'] == True]['eur_amount'].sum()

    # Calculate Fraud Rate
    # Manual.md Section 7: "Fraud is defined as the ratio of fraudulent volume over total volume."
    if total_volume > 0:
        fraud_rate = fraud_volume / total_volume
    else:
        fraud_rate = 0.0

    # Output the result
    # The question asks "What is the avg fraud rate".
    # I will print the percentage value.
    print(fraud_rate * 100)

except Exception as e:
    print(f"Error: {e}")