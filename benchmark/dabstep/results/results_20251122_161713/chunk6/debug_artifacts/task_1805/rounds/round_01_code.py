# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1805
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3171 characters (FULL CODE)
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

def load_and_filter_data():
    # Path to the payments file
    file_path = '/output/chunk6/data/context/payments.csv'
    
    print(f"Loading data from {file_path}...")
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print("Error: File not found.")
        return None

    # Filter parameters
    merchant_name = "Rafa_AI"
    target_year = 2023
    # July 2023 range: Day 182 to 212
    start_day = 182
    end_day = 212

    print(f"Filtering for Merchant: {merchant_name}, Year: {target_year}, Days: {start_day}-{end_day}...")

    # Apply filtering
    filtered_df = df[
        (df['merchant'] == merchant_name) &
        (df['year'] == target_year) &
        (df['day_of_year'] >= start_day) &
        (df['day_of_year'] <= end_day)
    ].copy()

    # Calculate metrics to verify against ground truth
    total_volume = filtered_df['eur_amount'].sum()
    fraud_volume = filtered_df[filtered_df['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = (fraud_volume / total_volume * 100) if total_volume > 0 else 0.0

    # Output results
    print("-" * 30)
    print(f"Filtered Transaction Count: {len(filtered_df)}")
    print(f"Total Volume: {total_volume:.2f}")
    print(f"Fraud Volume: {fraud_volume:.2f}")
    print(f"Fraud Rate: {fraud_rate:.4f}%")
    print("-" * 30)
    
    print("First 5 rows of filtered data:")
    print(filtered_df.head())

    return filtered_df

if __name__ == "__main__":
    df_rafa_july = load_and_filter_data()
