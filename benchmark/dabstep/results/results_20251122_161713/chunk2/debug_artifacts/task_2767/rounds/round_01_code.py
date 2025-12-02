# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2767
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2886 characters (FULL CODE)
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

def execute_step():
    # Load the payments dataset
    file_path = '/output/chunk2/data/context/payments.csv'
    df = pd.read_csv(file_path)

    # Filter the dataset based on the specified criteria:
    # 1. merchant is 'Belles_cookbook_store'
    # 2. year is 2023
    # 3. has_fraudulent_dispute is True
    filtered_df = df[
        (df['merchant'] == 'Belles_cookbook_store') &
        (df['year'] == 2023) &
        (df['has_fraudulent_dispute'] == True)
    ]

    # Output the results to verify the step
    print(f"Total transactions in dataset: {len(df)}")
    print(f"Transactions matching criteria (Belles_cookbook_store, 2023, Fraud): {len(filtered_df)}")
    
    # Display a sample of the filtered data, focusing on columns relevant to the overall goal (ACI, Amount)
    print("\nSample of filtered transactions:")
    print(filtered_df[['psp_reference', 'merchant', 'year', 'has_fraudulent_dispute', 'aci', 'eur_amount']].head())

    # Save the filtered data to a CSV for the next step in the analysis plan
    output_path = 'belles_fraud_2023.csv'
    filtered_df.to_csv(output_path, index=False)
    print(f"\nFiltered data saved to: {output_path}")

if __name__ == "__main__":
    execute_step()
