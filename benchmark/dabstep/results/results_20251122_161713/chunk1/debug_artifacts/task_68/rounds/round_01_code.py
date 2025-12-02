# ═══════════════════════════════════════════════════════════
# Round 1 - Task 68
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 5103 characters (FULL CODE)
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


import re

def main():
    # Path to the manual file
    manual_path = '/output/chunk1/data/context/manual.md'
    
    # Read the manual file
    try:
        with open(manual_path, 'r') as f:
            manual_text = f.read()
    except FileNotFoundError:
        print(f"Error: File not found at {manual_path}")
        return

    # Extract Section 5: "Understanding Payment Processing Fees"
    start_marker = "## 5. Understanding Payment Processing Fees"
    start_index = manual_text.find(start_marker)
    
    if start_index == -1:
        print("Section 5 not found in manual.md")
        return

    # Find the start of the next section (e.g., "## 6.") to delimit Section 5
    # We search starting from just after the start_marker
    next_section_match = re.search(r'\n## \d+\.', manual_text[start_index + len(start_marker):])
    
    if next_section_match:
        end_index = start_index + len(start_marker) + next_section_match.start()
        section_text = manual_text[start_index:end_index]
    else:
        section_text = manual_text[start_index:]

    print("--- Extracted Section 5 Content ---")
    # Print the section text for verification
    print(section_text.strip())
    print("\n--- Analysis of Relationships ---")

    # We need to identify factors where DECREASING the value leads to CHEAPER fees.
    # Let's parse the descriptions for the key factors mentioned in the plan: capture_delay, monthly_fraud_level, monthly_volume.

    factors_of_interest = ['capture_delay', 'monthly_fraud_level', 'monthly_volume']
    candidates = []

    # Split into lines to find the definitions
    lines = section_text.split('\n')
    
    for factor in factors_of_interest:
        # Find the line defining this factor
        factor_line = next((line for line in lines if f"**{factor}**" in line), "")
        
        if not factor_line:
            continue
            
        print(f"\nFactor: {factor}")
        print(f"Description: {factor_line.strip()}")
        
        # Logic derived from reading the text (simulated here based on known content of manual.md):
        
        if factor == 'capture_delay':
            # Text says: "The faster the capture to settlement happens, the more expensive it is."
            # Value: Days (e.g., 1, 2, 5). 
            # Decrease Value -> Fewer Days -> Faster -> More Expensive.
            # Result: NO.
            print("-> Relationship: Decreasing delay (faster) increases cost.")
            
        elif factor == 'monthly_fraud_level':
            # Text says: "payment processors will become more expensive as fraud rate increases."
            # Value: Fraud Rate %.
            # Decrease Value -> Lower Fraud Rate -> Less Expensive (Cheaper).
            # Result: YES.
            print("-> Relationship: Decreasing fraud level decreases cost (Cheaper).")
            candidates.append(factor)
            
        elif factor == 'monthly_volume':
            # Text says: "merchants with higher volume are able to get cheaper fees"
            # Value: Volume Amount.
            # Decrease Value -> Lower Volume -> More Expensive (Not Cheaper).
            # Result: NO.
            print("-> Relationship: Decreasing volume increases cost (loss of bulk discount).")

    print("\n--- Final Answer ---")
    print(f"The factor that contributes to a cheaper fee rate if its value is decreased is: {', '.join(candidates)}")

if __name__ == "__main__":
    main()
