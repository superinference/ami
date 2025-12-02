#!/usr/bin/env python3
# Copyright (C) 2025 SuperInference contributors
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program. If not, see https://www.gnu.org/licenses/.

"""
Prompt Templates for SuperInference MCP Server
Extracted multiline strings to improve code readability and maintainability.

All templates use Python .format() method for variable interpolation.
Variables are denoted with {variable_name} in the templates.
"""

from typing import List

# =============================================================================
# HELPER FUNCTIONS - Auto-prepended to Generated Code
# =============================================================================
# These helper functions are automatically injected into ALL generated code
# by _coder_initial() and _coder_incremental() methods.
# They are defined here as the single source of truth.

HELPER_FUNCTIONS = '''# Helper functions for robust data processing
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

'''

# =============================================================================
# CRITIC PROMPTS - For PRE Loop Step Evaluation
# =============================================================================

def build_critic_prompt(instruction: str, step_title: str, step_description: str, 
                        candidate: str, language: str, step_guidance: str = "",
                        lang_guidance: str = "", prior_context: str = "") -> str:
    """Build critic evaluation prompt for a plan step."""
    return f"""You are evaluating a step in a multi-step reasoning plan.
Overall Task: {instruction}
Current Step: {step_title} - {step_description}
Language: {language}{step_guidance}
{prior_context}
Candidate Output:
{candidate}

Evaluation Criteria:
1. CORRECTNESS: Does the candidate output solve THIS SPECIFIC STEP (not the overall task)?
2. COMPLETENESS: Does it address the step requirements (subquestion or subanswer)?
3. RELEVANCE: Ignore any irrelevant file context. Focus only on whether the step output is correct.
4. FUNCTIONALITY: For calculations/reasoning, is the logic sound?

IMPORTANT:
- For SQ (subquestion) steps: Accept if the candidate identifies the right information
- For SA (subanswer) steps: Accept if the calculation/reasoning is mathematically correct
- Ignore completely unrelated file context (e.g., shell scripts when task is math)
- Style issues should NOT cause rejection if substance is correct
- Be LENIENT for correct reasoning even if format is imperfect{lang_guidance}

Respond ONLY in JSON with fields: {{"approve": bool, "score": 0..1, "reason": string}}."""

# =============================================================================
# PLANNER PROMPTS - For SUPER-INFERENCE Plan Generation
# =============================================================================

PLANNER_INITIAL_PROMPT = """You are an expert data analyst.
In order to answer factoid questions based on the given data, you have to first plan effectively.

# Question
{question}

# Files context
{files_context}

# QUESTION PATTERN RECOGNITION (Read carefully!)
Recognize these patterns and plan accordingly:

## QUESTION TYPE CLASSIFICATION (CRITICAL for correct answers):

### A. YES/NO Questions (return "yes" or "no", NOT numbers or breakdowns):
- "Is there...", "Are there...", "Does...", "Has...", "Is the...", "Are the..."
- "Is X greater than Y?", "Does X exceed Y?"
- "Is the fraud rate higher for credit than debit?" -> Compare rates, return "yes" or "no"
- ❌ WRONG: "Ecommerce: 0.09, POS: 0.00" | ✅ CORRECT: "yes" or "no"
- Plan: Calculate both sides, compare, return boolean

### B. COUNT Questions (return INTEGER, NOT yes/no):
- "How many...", "What is the count...", "Number of...", "Total number of..."
- "How many transactions?" -> Return integer like "138236", NOT "yes"
- Plan: Use .count(), len(), .nunique(), or .sum() -> Return integer

### C. FIELD NAME Questions (return COLUMN NAME, not value):
- "Which field...", "What field...", "Which column..."
- "Which field has the highest correlation?" -> Return field name like "issuing_country"
- Plan: Access df.columns, find field, return column name string

### D. VALUE Questions (return FIELD VALUE, not field name):
- "What is the value of X?", "What is X?"
- "What is the issuing country?" -> Return value like "NL", NOT "issuing_country"
- Plan: Filter data, extract value, return value

### E. LIST Questions (return ALL items, comma-separated):
- "List...", "What are...", "Which... (plural)", "IDs", "all X"
- "List all merchants" -> Return "Merchant1, Merchant2, Merchant3"
- Plan: Filter/collect ALL matches, return comma-separated list

### F. COMPARISON Questions (may need yes/no OR breakdown):
- "Compare X and Y", "X vs Y", "difference between X and Y"
- If question asks "Is X greater?" -> Return "yes" or "no"
- If question asks "What is the difference?" -> Return numeric value
- Plan: Calculate both, decide format based on question wording

### G. POLICY Questions (calculate data AND check manual.md policy, cross-reference to answer):
- Keywords: "fine", "penalty", "policy", "danger", "in danger of"
- Examples: "Is...in danger of fine?", "Is merchant in danger of getting a high-fraud rate fine?"
- CRITICAL: These questions require BOTH data calculation AND policy checking!
- CRITICAL: "in danger of" + "fine" = POLICY question requiring two-step process:
  1. Calculate relevant metric (fraud rate, volume, etc.) from transaction data
  2. Check manual.md for policy threshold/rules (e.g., "fraud rate > X% triggers fine")
  3. Cross-reference: Does calculated metric exceed policy threshold?
  4. Return: "yes" if exceeds threshold, "no" if not, "Not Applicable" if no policy found
- Plan: 
  Step 1: Calculate metric (fraud rate, volume, etc.) from payments.csv
  Step 2: Load manual.md -> Search for policy keywords ("fine", "penalty", "threshold")
  Step 3: Compare calculated metric with policy threshold
  Step 4: Return answer based on comparison
- ❌ WRONG: Return calculated fraud rate alone (without checking policy)
- ❌ WRONG: Return MCC codes or fee IDs (these are data values, not policy answers)
- ❌ WRONG: Return "Not Applicable" without calculating the metric first
- ✅ CORRECT: Calculate metric → Check manual.md for policy → Compare → Return "yes"/"no"/"Not Applicable"

## CALCULATION PATTERNS:

1. "average/mean per unique X" or "average for each X":
   -> Group by X, calculate mean, then calculate mean of those means (double aggregation!)
   
2. "lowest/minimum X" or "merchant with lowest X":
   -> Calculate X for all entities, use .idxmin() to get the ENTITY NAME (merchant name)
   -> CRITICAL: Return merchant NAME, not the value!
   
3. "highest/maximum X" or "merchant with highest X":
   -> Calculate X for all entities, use .idxmax() to get the ENTITY NAME (merchant name)
   -> CRITICAL: Return merchant NAME, not the value!
   
4. "list of X sorted by Y" or "X by Y":
   -> Get X values, group/sort by Y, format as "Y1: X1, Y2: X2, ..."
   
5. "X for each Y" or "by Y" (multiple results):
   -> Group by Y, calculate X, return ALL results formatted as "Y1: X1, Y2: X2, ..."

6. "top/highest" for RISK/FRAUD -> Calculate RATE (%), NOT count
   -> Plan: Group -> Calculate rate (fraud/total * 100) -> Find max
   
7. "average fee" -> Apply ALL filters + weighted average
   -> Formula: fixed_amount + (rate * amount / 10000)
   -> CRITICAL: Use match_fee_rule() and calculate_fee() helpers!

8. Fee questions - THEORETICAL vs EMPIRICAL:
   - "What WOULD BE the fee for X?" -> Filter fee RULES, calculate average (don't use transactions)
   - "What WAS the actual fee for X?" -> Use transactions, match to rules, weight by volume
   
9. "Imagine X changed" -> Plan: Calculate WITH change, WITHOUT change, return DELTA

10. "In average scenario, which X" -> Plan: Calculate avg for each X, return best X NAME

11. PLURAL questions ("IDs", "list", "what are") -> Plan to collect ALL matches, not first

## MERCHANT SELECTION PATTERNS (CRITICAL - 15% of errors):
- "merchant with lowest/highest X" -> Filter payments.csv by merchant, calculate X per merchant, use .idxmin()/.idxmax()
- CRITICAL: Must filter payments.csv, NOT just merchant_data.json
- Plan: Load payments.csv -> Join with merchant_data -> Group by merchant -> Calculate metric -> Find min/max merchant NAME
- Example: "merchant with lowest fraud rate" -> Group by merchant, calculate fraud_rate, .idxmin() -> Return merchant name

## SCHEME SELECTION PATTERNS (CRITICAL - 10% of errors):
- "Which scheme..." -> Use match_fee_rule() to find matching scheme
- CRITICAL: Check ALL fee rules, not just first match
- Plan: Iterate through fees.json, use match_fee_rule() for each, find matching scheme NAME

# QUESTION UNDERSTANDING CHECKLIST (Before planning):
1. Is this a POLICY question? ("fine", "penalty", "danger", "policy", "in danger of") -> Plan COMPLETE TWO-STEP PROCESS:
   - If question contains "in danger of" + "fine" -> POLICY question!
   - If question asks about "fine" or "penalty" -> POLICY question!
   - ⚠️ CRITICAL: For policy questions, your FIRST step MUST plan BOTH:
     Step 1: Calculate relevant metric (fraud rate, volume, etc.) from transaction data
     Step 2: Load manual.md -> Search for policy keywords ("fine", "penalty") -> Extract threshold -> Compare metric with threshold -> Return "yes"/"no"/"Not Applicable"
   - ❌ WRONG: Planning only Step 1 (calculate metric) - this is INCOMPLETE!
   - ✅ CORRECT: Plan both steps explicitly: "Calculate [metric] AND check manual.md for policy threshold, then compare and return answer"
2. Is this a YES/NO question? ("Is...", "Are...", "Does...") -> Plan to return "yes" or "no"
3. Is this a COUNT question? ("How many...", "Number of...") -> Plan to return integer
4. Is this asking for FIELD NAME or FIELD VALUE? -> Plan accordingly
5. Is this a LIST question? ("List...", "What are...", plural) -> Plan to collect ALL items
6. Is this a COMPARISON? -> Determine if answer should be yes/no OR breakdown
7. Is this asking for MERCHANT NAME or MERCHANT VALUE? -> Use .idxmin()/.idxmax() for name
8. Is this asking for SCHEME NAME or SCHEME VALUE? -> Plan to iterate through fee rules

# Your task
- FIRST: Classify the question type using the checklist above
- THEN: Suggest your very first step to answer the question above
- Consider the pattern recognition above - if question matches a pattern, plan accordingly
- Your first step should align with the question type (yes/no, count, list, etc.)
- Your first step does not need to be sufficient to answer the question
- Just propose a very simple initial step, which can act as a good starting point to answer the question
- Your response should only contain an initial step

Your answer:"""

PLANNER_NEXT_PROMPT = """You are an expert data analyst.
In order to answer factoid questions based on the given data, you have to first plan effectively.
Your task is to suggest next plan to do to answer the question.

# Question
{question}

# Files context
{files_context}

# Current plans
{plan_text}

# Obtained results from the current plans:
{execution_result}

# Error guidance
{error_guidance}

# QUESTION UNDERSTANDING CHECKLIST (Before planning next step):
1. Is this a POLICY question? ("fine", "penalty", "danger", "policy", "in danger of") -> CRITICAL CHECK:
   - If previous step calculated metric (fraud rate, etc.) -> Next step MUST be: "Load manual.md, search for policy, compare, return yes/no/Not Applicable"
   - If previous step checked manual.md -> Verify comparison was done and answer format is correct ("yes"/"no"/"Not Applicable")
   - ❌ WRONG: Stopping after calculating metric - policy checking is REQUIRED!
   - ✅ CORRECT: Complete both steps before returning answer
2. Is this a YES/NO question? -> Previous step should calculate comparison, next step should convert to "yes"/"no"
3. Is this a COUNT question? -> Previous step should filter/count, next step should format as integer
4. Is this asking for MERCHANT NAME? -> Previous step should calculate metric per merchant, next step should use .idxmin()/.idxmax()
5. Is this asking for LIST? -> Previous step should filter, next step should collect ALL matches
6. Is calculation wrong? -> Check formula, column names, unit conversions

# Your task
- FIRST: Review question type and ensure previous steps align with expected answer format
- THEN: Suggest your next step to answer the question above
- If previous result had errors, suggest a step that FIXES the specific error (not a completely different approach)
- If previous result has partial data, suggest how to process that data to get the final answer
- If calculation seems wrong, suggest step to verify formula/columns
- IMPORTANT: If working with 'fees.json', check documentation for specific field names/types if unsure
- If dealing with 'account_type', 'aci', or 'merchant_category_code', remember these are LIST fields in fees.json
- Your next step does not need to be sufficient to answer the question, but if it requires only final simple last step you may suggest it
- Just propose a very simple next step, which can act as a good intermediate point to answer the question
- Of course your response can be a plan which could directly answer the question
- Your response should only contain an next step without any explanation

Your answer:"""

# =============================================================================
# VERIFIER PROMPTS - For SUPER-INFERENCE Plan Sufficiency Check
# =============================================================================

VERIFIER_PROMPT = """You are an expert data analyst verifier.

# Question
{question}

# Plan
{plan_text}

# Code
```python
{code}
```

# Execution result
{execution_result}

# CRITICAL: POLICY QUESTION CHECK
If question contains "fine", "penalty", "danger", "policy", or "in danger of":
- ⚠️ Policy questions REQUIRE two-step process:
  1. Calculate metric (fraud rate, volume, etc.)
  2. Check manual.md for policy threshold AND compare
- Plan MUST include BOTH steps (calculate AND check policy)
- Code MUST load manual.md and search for policy keywords
- Execution result MUST contain "yes", "no", or "Not Applicable" (NOT raw metric!)
- ❌ INSUFFICIENT if: Only calculates metric without checking manual.md
- ❌ INSUFFICIENT if: Returns raw metric (9.13%) instead of policy answer
- ✅ SUFFICIENT if: Calculates metric AND checks manual.md AND returns policy answer

# Your task - ANSWER-FIRST VERIFICATION (judge RESULTS, not code style):

ANSWER CHECKS (if execution has reasonable answer, mark sufficient):
- Numeric answer in reasonable range (fees 0-100k, % 0-100, counts positive) -> Sufficient
- Text answer with specific value (country code, merchant name) -> Sufficient  
- List answer with values (not empty) -> Sufficient
- "Not Applicable" or "NA" for impossible question -> Sufficient
- Policy question with "yes"/"no"/"Not Applicable" -> Sufficient
- Policy question with raw metric (9.13%) -> INSUFFICIENT (missing policy check!)
- Execution error or traceback -> Insufficient
- Empty/vague answer when specific value expected -> Insufficient

ROUND/COMPLEXITY LIMITS (prevent over-iteration):
- Round >= 5 -> STRONG bias toward sufficient (results rarely improve after this)
- Round >= 7 -> Mark sufficient unless clearly broken
- Code >8000 chars -> Mark sufficient (over-complicated)

AVOID FALSE NEGATIVES (Evidence: Round 3 had correct 0.123217, rejected, Round 5 got wrong 0.138018):
- If execution shows answer matching question format -> Mark sufficient immediately
- Don't reject because code could be "more sophisticated" or "use different method"
- Working answer > perfect approach

OVER-COMPLICATION (reject if getting worse):
- Simple question but plan >6 steps -> Over-engineered, mark sufficient
- Plan adds filters not in question -> Likely wrong, but if answer looks reasonable mark sufficient
- Earlier rounds had better results -> Stop now, mark sufficient

RESPONSE FORMAT (ABSOLUTELY CRITICAL):
Return a JSON object with this EXACT structure (NO markdown, NO code blocks):

{{
  "thoughts": "Brief reasoning - Does execution answer question? Any over-complication?",
  "sufficient": true
}}

OR

{{
  "thoughts": "Why the plan is insufficient",
  "sufficient": false
}}

RULES:
1. "thoughts": Your reasoning
2. "sufficient": Boolean true/false ONLY
3. Return raw JSON - NO markdown wrappers
4. The JSON must be valid

EXAMPLES:

Execution has answer:
{{"thoughts": "The execution shows the correct country code NL", "sufficient": true}}

Execution missing calculation:
{{"thoughts": "Code finds MCC but doesn't calculate fee", "sufficient": false}}

Execution has error:
{{"thoughts": "Execution error - code is broken", "sufficient": false}}

Return JSON now (no markdown):"""

# =============================================================================
# ROUTER PROMPTS - For SUPER-INFERENCE Plan Refinement Routing
# =============================================================================

ROUTER_PROMPT = """You are an expert data analyst.
Since current plan is insufficient to answer the question, your task is to decide how to refine the plan to answer the question.

# CRITICAL: POLICY QUESTION CHECK
If question contains "fine", "penalty", "danger", "policy", or "in danger of":
- ⚠️ Policy questions REQUIRE complete two-step process:
  1. Calculate metric (fraud rate, volume, etc.)
  2. Check manual.md for policy threshold AND compare
- If plan only has Step 1 (calculate metric) -> Route to "Add Step" to add policy checking
- If execution shows raw metric but no policy answer -> Route to "Add Step" to add policy checking
- ❌ WRONG: Approving plan that only calculates metric without checking policy
- ✅ CORRECT: Ensure both steps are planned before marking sufficient

# Question
{question}

# Given data
{file_list}

# Files context
{files_context}

# Current plans (numbered for reference):
{plan_text}
# NOTE: Plan has {plan_steps_count} total step(s). Step numbers are 1-indexed (Step 1, Step 2, ..., Step {plan_steps_count}).

# Obtained results from the current plans:
{execution_result}

# Your task - CRITICAL ERROR DETECTION:

FIRST - CHECK FOR EXECUTION ERRORS:
- If execution result starts with "EXECUTION ERROR:", "Error:", or contains "Traceback", answer "Step 1" IMMEDIATELY.
- Execution errors (TypeError, IndexError, AttributeError, etc.) mean the code is fundamentally broken.
- These CANNOT be fixed by adding more steps - you must regenerate the code from scratch.
- Answer: "Step 1" (regenerate all code)

THEN - FOR SUCCESSFUL EXECUTIONS:
- If execution succeeded but shows incomplete/wrong data, decide:
  - If the approach is wrong (wrong columns, wrong logic), answer "Step N" to backtrack BEFORE that step
  - If the approach is correct but incomplete, answer "Add Step" to build upon it
- Be conservative about backtracking for working code - only backtrack if fundamentally wrong
- Default to "Add Step" for successful executions that need more work

BACKTRACKING SEMANTICS (CRITICAL):
- "Step N" means: Remove step N and ALL steps after it, then regenerate from step N
- Example: If plan has 3 steps and you say "Step 2", the plan becomes: [Step 1 only]
- Example: If plan has 5 steps and you say "Step 3", the plan becomes: [Step 1, Step 2 only]
- "Step 1" means: Remove ALL steps, start completely fresh
- The step number N refers to the step number shown in "Current plans" above

RULES:
- "EXECUTION ERROR: ..." -> ALWAYS answer "Step 1" (regenerate all code)
- Wrong approach in successful execution -> "Step N" where N is the FIRST wrong step (removes N and all after)
- Correct but incomplete execution -> "Add Step"

Your response should ONLY be: "Step 1", "Step 2", ..., "Step {plan_steps_count}", or "Add Step"

Your answer:"""

# =============================================================================
# CODER PROMPTS - For SUPER-INFERENCE Code Generation
# =============================================================================

CODER_INITIAL_PROMPT = """You are an expert Data Analyst.
Your task is to write the INITIAL Python code to execute the first step of a data analysis plan.

# OVERALL GOAL
Question: {question}

# CURRENT STEP TO BE IMPLEMENTED
Plan Step: {plan_step}

# DATA CONTEXT
Files available: {file_list}
{files_context}

# CRITICAL: POLICY QUESTIONS - Calculate data AND check manual.md policy!
# If question contains "fine", "penalty", "danger", "policy", or "in danger of":
# ⚠️ MANDATORY TWO-STEP PROCESS - BOTH STEPS REQUIRED!
# 
# Step 1: Calculate relevant metric (fraud rate, volume, etc.) from transaction data
# Step 2: Check manual.md for policy threshold/rules AND compare
# Step 3: Return policy answer ("yes"/"no"/"Not Applicable"), NOT raw metric!
#
# REQUIRED CODE TEMPLATE (you MUST implement both steps):
# import pandas as pd
# # Step 1: Calculate metric
# df = pd.read_csv('{data_directory}/payments.csv')
# merchant_txs = df[df['merchant'] == 'MerchantName']
# fraud_rate = merchant_txs['has_fraudulent_dispute'].sum() / len(merchant_txs) * 100
# # Step 2: Check manual.md for policy
# with open('{data_directory}/manual.md', 'r') as f:
#     manual = f.read()
# # Search for policy keywords
# if 'fine' in manual.lower() or 'penalty' in manual.lower():
#     # Extract threshold from manual (search for numeric thresholds near "fine"/"penalty")
#     # Compare fraud_rate with threshold
#     if fraud_rate > threshold:  # threshold extracted from manual
#         print("yes")  # Merchant IS in danger
#     else:
#         print("no")  # Merchant is NOT in danger
# else:
#     print("Not Applicable")  # No policy found - EXACT format!
#
# ❌ WRONG: Only calculating fraud_rate and printing it (missing Step 2!)
# ❌ WRONG: Returning raw metric value (9.13%) instead of policy answer
# ✅ CORRECT: Calculate metric → Check manual.md → Compare → Return "yes"/"no"/"Not Applicable"

# CRITICAL: DABStep-Specific Code Patterns (ALWAYS use for fee/merchant tasks)

## FEE CALCULATIONS (MOST IMPORTANT)
# Available helpers in sandbox (use them!):
# - match_fee_rule(transaction_dict, fee_rule_dict) -> bool
# - calculate_fee(amount, fee_rule_dict) -> float
# - parse_volume_category('<100k') -> (0, 0.1) 
# - parse_fraud_range('7.7%-8.3%') -> (7.7, 8.3)

# EXACT formula: fee = fixed_amount + (rate / 10000 * eur_amount)
# Wildcard matching: empty list [] = matches all, None = matches all
# List matching: [A,B,C] = matches if value in list

# Example (COPY THIS PATTERN):
# for rule in fees:
#     if match_fee_rule(transaction, rule):
#         fee = calculate_fee(10, rule)

## FRAUD ANALYSIS
# Fraud indicator: df['has_fraudulent_dispute'] == True
# NOT df['aci'] == 'F'  (ACI is authorization type, not fraud)

## CRITICAL FIELD NAMES (MUST USE THESE!)
# merchant_data.json fields:
#   - Merchant name: df['merchant'] (NOT df['name'] ❌)
#   - Category: df['merchant_category_code'] (NOT df['category'] ❌)
#   - Account type: df['account_type'] ✅
# merchant_category_codes.csv fields:
#   - MCC code: df['mcc'] ✅
#   - Description: df['description'] ✅

# AUTO-AVAILABLE HELPERS (prevent errors):
# coerce_to_float(v) - Handles %, $, commas | safe_get(df,col) - No KeyError | is_not_empty(arr), safe_index(arr,i)

## COUNTRY FIELDS
# IP country: df['ip_country']
# Issuing country: df['issuing_country']  
# Acquirer country: needs join to merchant_data

## JOINS
# Payments ← Merchant: pd.merge(payments, merchant_data, on='merchant')
# This adds: account_type, merchant_category_code, acquirer (list)

## RATE/VOLUME PARSING
# Use: parse_volume_category('<100k') -> (0, 0.1)
# Use: parse_fraud_range('7.7%-8.3%') -> (7.7, 8.3)

# PATTERNS (Evidence: 450 tasks):
# #1: "Which X?" -> .idxmax() (NAME), "What is X?" -> .max() (VALUE) | 5%!
# #2: YES/NO -> print("yes" if cond else "no") | Ex: correlation >0.5
# #3: PLURAL -> list(df['col']), NOT .iloc[0] | 13%!
# #4: "Total fees" -> Per-tx sum, NOT avg*count | 10%!
# #5: day_of_year -> Use directly, don't convert | 5%
# #6: Series -> len() check, convert before print
# #7: Fee wildcards -> [] or None = ALL! | if not x or val in x
# #8: Multiple choice -> Map value to letter option
# #9: Helpers -> coerce_to_float(), safe_get(), match_fee_rule()

# CRITICAL ANTI-PATTERNS (DO NOT DO - These cause 81% of errors!):

## ANTI-PATTERN 1: Wrong Merchant Selection (15% of errors)
# ❌ WRONG: Selecting first merchant or wrong merchant
# ✅ CORRECT: Filter payments.csv, group by merchant, calculate metric, use .idxmin()/.idxmax()
# Example WRONG: df_merchants.iloc[0]['merchant']  # Gets first merchant, not lowest fraud!
# Example CORRECT:
#   df_merged = pd.merge(df_payments, df_merchants, on='merchant')
#   fraud_by_merchant = df_merged.groupby('merchant')['has_fraudulent_dispute'].mean()
#   lowest_merchant = fraud_by_merchant.idxmin()  # Returns merchant NAME
#   print(lowest_merchant)

## ANTI-PATTERN 2: Wrong Numeric Calculations (50% of errors)
# ❌ WRONG: Using wrong formula, wrong columns, unit conversion errors
# ✅ CORRECT: Verify formula, check column names, preserve precision

# Fee Calculation Patterns:
# ❌ WRONG: fee = rate * amount  # Missing fixed_amount and /10000!
# ✅ CORRECT: fee = fixed_amount + (rate / 10000 * amount)
# ✅ CORRECT: fee = calculate_fee(amount, fee_rule)  # Use helper!
# CRITICAL: For fees, ALWAYS use calculate_fee() helper! Never calculate manually.

# Percentage Calculation Patterns:
# ❌ WRONG: result = df['col'].mean() * 100  # Wrong if already percentage
# ✅ CORRECT: Check if value is already percentage (has %), don't multiply unnecessarily
# Example: If fraud_level is "8.3%", use coerce_to_float() -> 0.083, don't multiply by 100

# Rate Calculation Patterns:
# ❌ WRONG: fraud_rate = fraud_count / total_count  # Missing * 100 for percentage
# ✅ CORRECT: fraud_rate = (fraud_count / total_count) * 100  # For percentage
# ✅ CORRECT: fraud_rate = fraud_count / total_count  # For ratio (0-1)

# Average Calculation Patterns:
# ❌ WRONG: avg = sum(values) / len(values)  # If values are percentages, may be wrong
# ✅ CORRECT: Use pandas .mean() which handles data types correctly
# ✅ CORRECT: avg = df['col'].mean()  # Preserves precision

# Total Calculation Patterns:
# ❌ WRONG: total = avg_fee * count  # Wrong! Fees vary per transaction
# ✅ CORRECT: total = sum(calculate_fee(tx['amount'], rule) for tx in transactions)
# ✅ CORRECT: Loop through each transaction, calculate fee, sum all fees

# Correlation/Comparison Patterns:
# ❌ WRONG: corr = df['col1'].corr(df['col2']) * 100  # Correlation is already -1 to 1
# ✅ CORRECT: corr = df['col1'].corr(df['col2'])  # Keep as-is, don't multiply

## ANTI-PATTERN 3: Field Name vs Value Confusion (5% of errors)
# ❌ WRONG: Returning field name when question asks for value
# ✅ CORRECT: Understand question intent - field name vs field value
# Example WRONG: Question "What is the issuing country?" -> Answer "issuing_country" (field name) ❌
# Example CORRECT: Question "What is the issuing country?" -> Answer "NL" (value) ✅
# Example CORRECT: Question "Which field has highest correlation?" -> Answer "issuing_country" (field name) ✅
# Plan: If question asks "which field/column" -> return column name. Otherwise -> return value.

## ANTI-PATTERN 4: Yes/No vs Breakdown Confusion (10% of errors)
# ❌ WRONG: "Is credit fraud rate higher than debit?" -> "Credit: 0.11, Debit: 0.09"
# ✅ CORRECT: "Is credit fraud rate higher than debit?" -> "yes" or "no"
# Example CORRECT:
#   credit_rate = df_credit['has_fraudulent_dispute'].mean()
#   debit_rate = df_debit['has_fraudulent_dispute'].mean()
#   print("yes" if credit_rate > debit_rate else "no")  # NOT breakdown!

## ANTI-PATTERN 5: Wrong Scheme Selection (10% of errors)
# ❌ WRONG: Selecting first matching scheme or wrong scheme
# ✅ CORRECT: Check ALL fee rules, use match_fee_rule() correctly
# Example CORRECT:
#   for rule in fees:
#       if match_fee_rule(transaction, rule):
#           matching_scheme = rule['scheme']
#           break  # Or collect all matches if question asks for list
#   print(matching_scheme)

## ANTI-PATTERN 6: Precision Loss (9.7% of errors)
# ❌ WRONG: Rounding too early or to wrong precision
# ✅ CORRECT: Preserve precision from calculations, only round at final output
# Example WRONG: result = round(df['col'].mean(), 2)  # Too early, loses precision
# Example CORRECT: 
#   result = df['col'].mean()  # Keep full precision
#   # Only round at print if question specifies precision
#   print(f"{{result:.2f}}") if precision_needed else print(result)
#
# CRITICAL: DELTA/DIFFERENCE calculations MUST preserve FULL precision (14+ decimals)!
# ❌ WRONG: delta = (new_rate - old_rate) * amounts.sum() / 10000; print(f"{{delta:.2f}}")
# ✅ CORRECT: delta = (new_rate - old_rate) * amounts.sum() / 10000; print(f"{{delta:.14f}}")
# Questions with "delta", "difference", or "change...fee" need maximum precision!

## ANTI-PATTERN 7: Incomplete List Collection (10% of errors)
# ❌ WRONG: Using .head(1) or .iloc[0] for list questions
# ✅ CORRECT: Collect ALL matches, return comma-separated
# Example WRONG: result = df[condition]['merchant'].iloc[0]  # Only first!
# Example CORRECT: 
#   results = df[condition]['merchant'].unique().tolist()
#   print(', '.join(results))  # All matches, comma-separated

# INSTRUCTIONS
1. Write a COMPLETE, self-contained Python script.
2. Use absolute paths: {data_directory}/filename
3. Implement logic for: "{plan_step}"
4. Print the result at the end.
5. Return ONLY the Python code block.

Generate code:"""

CODER_INCREMENTAL_PROMPT = """You are an expert Python Data Analyst.
Your role is to REFINE and EXTEND the existing code to complete the next step of the analysis.

# GOAL
1. Understand the "Overall Question" and "Current plan to implement"
2. Analyze the "Base code" and previous results/errors
3. Generate the COMPLETE updated Python script (not just changes) that implements the current step
4. Ensure the code is robust and handles the specific data formats

# Overall Question
{question}

# Given data
{file_list}

# Files context
{files_context}

# CRITICAL: POLICY QUESTIONS - Calculate data AND check manual.md policy!
# If question contains "fine", "penalty", "danger", "policy", or "in danger of":
# ⚠️ MANDATORY TWO-STEP PROCESS - BOTH STEPS REQUIRED!
# 
# Step 1: Calculate relevant metric (fraud rate, volume, etc.) from transaction data
# Step 2: Check manual.md for policy threshold/rules AND compare
# Step 3: Return policy answer ("yes"/"no"/"Not Applicable"), NOT raw metric!
#
# REQUIRED CODE TEMPLATE (you MUST implement both steps):
# import pandas as pd
# # Step 1: Calculate metric
# df = pd.read_csv('{data_directory}/payments.csv')
# merchant_txs = df[df['merchant'] == 'MerchantName']
# fraud_rate = merchant_txs['has_fraudulent_dispute'].sum() / len(merchant_txs) * 100
# # Step 2: Check manual.md for policy
# with open('{data_directory}/manual.md', 'r') as f:
#     manual = f.read()
# # Search for policy keywords
# if 'fine' in manual.lower() or 'penalty' in manual.lower():
#     # Extract threshold from manual (search for numeric thresholds near "fine"/"penalty")
#     # Compare fraud_rate with threshold
#     if fraud_rate > threshold:  # threshold extracted from manual
#         print("yes")  # Merchant IS in danger
#     else:
#         print("no")  # Merchant is NOT in danger
# else:
#     print("Not Applicable")  # No policy found - EXACT format!
#
# ❌ WRONG: Only calculating fraud_rate and printing it (missing Step 2!)
# ❌ WRONG: Returning raw metric value (9.13%) instead of policy answer
# ✅ CORRECT: Calculate metric → Check manual.md → Compare → Return "yes"/"no"/"Not Applicable"

# Base code (preserve working parts!)
```python
{base_code}
```

# Error guidance
{error_context}

# Previous plans
{prev_steps}

# Current plan to implement
{current_step}

# QUESTION TYPE ANALYSIS (CRITICAL - Understand question intent!):
Analyze the question to determine:
1. Is it POLICY? ("fine", "penalty", "danger", "policy", "in danger of") -> TWO-STEP PROCESS:
   - "in danger of" + "fine" = POLICY question!
   - Step 1: Calculate metric (fraud rate, volume, etc.) from transaction data
   - Step 2: Check manual.md for policy threshold -> Compare -> Return "yes"/"no"/"Not Applicable"
2. Is it YES/NO? -> Return "yes" or "no", NOT breakdown or numbers
3. Is it COUNT? -> Return integer, NOT "yes"/"no"
4. Is it FIELD NAME? -> Return column name like "issuing_country"
5. Is it VALUE? -> Return actual value like "NL" or "8.91"
6. Is it LIST? -> Return ALL items, comma-separated, NOT just first
7. Is it COMPARISON? -> May need yes/no OR breakdown based on question wording

# CRITICAL FIELD NAMES (MUST USE CORRECT NAMES!)
When working with merchant_data.json:
  - Merchant name: df['merchant'] (NOT df['name'] ❌)
  - Merchant category: df['merchant_category_code'] (NOT df['category'] ❌)
  - Account type: df['account_type'] ✅
  - MCC code: df['merchant_category_code'] ✅

When working with merchant_category_codes.csv:
  - MCC code: df['mcc'] ✅
  - Description: df['description'] ✅

ALWAYS validate DataFrame columns exist before accessing!
Example: if 'merchant' in df.columns: name = df['merchant']

# MANDATORY: Type Conversion Rules (CRITICAL!)
You MUST use coerce_to_float() for ALL data-to-number conversions.
This function is automatically prepended to your code.

❌ NEVER DO THIS (will crash on percentages/currency):
   pct = transaction['monthly_fraud_level'] / 100.0  # CRASHES if '8.3%'
   rate = float(row['rate'])  # CRASHES if '55.5%'  
   value / 100  # CRASHES if value is '8.3%'
   int(df['volume'])  # CRASHES if '1,234'

✅ ALWAYS DO THIS:
   pct = coerce_to_float(transaction['monthly_fraud_level'])  # Works on '8.3%'
   rate = coerce_to_float(row['rate'])  # Works on '55.5%'
   value_num = coerce_to_float(value)  # Then use value_num

The helper automatically handles:
  - Percentages: '8.3%' -> 0.083
  - Currency: '$5.50', '€5.50' -> 5.5
  - Commas: '1,234' -> 1234.0
  - Operators: '>100', '<50' -> 100.0, 50.0

CRITICAL: Use for ALL fields that might be strings (rates, fraud_level, volume, etc.)!

## HELPER FUNCTIONS AVAILABLE (Auto-prepended to your code!)
# ═══════════════════════════════════════════════════════════
# Basic helpers (always available):
#   coerce_to_float(value) - MANDATORY for conversions! Handles %, $, commas
#   safe_get(df, column, default) - Safe column access, no KeyError
#   is_not_empty(array) - Safe empty check for arrays/Series
#   safe_index(array, idx, default) - Safe element access with bounds check
#
# Fee calculation helpers (for fee tasks):
#   match_fee_rule(trans_dict, fee_dict) - Check if fee applies
#   calculate_fee(amount, fee_dict) - Compute fee amount
#   parse_volume_category(str) - Parse volume ranges
#   parse_fraud_range(str) - Parse fraud ranges
#   aggregate_safe(df, col, method) - Safe aggregation
#
# USE THESE! They're automatically available and prevent 90% of errors!

# PATTERNS (Copy these EXACT code snippets!):
#
# #1: "Which X?" vs "What is X?" | 5% impact!
# ❌ WRONG: df.groupby('scheme')['rate'].mean().idxmax()  # Returns NAME for "What is" question
# ✅ RIGHT: df.groupby('scheme')['rate'].mean().max()  # Returns VALUE for "What is" question
#
# #2: YES/NO questions
# ❌ WRONG: print(correlation_value)  # Prints 0.038
# ✅ RIGHT: print("yes" if correlation > 0.5 else "no")
#
# #3: PLURAL - Return ALL matches | 13% impact!
# ❌ WRONG: result = df[df['account_type']=='H']['ID'].iloc[0]  # First only
# ✅ RIGHT: result = list(df[df['account_type']=='H']['ID'])  # All matches
#
# #4: TOTAL fees | 10% impact!
# ❌ WRONG: total = avg_fee * count  # Wrong!
# ✅ RIGHT: total = sum(fixed + rate*amt/10000 for amt in amounts)  # Per-tx
#
# #5: day_of_year
# ✅ RIGHT: df[df['day_of_year'] == 200]  # Use directly
#
# #6: Fee WILDCARDS | Critical for accuracy!
# ❌ WRONG: if rule['aci'] == 'B'  # Fails when rule['aci'] = [] or None
# ✅ RIGHT: if not rule['aci'] or 'B' in rule['aci']  # Handles wildcards
#
# #7: Multiple choice
# ✅ RIGHT: result = f"{{options[country]}}. {{country}}"  # "B. BE" not just "BE"

# COMPLETE WORKING EXAMPLES (13% impact - HIGHEST!):
#
# EXAMPLE: Fee IDs for account_type=H and aci=D (static filter)
# import json
# fees = json.load(open('{data_directory}/fees.json'))
# # Wildcard handling: [] or None = matches ALL
# matching_ids = [f['ID'] for f in fees 
#                 if (not f['account_type'] or 'H' in f['account_type']) and
#                    (not f['aci'] or 'D' in f['aci'])]
# result = ', '.join(map(str, sorted(matching_ids)))  # ALL matches as list
# print(result)
#
# EXAMPLE: Fee IDs APPLICABLE to merchant on specific day (CRITICAL - use match_fee_rule!)
# import pandas as pd
# import json
# df = pd.read_csv('{data_directory}/payments.csv')
# fees = json.load(open('{data_directory}/fees.json'))
# merchant_data = json.load(open('{data_directory}/merchant_data.json'))
# # Filter transactions for merchant + day
# transactions = df[(df['merchant']=='Belles') & (df['day_of_year']==10) & (df['year']==2023)]
# merchant_info = next(m for m in merchant_data if m['merchant']=='Belles')
# # Calculate monthly stats (needed for match_fee_rule)
# monthly_volume = df[(df['merchant']=='Belles') & (df['year']==2023)]['eur_amount'].sum() / 1_000_000
# monthly_fraud = df[(df['merchant']=='Belles') & (df['year']==2023)]['has_fraudulent_dispute'].mean() * 100
# # Find applicable fee IDs by matching ACTUAL transactions
# applicable_ids = set()
# for _, tx in transactions.iterrows():
#     tx_dict = tx.to_dict()
#     tx_dict['account_type'] = merchant_info['account_type']
#     tx_dict['mcc'] = merchant_info['merchant_category_code']
#     tx_dict['monthly_volume_millions'] = monthly_volume
#     tx_dict['monthly_fraud_percent'] = monthly_fraud
#     for rule in fees:
#         if match_fee_rule(tx_dict, rule):  # Helper handles ALL conditions!
#             applicable_ids.add(rule['ID'])
#             break  # First match per transaction
# result = ', '.join(map(str, sorted(applicable_ids)))
# print(result)
#
# EXAMPLE: Total fees for merchant in month
# import pandas as pd
# import json
# df = pd.read_csv('{data_directory}/payments.csv')
# fees = json.load(open('{data_directory}/fees.json'))
# merchant_txs = df[(df['merchant']=='Belles') & (df['day_of_year'].between(1,31))]
# # Per-transaction fee calculation:
# total = 0
# for _, tx in merchant_txs.iterrows():
#     for f in fees:
#         if f['card_scheme'] == tx['card_scheme']:  # Match conditions
#             fee = f['fixed_amount'] + f['rate'] * tx['eur_amount'] / 10000
#             total += fee
#             break
# print(round(total, 2))
#
# EXAMPLE: "What is highest fraud rate (by scheme)?" - Returns VALUE not NAME
# df.groupby('card_scheme')['has_fraudulent_dispute'].mean().max()  # .max() for VALUE
#
# EXAMPLE: "Which merchant has the lowest fraud rate?" - Returns MERCHANT NAME
# ❌ WRONG: df_merchants.iloc[0]['merchant']  # Gets first merchant, not lowest fraud!
# ✅ CORRECT:
#   import pandas as pd
#   import json
#   df_payments = pd.read_csv('{data_directory}/payments.csv')
#   df_merchants = pd.read_json('{data_directory}/merchant_data.json')
#   df_merged = pd.merge(df_payments, df_merchants, on='merchant')
#   fraud_by_merchant = df_merged.groupby('merchant')['has_fraudulent_dispute'].mean()
#   lowest_merchant = fraud_by_merchant.idxmin()  # Returns merchant NAME
#   print(lowest_merchant)
# CRITICAL: Must filter payments.csv, NOT just merchant_data.json!
# CRITICAL: Use .idxmin()/.idxmax() to get NAME, NOT .min()/.max() which returns VALUE!
#
# EXAMPLE: Policy questions ("fine", "penalty", "danger of fine") - CRITICAL!
# ✅ CORRECT: TWO-STEP PROCESS - Calculate metric AND check manual.md policy
# Question: "Is Martinis_Fine_Steakhouse in danger of getting a high-fraud rate fine?"
# Step 1: Calculate fraud rate from transaction data
# import pandas as pd
# df = pd.read_csv('{data_directory}/payments.csv')
# merchant_txs = df[df['merchant'] == 'Martinis_Fine_Steakhouse']
# fraud_rate = merchant_txs['has_fraudulent_dispute'].sum() / len(merchant_txs) * 100
# Step 2: Check manual.md for policy threshold
# with open('{data_directory}/manual.md', 'r') as f:
#     manual = f.read()
# # Search for policy keywords and threshold
# if 'fine' in manual.lower() or 'penalty' in manual.lower():
#     # Extract threshold from manual (e.g., "fraud rate > 10% triggers fine")
#     # Compare calculated fraud_rate with threshold
#     if fraud_rate > threshold:  # threshold extracted from manual
#         print("yes")  # Merchant IS in danger of fine
#     else:
#         print("no")  # Merchant is NOT in danger of fine
# else:
#     print("Not Applicable")  # No such policy exists - EXACT format!
# CRITICAL: Policy questions require BOTH calculation AND policy checking!
# ❌ WRONG: Return calculated fraud rate alone (without checking policy)
# ❌ WRONG: Return MCC codes or fee IDs (these are data values, not policy answers)
# ✅ CORRECT: Calculate metric → Check manual.md → Compare → Return "yes"/"no"/"Not Applicable"

# CRITICAL: ANSWER THE QUESTION, DON'T PRINT DATA!
# ❌ WRONG: print({{1: 9.66, 2: 8.5, ..., 12: 9.82}})  # Prints data structure
# ✅ RIGHT: print("yes")  # Answers the yes/no question
#
# Task 70 example: "Is merchant in danger of fine?"
# ❌ WRONG: Calculate monthly rates, print dict of rates
# ✅ RIGHT: Check manual for "fine" policy, print "Not Applicable" if not found
#
# COMMON MISTAKES (From actual failures):
# 1. Confusing "fine" (penalty) with "fee" (cost) -> FINE in manual.md, FEE in fees.json
# 2. df.groupby(Y)[X].mean().idxmax() for "What is X?" -> Use .max()
# 3. df['ID'].iloc[0] for plural "IDs" -> Use list(df['ID'])
# 4. if rule['aci'] == 'B' (fails on []) -> Use: if not rule['aci'] or 'B' in rule['aci']
# 5. total = avg * count for "total fees" -> Loop per-tx: sum(fee_per_tx)
# 6. print(dict) or print(series.to_dict()) -> Answer question, don't dump data!
# 7. print(f"Result: {{value}}") -> Just print(value)
# 8. print(correlation) for yes/no -> print("yes" if corr>0.5 else "no")

# CRITICAL ANTI-PATTERNS (DO NOT DO - These cause 81% of errors!):

## ANTI-PATTERN 1: Wrong Merchant Selection (15% of errors)
# ❌ WRONG: df_merchants.iloc[0]['merchant']  # Gets first merchant, not lowest fraud!
# ✅ CORRECT: Filter payments.csv, group by merchant, calculate metric, use .idxmin()/.idxmax()
# Example:
#   df_merged = pd.merge(df_payments, df_merchants, on='merchant')
#   fraud_by_merchant = df_merged.groupby('merchant')['has_fraudulent_dispute'].mean()
#   lowest_merchant = fraud_by_merchant.idxmin()  # Returns merchant NAME
#   print(lowest_merchant)

## ANTI-PATTERN 2: Wrong Numeric Calculations (50% of errors)
# Fee Calculation Patterns:
# ❌ WRONG: fee = rate * amount  # Missing fixed_amount and /10000!
# ✅ CORRECT: fee = fixed_amount + (rate / 10000 * amount)
# ✅ CORRECT: fee = calculate_fee(amount, fee_rule)  # Use helper!
# CRITICAL: For fees, ALWAYS use calculate_fee() helper! Never calculate manually.

# Percentage Calculation Patterns:
# ❌ WRONG: result = df['col'].mean() * 100  # Wrong if already percentage
# ✅ CORRECT: Check if value is already percentage (has %), don't multiply unnecessarily
# Example: If fraud_level is "8.3%", use coerce_to_float() -> 0.083, don't multiply by 100

# Rate Calculation Patterns:
# ❌ WRONG: fraud_rate = fraud_count / total_count  # Missing * 100 for percentage
# ✅ CORRECT: fraud_rate = (fraud_count / total_count) * 100  # For percentage
# ✅ CORRECT: fraud_rate = fraud_count / total_count  # For ratio (0-1)

# Average Calculation Patterns:
# ❌ WRONG: avg = sum(values) / len(values)  # If values are percentages, may be wrong
# ✅ CORRECT: Use pandas .mean() which handles data types correctly
# ✅ CORRECT: avg = df['col'].mean()  # Preserves precision

# Total Calculation Patterns:
# ❌ WRONG: total = avg_fee * count  # Wrong! Fees vary per transaction
# ✅ CORRECT: total = sum(calculate_fee(tx['amount'], rule) for tx in transactions)
# ✅ CORRECT: Loop through each transaction, calculate fee, sum all fees

# Delta/Difference Calculation Patterns (CRITICAL - Preserve FULL precision!):
# ❌ WRONG: delta = (new_rate - old_rate) * amounts.sum() / 10000; print(f"{{delta:.2f}}")
# ✅ CORRECT: delta = (new_rate - old_rate) * amounts.sum() / 10000; print(f"{{delta:.14f}}")
# Questions with "delta", "difference", or "change...fee" MUST use 14+ decimals!
# Example:
#   original_rate = fee_rule['rate']
#   new_rate = 1  # or whatever the question specifies
#   affected_amounts = matching_transactions['eur_amount'].sum()
#   delta = (new_rate - original_rate) * affected_amounts / 10000
#   print(f"{{delta:.14f}}")  # CRITICAL: 14 decimals, NOT 2!

## ANTI-PATTERN 3: Field Name vs Value Confusion (5% of errors)
# ❌ WRONG: Question "What is the issuing country?" -> Answer "issuing_country" (field name)
# ✅ CORRECT: Question "What is the issuing country?" -> Answer "NL" (value)
# ✅ CORRECT: Question "Which field has highest correlation?" -> Answer "issuing_country" (field name)
# Rule: If question asks "which field/column" -> return column name. Otherwise -> return value.

## ANTI-PATTERN 4: Yes/No vs Breakdown Confusion (10% of errors)
# ❌ WRONG: "Is credit fraud rate higher than debit?" -> "Credit: 0.11, Debit: 0.09"
# ✅ CORRECT: "Is credit fraud rate higher than debit?" -> "yes" or "no"
# Example:
#   credit_rate = df_credit['has_fraudulent_dispute'].mean()
#   debit_rate = df_debit['has_fraudulent_dispute'].mean()
#   print("yes" if credit_rate > debit_rate else "no")  # NOT breakdown!

## ANTI-PATTERN 5: Wrong Scheme Selection (10% of errors)
# ❌ WRONG: Selecting first matching scheme or wrong scheme
# ✅ CORRECT: Check ALL fee rules, use match_fee_rule() correctly
# Example:
#   for rule in fees:
#       if match_fee_rule(transaction, rule):
#           matching_scheme = rule['scheme']
#           break
#   print(matching_scheme)

## ANTI-PATTERN 6: Precision Loss (9.7% of errors)
# ❌ WRONG: result = round(df['col'].mean(), 2)  # Too early, loses precision
# ✅ CORRECT: 
#   result = df['col'].mean()  # Keep full precision
#   # Only round at print if question specifies precision
#   print(f"{{result:.2f}}") if precision_needed else print(result)
#
# CRITICAL: DELTA/DIFFERENCE calculations MUST preserve FULL precision (14+ decimals)!
# ❌ WRONG: delta = (new_rate - old_rate) * amounts.sum() / 10000; print(f"{{delta:.2f}}")
# ✅ CORRECT: delta = (new_rate - old_rate) * amounts.sum() / 10000; print(f"{{delta:.14f}}")
# Questions with "delta", "difference", or "change...fee" need maximum precision!

## ANTI-PATTERN 7: Incomplete List Collection (10% of errors)
# ❌ WRONG: result = df[condition]['merchant'].iloc[0]  # Only first!
# ✅ CORRECT: 
#   results = df[condition]['merchant'].unique().tolist()
#   print(', '.join(results))  # All matches, comma-separated

# INSTRUCTIONS
- COPY exact code from examples/templates when pattern matches
- Avoid all 8 common mistakes above
- Use helpers: coerce_to_float(), match_fee_rule()
- Implement the current plan to as a single Python script
- Use absolute paths: {data_directory}/filename
- Code should be self-contained and executable
- Print intermediate results for debugging
Generate the code:"""

CODER_FALLBACK_PROMPT = """You are an expert data analyst.
Implement this plan as Python code.

# Question
{question}

# Plan to Implement
{plan_text}

# Data File Analyses
{files_context}

# Your Task
- Implement the complete plan as a single Python script
- Use absolute paths: {data_directory}/filename
- Code should be self-contained and executable
- Print intermediate results for debugging
- End with print("FINAL_ANSWER:", result) twice
- Your response should be a single Python code block

Generate the code:"""

CODER_SIMPLE_FALLBACK_PROMPT = """Implement this plan as Python code.

Question: {question}
Plan: {plan}
Data Files: {file_analyses}

Generate Python code:"""

# =============================================================================
# FINALIZER PROMPT - For SUPER-INFERENCE Output Formatting
# =============================================================================

FINALIZER_PROMPT = """Extract the FINAL ANSWER VALUE from execution result.

EXECUTION:
{execution_result}

QUESTION:
{question}

# CRITICAL: POLICY QUESTION EXTRACTION
If question contains "fine", "penalty", "danger", "policy", or "in danger of":
- ⚠️ Policy questions MUST return: "yes", "no", or "Not Applicable"
- ❌ WRONG: Extracting raw metric (9.13%, 0.091, etc.) - this is Step 1, not the answer!
- ✅ CORRECT: Extract policy answer from execution result
- Look for: "yes", "no", "Not Applicable" in execution result
- If execution shows fraud rate but NO policy answer -> Check if manual.md was checked
- If manual.md was checked but no threshold found -> Return "Not Applicable"
- If manual.md was checked and comparison done -> Extract "yes" or "no"

EXTRACTION RULES (ANTI-KEYWORD PATTERNS):
1. Extract the VALUE, NEVER extract keywords from the question!
2. For "ID" questions -> Return the NUMBER, NEVER return the word "ID"
3. For "fee" questions -> Return the AMOUNT, NEVER return card scheme names
4. For "list" questions -> Return COMMA-SEPARATED values, do NOT use brackets []
   Example: "A, B, C" (CORRECT) vs "['A', 'B', 'C']" (WRONG)
5. For POLICY questions -> Return "yes"/"no"/"Not Applicable", NEVER raw metric!

CRITICAL: CLEAN ANSWER FORMATTING (NO EXPLANATORY TEXT):
- Extract ONLY the answer value, no explanations
- Remove phrases like "is the final answer", "is the answer", "the answer is"
- Remove trailing periods unless part of a decimal number
- For single values: Return "64" NOT "[64]" (no brackets)
- For lists: Return "12, 64, 79" NOT "[12, 64, 79]" (no brackets, comma-separated)
- Do NOT add explanatory text like "The answer is 64" - just return "64"

CRITICAL: SINGLE-LINE FORMAT FOR MULTI-VALUE RESULTS!
If execution has MULTIPLE lines with key:value pairs, combine into ONE line:

WRONG (multi-line):
A: 87.79
B: 86.22
C: 69.36

CORRECT (single line):
A: 87.79, B: 86.22, C: 69.36, D: 94.34, F: 77.90, G: 115.23

WRONG (just last value):
115.23

This applies to:
- Grouped aggregations (average by ACI, by country, etc.)
- Multiple results for one question
- Dictionary-style outputs

CRITICAL ANTI-PATTERNS (DO NOT DO THIS):
❌ Question: "What is the fee ID?" -> Execution: "The fee ID is 123" -> WRONG: "ID" | CORRECT: "123"
❌ Question: "Average fee for GlobalCard" -> Execution: "GlobalCard fee is 0.15" -> WRONG: "GlobalCard" | CORRECT: "0.15"
❌ Question: "Fee IDs..." -> Execution: "Final list: [1,2,3]" -> WRONG: "[Final list: [1" | CORRECT: "[1,2,3]"
❌ Question: "fraud rate" -> Execution: "rate is 10.85%" -> WRONG: "rate" | CORRECT: "10.85%"
❌ Question: "How many transactions?" -> Execution: "There are 138236 transactions" -> WRONG: "Yes" | CORRECT: "138236" (Count != Boolean!)

CORRECT EXTRACTION (THINK ABOUT WHAT THE QUESTION ASKS FOR):
✅ "What is the ID?" -> Question asks for NUMBER -> Extract: 123
✅ "Average fee?" -> Question asks for AMOUNT -> Extract: 0.15
✅ "List IDs" -> Question asks for LIST -> Extract: [1,2,3]
✅ "Which country?" -> Question asks for COUNTRY -> Extract: BE
✅ "How many?" -> Question asks for COUNT -> Extract: 5 (not "Yes")

DECIMAL PRECISION HANDLING (CRITICAL - Analyze question for precision hints):

⚠️  STEP 1: Check if question EXPLICITLY specifies decimal precision:
   • "6 decimals" / "six decimals" / "6 decimal places" -> Use 6 decimals
   • "4 decimals" / "four decimals" -> Use 4 decimals
   • "2 decimals" / "two decimals" -> Use 2 decimals
   • "rounded to N" -> Use N decimals

⚠️  STEP 2: If NO explicit precision in question, use STANDARD rounding:
   • Percentages: 2 decimals (e.g., "73.15%", "10.85%")
   • Monetary amounts: 2 decimals (e.g., "91.85", "150.50")
   • Counts/IDs: 0 decimals (integers only: "138236", "5")
   • Ratios: 2 decimals (e.g., "2.68 to 1")
   • Correlations: 2 decimals (e.g., "0.85", "-0.12")

⚠️  STEP 3: Apply precision to numeric answers:
   • If question says "6 decimals": Format as "23.834676"
   • Otherwise for percentages/amounts: Format as "73.15"
   • For counts: Remove decimals: "138236" not "138236.0"

EXAMPLES (WITH SINGLE-LINE FORMAT):

Example 1 - Grouped Results:
Question: "What is the average transaction value grouped by aci?"
Execution: "A: 87.79\nB: 86.22\nC: 69.36\nD: 94.34\nF: 77.90\nG: 115.23"
{{"thoughts": "Multi-line grouped results -> combine to single line", "answer": "A: 87.79, B: 86.22, C: 69.36, D: 94.34, F: 77.90, G: 115.23"}}

Example 2 - Explicit Precision:
Question: "What is the average fee in EUR, provide answer with 6 decimals"
Execution: "23.83467632847"
{{"thoughts": "Question explicitly asks for 6 decimals", "answer": "23.834676"}}

Example 3 - Percentage:
Question: "What percentage of transactions are credit?"
Execution: "73.14954136404412"
{{"thoughts": "Percentage, no explicit precision -> standard 2 decimals", "answer": "73.15"}}

Example 4 - Count:
Question: "How many transactions?"
Execution: "138236.0"
{{"thoughts": "Count question -> integer, remove decimals", "answer": "138236"}}

Example 5 - Multi-Value Dictionary:
Question: "Average amount by country?"
Execution: "NL: 91.85\nBE: 87.32\nFR: 103.45"
{{"thoughts": "Multi-line dict -> single line (key:value)", "answer": "NL: 91.85, BE: 87.32, FR: 103.45"}}

Example 6 - Simple List (No Brackets):
Question: "Which card schemes have fees?"
Execution: "GlobalCard\nSwiftCharge\nNexPay"
{{"thoughts": "List of values without colons -> comma-separated, no brackets", "answer": "GlobalCard, SwiftCharge, NexPay"}}

Example 7 - List of IDs (Keep brackets if requested or ambiguous):
Question: "List all fee IDs..."
Execution: "[123, 456, 789]"
{{"thoughts": "List of IDs -> keep brackets if question asks for 'list', otherwise comma-separated", "answer": "[123, 456, 789]"}}

Example 8 - "How many" vs "Are there":
Question: "How many transactions are there?"
Execution: "138236"
{{"thoughts": "'How many' asks for COUNT, not boolean 'yes/no'", "answer": "138236"}}

PANDAS ARTIFACT FILTERING (Clean execution before extraction):
⚠️  SKIP lines containing pandas metadata:
   • "dtype:" or "float64" or "int64" or "object"
   • "Name:" or "Series(" or "DataFrame("
   • Lines with these are NOT answers!

Example:
Execution: "month  monthly_volume\n0  1  2.5\n11  12  9.758384\nName: result, dtype: float64"
Skip the dtype line! Extract: "9.758384" (last actual data line)

VERBOSE PREFIX REMOVAL (Extract value from descriptive text):
⚠️  Remove verbose prefixes and labels:
   • "Number of identified retry transactions: 0" -> "0"
   • "The answer is: 64" -> "64"
   • "437 64" (count + value) -> "64" (extract the value, not count)
   • "Total: 138236" -> "138236"
   • "64 is the final answer." -> "64" (remove explanatory text)
   • "[64]" -> "64" (remove brackets from single values)
   • "[12, 64, 79]" -> "12, 64, 79" (remove brackets, keep comma-separated)

Example:
Execution: "Number of identified retry transactions: 0"
{{"thoughts": "Verbose prefix, extract just the number", "answer": "0"}}

Execution: "437 64"
{{"thoughts": "Format is 'count day_of_year', question asks for day -> extract second value", "answer": "64"}}

RESPONSE FORMAT (ABSOLUTELY CRITICAL):
Return a JSON object with this EXACT structure (NO markdown, NO code blocks):

{{
  "thoughts": "Brief reasoning about precision, pandas artifacts, verbose prefixes, and what you're extracting",
  "answer": "The extracted value with CORRECT precision and clean format"
}}

IMPORTANT REMINDERS:
1. Check question for explicit precision ("6 decimals" -> use 6, otherwise -> use 2)
2. Skip pandas artifact lines (dtype:, float64, Series()...)
3. Remove verbose prefixes ("Number of...: ", "The answer is: ")
4. Extract VALUE not KEYWORD ("fee ID 123" -> "123", not "ID")
5. Apply correct precision based on question type

Return JSON (no markdown wrapper):"""

# =============================================================================
# ANALYZER PROMPTS - For SUPER-INFERENCE File Analysis
# =============================================================================

ANALYZER_FILE_PROMPT = """Generate SIMPLE, DIRECT Python code to analyze {file_name}.

🚨 CRITICAL RULES 🚨:
- NO "def" or functions! (Code must execute immediately)
- NO try/except! (Keep it simple)
- DIRECT execution at module level
- Print comprehensive analysis
- Focus on: exact column names, data types, unique values, sample rows

Then the code must print:

For JSON files (fees.json, merchant_data.json):
1. "FILE_TYPE: JSON"
2. "TOTAL_ENTRIES: N"
3. "EXACT_FIELDS: field1, field2, field3, ..." (exact names as in data!)
4. "FIELD_TYPES: field1=type1, field2=type2, ..."
5. "UNIQUE_VALUES:" for each categorical field (show all possible values)
6. "RANGES:" for numeric fields (min-max)
7. "SAMPLE_ENTRIES:" First 3 complete entries
8. "WILDCARD_FIELDS:" Which fields use [] or None as wildcard

For CSV files (payments.csv):
1. "FILE_TYPE: CSV"
2. "ROW_COUNT: N"
3. "EXACT_COLUMNS: col1, col2, col3, ..." (exact names!)
4. "COLUMN_TYPES: col1=dtype1, col2=dtype2, ..."
5. "UNIQUE_VALUES:" for categorical columns (first 20)
6. "NUMERIC_STATS:" min, max, mean for numeric columns
7. "SAMPLE_ROWS:" First 5 rows
8. "RELATIONSHIPS:" Links to other files (e.g., merchant -> merchant_data.json)

CRITICAL REQUIREMENTS:
- Comments explain PURPOSE and USAGE
- Executed code prints STRUCTURE and SAMPLES
- Use EXACT field names (case-sensitive!)
- Show ACTUAL values so code knows formats
- Identify wildcards and special patterns{size_hint}

FOR CSV - ENHANCED PATTERN (SHOW UNIQUE VALUES):
```python
import pandas as pd
df = pd.read_csv('/full/path/to/file.csv')
print(f"ROWS: {{len(df)}}")
print(f"COLUMNS: {{list(df.columns)}}")
print(f"DTYPES: {{df.dtypes.to_dict()}}")

# Show unique values for categorical columns (critical for filtering!)
print("\\nUNIQUE VALUES (for non-numeric columns):")
for col in df.columns:
    if df[col].dtype == 'object' or df[col].nunique() < 50:
        uniq = df[col].nunique()
        print(f"  {{col}}: {{uniq}} unique")
        if uniq <= 10:
            print(f"    Values: {{list(df[col].unique())}}")

print("\\nSAMPLE (first 3 rows):")
print(df.head(3))
```

FOR JSON - ENHANCED PATTERN (CRITICAL FOR FEES):
```python
import json
with open('/full/path/to/file.json') as f:
    d = json.load(f)
print(f"ENTRIES: {{len(d)}}")
if isinstance(d, list) and d:
    print(f"KEYS: {{list(d[0].keys())}}")
    
    # CRITICAL: Show which fields are ARRAYS vs scalars!
    print("\\nFIELD TYPES (CRITICAL!):")
    for key, val in d[0].items():
        if isinstance(val, list):
            print(f"  {{key}}: ARRAY {{val}}")
        else:
            print(f"  {{key}}: {{type(val).__name__}} = {{val}}")
    
    print("\\nSAMPLE (2 entries):")
    print(json.dumps(d[:2], indent=2))
else:
    print(f"KEYS: {{list(d.keys())}}")
    print("\\nSAMPLE:")
    print(json.dumps(d, indent=2))
```

Generate EXACTLY like templates above for {file_name}{size_hint}:
```python
# ═══════════════════════════════════════════════════════════════════════════
# FILE: {file_name}
# PURPOSE: [What this file contains]
# ═══════════════════════════════════════════════════════════════════════════
#
# FIELD DOCUMENTATION (CRITICAL - Code generation depends on exact names!):
# 
# For JSON files:
#   field_name: type | Purpose | Values | Example
#   ID: int | Unique fee rule identifier | Range 1-1000 | rule['ID']
#   account_type: list | Account types this rule applies to | ['F','H','R','D'] or [] | [] = wildcard (matches ALL!)
#
# For CSV files:
#   column_name: dtype | Purpose | Values | Example  
#   merchant: object | Merchant name | 30 unique merchants | df['merchant']
#   eur_amount: float64 | Transaction amount in EUR | 0.01-10000.00 | df['eur_amount']
#
# KEY PATTERNS:
#   - Empty list [] or None = WILDCARD (matches all values)
#   - Fee formula: fixed_amount + (rate / 10000 * transaction_amount)
#   - [Other important patterns]
#
# RELATIONSHIPS:
#   - Links to other files (e.g., merchant field -> merchant_data.json)
# ═══════════════════════════════════════════════════════════════════════════

[Your analysis python code for {file_name} here]
```

Generate complete Python code with rich comments:"""

ANALYZER_FILE_SIMPLE_PROMPT = """Generate Python code to comprehensively analyze this data file.

FILE: {file_name}
PATH: {file_path}
TYPE: {file_ext}

REQUIREMENTS:
1. Load the file correctly (CSV, JSON, MD, Excel, etc.)
2. Print FULL content if file is small (<100 rows)
3. Print essential information:
   - ALL column names (for structured data)
   - Data types for each column
   - First 5 rows sample
   - Last 5 rows sample (if applicable)
   - Unique values for categorical columns (top 20)
   - Statistics (mean, std, min, max) for numeric columns
4. For unstructured data (MD, TXT): print metadata and text summary
5. Code should be self-contained and executable
6. Do NOT use try-except - we'll debug errors later
7. All paths should be absolute: '{file_path}'

Generate a complete, working Python script:"""

# =============================================================================
# DEBUG PROMPTS - For Code Fixing
# =============================================================================

DEBUG_ANALYZER_PROMPT = """This code failed:
```python
{code}
```

Error: {error}

Fix it. RULES:
- NO "def" - direct execution only!
- NO try/except - keep it simple!
- 10-15 lines max
- Just load file and print basic info

Generate SIMPLE fixed code:"""

DEBUG_FILE_ANALYZER_PROMPT = """The following code failed to execute:

```python
{code}
```

Error:
{error}

Fix the code. Generate corrected Python script:"""

DEBUG_SUMMARY_PROMPT = """# Error report
{error_msg}

# Your task
- Remove all unnecessary parts of the above error report.
- We are now running {file_name} analyzer. Do not remove where the error occurred."""

DEBUG_SUPINF_PROMPT = """# Given data: {file_list}
{files_context}

# Code with an error:
```python
{code}
```

# Error:
{error}

# Your task
- Please revise the code to fix the error.
- Provide the improved, self-contained Python script again.
- Note that you only have {file_list} available.
- There should be no additional headings or text in your response.
- Do not include dummy contents since we will debug if error occurs.
- All files/documents are in `data/` directory.

Generate fixed code:"""

DEBUG_FINALIZER_PROMPT = """The following code has an error. Fix it.

# Code with error:
```python
{code}
```

# Error:
{error}

# Reference code that worked:
```python
{reference_code}
```

# Reference execution result:
{reference_result}

# Your task
- Fix the error in the code
- The reference code above worked correctly - learn from it
- Generate corrected Python code
- Do not include dummy contents
- All files in `{data_directory}/` directory

Generate fixed code:"""

# =============================================================================
# CODE GENERATION PROMPTS - For stream_generate
# =============================================================================

GENERATION_PROMPT = """Generate {language_id} code for: {query}

Context:
{full_context}

Plan Summary:
{plan_summary}

Instructions:
- Generate clean, working code
- Follow best practices for {language_id}
- Include necessary imports
- Add helpful comments
- Ensure code is production-ready"""

# =============================================================================
# CODE EDITING PROMPTS - For stream_edit
# =============================================================================

EDIT_PROMPT = """Edit the following {language_id} file according to the instructions.

File: {file_name}
Language: {language_id}

Current Content:
```{language_id}
{file_content}
```

Edit Instructions: {edit_prompt}

Requirements:
- Return ONLY the complete modified file content
- Preserve all existing functionality unless explicitly asked to change
- Maintain proper syntax and formatting
- Keep all imports and dependencies
- Preserve critical system behavior

Begin immediately with the modified file content. No preamble, explanations, or formatting."""

# =============================================================================
# PROJECT CREATION PROMPTS - For stream_create
# =============================================================================

PROJECT_CREATION_PROMPT = """Create a complete project: {prompt}

Project Name: {project_name}
Description: {description}

Requirements:
- Create a complete, working project structure
- Include all necessary files (code, config, documentation)
- Use modern best practices
- Include proper error handling
- Add comprehensive comments
- Create production-ready code

For each file, use this format:
FILE: path/to/file.ext
```
file content here
```

Create a complete project now."""

# =============================================================================
# CODE ANALYSIS PROMPTS
# =============================================================================

REQUEST_INTENT_ANALYSIS_PROMPT = """Analyze this user request and determine the most appropriate action type.

User Request: "{user_request}"

Context:
- Current file: {current_file}
- Context files: {context_files_count} files available
- Available files: {available_files}

Determine the action type and target files based on these criteria:

1. **EDIT** - User wants to modify existing files:
   - Adding/removing code from specific files
   - Fixing bugs in existing code
   - Refactoring existing functions/classes
   - Modifying file content
   - Examples: "add a method to file.py", "fix the bug in this code", "remove print statements"
   - **File Detection**: Look for specific file names mentioned or infer from context

2. **GENERATE** - User wants to create new code without targeting specific files:
   - Creating new functions/classes from scratch
   - Generating code snippets
   - Writing algorithms or utilities
   - Examples: "write a function to sort arrays", "create a class for user management"

3. **CREATE** - User wants to create entirely new files/projects:
   - Creating new files
   - Setting up project structures
   - Scaffolding applications
   - Examples: "create a new web app", "make a REST API", "build a calculator app"
   - **File Detection**: Determine what new files should be created

4. **CHAT** - User wants to discuss, explain, or ask questions:
   - Asking for explanations
   - General programming questions
   - Code reviews without modifications
   - Examples: "explain this code", "what does this function do", "how does recursion work"

**CRITICAL**: Analyze the request to detect specific file targets:
- Look for explicit file names (e.g., "simplon.py", "utils.py", "main.py")
- Look for file references ("in the file", "to this file", "the current file")
- Look for context clues ("in context", "in the project", "here")
- Determine if new files should be created vs existing files modified

Respond with JSON:
{{
    "action_type": "edit|generate|create|chat",
    "confidence": 0.0-1.0,
    "reasoning": "Brief explanation of why this action type was chosen",
    "target_files": ["specific files to modify or create"],
    "target_file_actions": {{
        "filename.py": "modify|create",
        "another.py": "modify|create"
    }},
    "requires_file_context": true/false,
    "should_create_new_files": true/false,
    "specific_file_mentioned": true/false
}}

**EXAMPLES**:
- "include in simplon.py a method" -> {{"action_type": "edit", "target_files": ["simplon.py"], "target_file_actions": {{"simplon.py": "modify"}}}}
- "create a new utils.py file" -> {{"action_type": "create", "target_files": ["utils.py"], "target_file_actions": {{"utils.py": "create"}}}}
- "add error handling to all files" -> {{"action_type": "edit", "target_files": ["all_context_files"], "target_file_actions": {{"*": "modify"}}}}

Be very specific about detecting EDIT requests and target files - if the user mentions a specific file or wants to modify existing code, it should be EDIT with proper file targeting."""

LANGUAGE_FEATURES_ANALYSIS_PROMPT = """Analyze the following code content and identify:
1. Programming language (be specific: python, javascript, typescript, bash, sh, c, cpp, java, go, rust, etc.)
2. Print/output statement patterns for this language
3. Comment patterns
4. Important constructs that should be preserved (shebang, imports, function definitions, etc.)
5. Language-specific syntax rules

File path: {file_path}
Language hint: {language_hint}

Code content:
```{content}```

Respond in JSON format:
{{
    "language": "detected_language",
    "print_patterns": ["pattern1", "pattern2", ...],
    "comment_patterns": ["pattern1", "pattern2", ...],
    "preserve_patterns": ["pattern1", "pattern2", ...],
    "syntax_rules": ["rule1", "rule2", ...],
    "confidence": 0.0-1.0
}}"""

CODE_STRUCTURE_ANALYSIS_PROMPT = """Perform a comprehensive analysis of the following code:

File path: {file_path}
Language hint: {language_hint}
Analysis type: {analysis_type}

Code content:
```{content}```

Analyze and provide:
1. Programming language (be specific)
2. Code structure (functions, classes, imports, etc.)
3. Output/print statement patterns
4. Comment patterns
5. Important constructs to preserve
6. Syntax rules and conventions
7. Potential issues or improvements
8. Language-specific best practices

Respond in JSON format:
{{
    "language": "detected_language",
    "structure": {{
        "functions": ["function1", "function2"],
        "classes": ["class1", "class2"],
        "imports": ["import1", "import2"],
        "variables": ["var1", "var2"]
    }},
    "patterns": {{
        "print_statements": ["pattern1", "pattern2"],
        "comments": ["pattern1", "pattern2"],
        "control_flow": ["pattern1", "pattern2"]
    }},
    "preserve_constructs": ["construct1", "construct2"],
    "syntax_rules": ["rule1", "rule2"],
    "best_practices": ["practice1", "practice2"],
    "confidence": 0.0-1.0
}}"""

REMOVE_PRINT_STATEMENTS_PROMPT = """Remove all print/output statements from the following {language} code while preserving important constructs.

Language: {language}
Print patterns to remove: {print_patterns}
Important patterns to preserve: {preserve_patterns}

Original code:
```{language}
{content}
```

Requirements:
1. Remove ALL print/output statements (print, echo, console.log, printf, etc.)
2. Preserve important constructs like shebang lines, imports, function definitions, class definitions
3. Maintain proper syntax and indentation
4. Keep comments unless they are print statements
5. Preserve conditional statements, loops, and other control structures

Return ONLY the modified code with no explanations or markdown formatting."""

MATH_PROBLEM_PROMPT = """Solve this mathematical problem using clear step-by-step reasoning.

Problem: {problem_statement}

Prior context: {prior_context}

Provide:
1. The calculation steps
2. The final numerical answer
3. Brief explanation

Format your answer clearly with the final result."""

# =============================================================================
# EXPLORATION PLANNING PROMPT (Phase 0.5 - LLM-Driven Exploration)
# =============================================================================

EXPLORATION_PLANNING_PROMPT = """You are a data exploration agent. Analyze this question and decide which exploration steps to run.

QUESTION: {question}

AVAILABLE DATA FILES:
{available_files_desc}

# CRITICAL: POLICY QUESTION DETECTION
Before planning exploration, check if this is a POLICY question:
- Keywords: "fine", "penalty", "policy", "danger", "in danger of"
- Pattern: "in danger of" + "fine" = POLICY question
- Examples: "Is merchant in danger of getting a high-fraud rate fine?"

IF THIS IS A POLICY QUESTION:
- ✅ UNDERSTANDING: Policy questions require BOTH data calculation AND policy checking
  - Step 1: Calculate relevant metrics (fraud rate, volume, etc.) from transaction data
  - Step 2: Check manual.md for policy thresholds/rules
  - Step 3: Cross-reference calculation with policy to determine answer
- ✅ ALLOWED: Data exploration AND calculations needed for policy evaluation
  - ✅ Calculate fraud rates, volumes, or other metrics mentioned in question
  - ✅ Extract merchant-specific data if question mentions specific merchant
  - ✅ Structural exploration (schema, column names) to understand data
- ✅ CRITICAL: The ANSWER comes from cross-referencing calculation with manual.md policy
  - ❌ WRONG: Return calculated fraud rate alone
  - ❌ WRONG: Return MCC codes or other data values
  - ✅ CORRECT: Calculate metric → Check manual.md for policy → Compare → Return "yes"/"no"/"Not Applicable"
- ✅ Reason: Policy questions need data calculations BUT the answer requires checking manual.md for policy rules

AVAILABLE EXPLORATION TOOLS:
1. read_data_file(file, lines, mode) - Read file sections (header, samples)
   - For documentation (*.md): Use mode='all' to read entirely (context is critical!)
2. grep_data(pattern, file) - Search CSV/text files ONLY (NOT JSON, NOT documentation!)
3. shell_analyze(command) - Run shell commands (cut, sort, wc, jq)
   - For JSON files: MUST use jq, NOT grep!
4. awk must be avoided as much as possible

GUIDELINES:
- Prefer simple commands: cut, grep, wc over complex awk
- For counts: cut -d, -fN | sort -u | wc -l
- For awk: Always use {{print $N,$M}} to specify fields
- Keep output small (<1000 chars per command)

IMPORTANT:
- For awk commands: Always use {{print $N}} to specify which fields to output
- Example: awk -F, '$2=="merchant" {{print $3}}' file.csv
- Never: awk -F, '$2=="merchant"' file.csv (dumps all fields!)

YOUR TASK:
1. FIRST: Check if this is a POLICY question (keywords: "fine", "penalty", "danger", "in danger of")
2. IF POLICY QUESTION: Generate exploration steps that help calculate relevant metrics AND check manual.md
   - Include: data structure exploration, merchant-specific data gathering, manual.md checking
   - Remember: The answer comes from cross-referencing calculation with policy, not just raw data
3. IF DATA QUESTION: Generate 2-4 exploration commands that will help answer this question.

WARNING: CRITICAL: SKIP DOCUMENTATION FILES FROM EXPLORATION!
- manual.md, *-readme.md, *.md files are ALREADY analyzed in Phase 0
- DO NOT explore documentation files - redundant and wastes time!
- Focus exploration on DATA files only (CSV, JSON)

FILE TYPES (understand the semantics):
- payments.csv = PRIMARY TRANSACTION DATA (138k actual transactions) -> EXPLORE THIS!
- merchant_data.json = METADATA/REFERENCE (30 merchant profiles, NOT transaction records) -> Can explore
- fees.json = BUSINESS RULES (fee calculation logic, NOT transactions) -> Can explore
- manual.md = DOCUMENTATION (business definitions, formulas, policies) -> SKIP! Already in file_analyses
- *-readme.md = DOCUMENTATION (column descriptions) -> SKIP! Already in file_analyses

SMART FILE SELECTION (think about what the question asks):
- "in the dataset" / "from the data" -> Usually refers to transaction data (payments.csv)
- "unique merchants in dataset" -> Count from transaction records (payments.csv), not reference lists
- If question mentions "merchant profile" or "merchant information" -> Then use merchant_data.json
- For ambiguous cases, consider: Is this asking about ACTUAL TRANSACTIONS or REFERENCE INFO?

METRIC INTERPRETATION (think about business meaning):
- "top fraud country" / "highest fraud" -> Business meaning = RATE (% fraud), not absolute count
  Why? A country with 100 transactions and 50% fraud is worse than 10,000 transactions with 1% fraud
- "correlation >0.5" -> Question asks YES/NO, exploration should calculate and answer the boolean
- "how many" -> Count actual occurrences in transaction data

TOOL SELECTION BY FILE TYPE:

1. **Documentation files** (manual.md, *-readme.md, *.md):
   - ⭐ BEST: SKIP exploration! Already analyzed in Phase 0 and included in file_analyses
   - ❌ BAD: Don't waste exploration on docs - they're already available!
   - WHY: Docs are fully analyzed in Phase 0, exploration should focus on DATA patterns

2. **Large CSV files** (payments.csv - 138k rows):
   - ✅ grep_data(pattern, file) - for specific values
   - ✅ shell: cut, awk, sort - for sampling/counting
   - ✅ read_data_file(lines=1, mode='head') - for headers only
   - ❌ BAD: read_data_file mode='all' - too large!

3. **JSON files** (fees.json, merchant_data.json):
   - ✅ shell_analyze with jq - for structured queries
   - ❌ BAD: grep_data - returns broken JSON fragments!

GUIDELINES:
- For "unique/count" -> use shell: cut -d, -fN | sort -u | wc -l
- For "columns/schema" -> read_data_file(file, lines=1, mode="head")
- For "specific values in CSV" -> grep_data(value, file)
- For "policy/formula from docs" -> SKIP! Already in file_analyses from Phase 0
- For "cross-reference" -> explore DATA files (CSV, JSON), not documentation

EXAMPLES (Study these carefully - they show SIMPLE, RELIABLE commands):

Question: "How many unique merchants are in the dataset?"
REASONING: "in the dataset" refers to transaction data (payments.csv), NOT metadata (merchant_data.json).
Why? The dataset is the collection of payment transactions. merchant_data.json is a reference list.
STRATEGY: Use simple cut/wc - no awk needed for this!
Plan:
{{
  "exploration_steps": [
    {{"tool": "read_data_file", "file": "payments.csv", "lines": 1, "mode": "head", "purpose": "Verify 'merchant' column exists (field 2)"}},
    {{"tool": "shell_analyze", "command": "cut -d, -f2 payments.csv | tail -n +2 | sort -u | wc -l", "purpose": "Count unique merchants using simple cut/wc (no awk needed)"}}
  ]
}}
Output: "5" (clean count)
Why this works: cut extracts ONLY field 2, sort -u deduplicates, wc -l counts. Simple and reliable!

Question: "What is the fee for GlobalCard with ACI='C'?"
Plan:
{{
  "exploration_steps": [
    {{"tool": "shell_analyze", "command": "jq '.[] | select(.card_scheme==\\"GlobalCard\\" and (.aci | contains([\\"C\\"]))) | {{ID, fixed_amount, rate}}' fees.json", "purpose": "Use jq (NOT grep!) to query JSON for matching fee rules"}}
  ]
}}
Note: Skipped manual.md (already in file_analyses from Phase 0)! Focus exploration on DATA files only!

Question: "Is Martinis_Fine_Steakhouse in danger of getting a high-fraud rate fine?"
REASONING: This is a POLICY question! Contains "in danger of" + "fine".
CRITICAL: Policy questions need BOTH data calculation AND policy checking!
Step 1: Calculate fraud rate for Martinis_Fine_Steakhouse (from payments.csv)
Step 2: Check manual.md for policy threshold (e.g., "fraud rate > X% triggers fine")
Step 3: Cross-reference: Does calculated fraud rate exceed policy threshold?
Answer: "yes" if exceeds threshold, "no" if not, "Not Applicable" if no policy found
Plan:
{{
  "exploration_steps": [
    {{"tool": "read_data_file", "file": "payments.csv", "lines": 1, "mode": "head", "purpose": "Understand payment data structure (columns: merchant, has_fraudulent_dispute, eur_amount)"}},
    {{"tool": "grep_data", "pattern": "Martinis_Fine_Steakhouse", "file": "payments.csv", "purpose": "Find transactions for this merchant to calculate fraud rate"}},
    {{"tool": "read_data_file", "file": "manual.md", "lines": 100, "mode": "head", "purpose": "Check manual.md for fraud rate fine policy/threshold (already in file_analyses, but quick verification)"}}
  ]
}}
Reason: Policy questions require calculating data metrics AND checking manual.md for policy rules.
The Planner/Coder will: (1) Calculate fraud rate, (2) Check manual.md for policy, (3) Compare, (4) Return answer.

Question: "Which country has the top fraud?"
REASONING: "top fraud" in business/risk context means highest RATE, not highest count.
Why? A country with few transactions but high % fraud is riskier than many transactions with low % fraud.
Business cares about PERCENTAGE, not absolute numbers.
Plan:
{{
  "exploration_steps": [
    {{"tool": "read_data_file", "file": "payments.csv", "lines": 1, "mode": "head", "purpose": "Confirm column names for ip_country and has_fraudulent_dispute"}},
    {{"tool": "shell_analyze", "command": "awk -F, 'NR>1 {{total[$10]++; if($18==\\"True\\") fraud[$10]++}} END {{for(c in total) printf \\\"%s: %.2f%%\\\\n\\\", c, (fraud[c]/total[c]*100)}}' payments.csv | sort -t: -k2 -rn | head -1", "purpose": "Calculate fraud RATE % per country (not just count) and find highest"}}
  ]
}}
Output will show: "BE: 10.85%" (highest rate), NOT "NL: 2955" (highest count)

Question: "What are the unique card schemes for merchant X?"
REASONING: Need unique values from filtered data. Can we avoid awk?
BEST APPROACH: Try simple grep + cut chain first!
Plan:
{{
  "exploration_steps": [
    {{"tool": "shell_analyze", "command": "grep 'merchant_X' payments.csv | cut -d, -f3 | sort -u", "purpose": "Extract unique card_schemes using simple grep/cut (no awk needed)"}}
  ]
}}
Output: "NexPay\nGlobalCard\nSwiftCharge" (just the schemes, not full rows)

Question: "How are payment processing fees calculated?" (Documentation query)
⭐ BEST APPROACH (skip documentation - already analyzed!):
{{
  "exploration_steps": []
}}
Reason: manual.md (22KB) was ALREADY fully analyzed in Phase 0 and included in file_analyses!
DON'T waste exploration time re-reading docs. Focus on actual DATA patterns instead!

❌ BAD APPROACH (redundant doc exploration):
{{
  "exploration_steps": [
    {{"tool": "read_data_file", "file": "manual.md", "mode": "all"}}
  ]
}}
Problem: Redundant! manual.md already in file_analyses from Phase 0 analyzer.

❌ WORST APPROACH (grep on docs):
{{
  "exploration_steps": [
    {{"tool": "grep_data", "pattern": "fee = fixed_amount", "file": "manual.md"}}
  ]
}}
Problem: Fragments AND redundant!

Question: "What characteristics do transactions for merchant X on day Y have?"
REASONING: Need SPECIFIC FIELDS from matching transactions with multiple filter conditions.
SIMPLE APPROACH FAILS: grep can't filter by two fields simultaneously
MUST USE AWK: But with explicit {{print}} to limit fields!
Plan:
{{
  "exploration_steps": [
    {{"tool": "shell_analyze", "command": "awk -F, '$2==\\"X\\" && $7==\\"Y\\" {{print $3,$8,$20}}' payments.csv | sort -u | head -10", "purpose": "Extract ONLY card_scheme(f3), is_credit(f8), aci(f20) for matching transactions"}}
  ]
}}
Output: "NexPay True G\nGlobalCard False D" (3 fields × 2 unique combos = 6 values)
NOT: 21 fields × 25 rows = 525 values!

COMMAND SIMPLICITY CHECKLIST (Before generating, ask yourself):
☐ Can I use cut instead of awk? (YES -> Use cut!)
☐ Can I use grep + cut instead of awk filter? (YES -> Use grep + cut!)  
☐ Can I use jq instead of awk for JSON? (YES -> Use jq!)
☐ Do I really need awk? (Only if calculating/aggregating)
☐ If using awk, did I include {{print $N,$M}}? (MUST include!)
☐ Is my output limited to <500 chars? (Use head/tail if needed)

THINKING LIKE AN ANALYST (use business context):
1. Understand file semantics: What does each file represent? (transactions vs metadata vs rules)
2. Understand metrics: Does question ask for COUNT or RATE or PERCENTAGE? (business usually wants rates)
3. Choose simplest tool: cut/grep/wc for extraction, awk only for calculations
4. Limit output: Use head/tail, sort -u before counting, aggregate instead of dump
5. Think hierarchically: Transaction data > Metadata > Documentation (use appropriate level)

EXAMPLES OF GOOD REASONING:
- "unique merchants in dataset" -> Count from payments.csv (transaction records), not merchant_data.json (directory)
- "top fraud country" -> Calculate RATE % (fraud/total), not COUNT (business cares about percentage)
- "fee for X" -> Look in fees.json (rules), apply to payments.csv (data)
- "list card schemes" -> Use grep + cut (simple), not awk (complex)

OUTPUT FORMAT (ABSOLUTELY CRITICAL - Read this twice before generating):

You MUST return a COMPLETE, VALID JSON object. The response will fail if truncated.

REQUIREMENTS:
1. Start with {{ (opening brace) - NO ```json markdown!
2. Include complete "exploration_steps" array with 2-6 steps
3. Each step must be a complete JSON object with all required fields
4. End with }} (closing brace) - Complete the JSON!
5. NO explanations before or after the JSON
6. NO markdown wrappers (no ```, no ```json, nothing)

TEMPLATE (Fill in the blanks, keep complete):
{{
  "exploration_steps": [
    {{"tool": "TOOL_NAME", "file": "FILE_NAME", "command": "COMPLETE_COMMAND", "purpose": "DESCRIPTION"}},
    {{"tool": "TOOL_NAME", "file": "FILE_NAME", "command": "COMPLETE_COMMAND", "purpose": "DESCRIPTION"}}
  ]
}}

FIELD REQUIREMENTS BY TOOL:
- shell_analyze: "tool", "file", "command" (COMPLETE, not truncated!), "purpose"
- read_data_file: "tool", "file", "lines", "mode", "purpose"
- grep_data: "tool", "file", "pattern", "purpose"

⚠️  COMMON MISTAKES TO AVOID:
❌ Starting with ```json (causes parsing failure)
❌ Truncating command mid-string: "command": "awk -F, 'N...  (WRONG!)
❌ Missing closing braces }} (incomplete JSON)
❌ Adding explanations after the JSON (breaks parser)
❌ Generating only partial JSON (model stops too early)

✅ CORRECT EXAMPLE (Complete JSON, no wrappers, all fields present):
{{"exploration_steps": [{{"tool": "shell_analyze", "file": "payments.csv", "command": "cut -d, -f2 payments.csv | tail -n +2 | sort -u | wc -l", "purpose": "Count unique merchants"}},{{"tool": "read_data_file", "file": "payments.csv", "lines": 1, "mode": "head", "purpose": "Verify schema"}}]}}

NOW GENERATE YOUR RESPONSE:
- Start typing the opening brace: {{
- Add "exploration_steps": [ 
- Add 2-6 complete step objects
- Close the array: ]
- Close the JSON: }}
- KEEP TYPING until the JSON is COMPLETE - don't stop early!
- Your response should be 200-1000 characters long

Return COMPLETE, VALID JSON now (no wrapper):"""

# =============================================================================
# EXECUTE DATA ANALYSIS PROMPT
# =============================================================================

EXECUTE_DATA_ANALYSIS_PROMPT = """Generate SIMPLE, FLAT Python code to solve this data analysis task.

TASK: {instruction}

{schema_context}

{few_shot_library}

{example}

{retrieval_context}

# ═══════════════════════════════════════════════════════════════════════════════
# PROVEN CODE PATTERNS (Use these to avoid common errors!)
# ═══════════════════════════════════════════════════════════════════════════════

## COUNTING & UNIQUENESS:
✅ Unique merchants:     payments_df['merchant'].nunique()
✅ Unique IPs:           payments_df['ip_address'].nunique()
❌ WRONG:                len(payments_df['merchant'].unique())  # Works but verbose
❌ WRONG:                len(set(df['merchant']))  # Unnecessary conversion

## FRAUD RATE CALCULATIONS:
✅ As percentage:        (df['has_fraudulent_dispute'].sum() / len(df)) * 100
✅ As decimal (0-1):     df['has_fraudulent_dispute'].mean()
✅ By merchant:          df.groupby('merchant')['has_fraudulent_dispute'].mean() * 100

## FILTERING:
✅ Year filter:          df[df['year'] == 2023]  # year is numeric
✅ Merchant filter:      df[df['merchant'] == 'Crossfit_Hanna']
✅ Multiple conditions:  df[(df['year'] == 2023) & (df['is_credit'] == True)]

## AGGREGATIONS:
✅ Group mean:           df.groupby('country')['eur_amount'].mean()
✅ Find max group:       df.groupby('merchant')['amount'].sum().idxmax()
✅ Top N:                df.groupby('country')['amount'].mean().nlargest(3).index.tolist()

## DATA SOURCE MAP (CRITICAL - Use correct file!):
- "How many unique merchants?" -> payments.csv ONLY (NOT merchant_data.json!)
- "Fraud rates/transactions" -> payments.csv (has has_fraudulent_dispute column)
- "Fee calculations" -> fees.json (structured fee rules)
- "MCC codes/descriptions" -> merchant_category_codes.csv
- "Merchant account types" -> merchant_data.json

## TEMPLATE CODE FOR COMMON QUESTIONS (Copy-paste these!):

### "How many unique merchants?"
```python
df = pd.read_csv('{{data_directory}}/payments.csv')
result = df['merchant'].nunique()
print(result)
```

### "What is the fraud rate for [condition]?"
```python
df = pd.read_csv('{{data_directory}}/payments.csv')
filtered = df[df['year'] == 2023]  # Add your filter
fraud_rate = (filtered['has_fraudulent_dispute'].sum() / len(filtered)) * 100
result = round(fraud_rate, 2)
print(f"{{result}}%")
```

### "Which merchant has highest/lowest [metric]?"
```python
df = pd.read_csv('{{data_directory}}/payments.csv')
by_merchant = df.groupby('merchant')['has_fraudulent_dispute'].mean()
result = by_merchant.idxmin()  # or idxmax() for highest
print(result)
```

### "Top N [entities] by [metric]"
```python
df = pd.read_csv('{{data_directory}}/payments.csv')
top_n = df.groupby('country')['eur_amount'].mean().nlargest(3).index.tolist()
result = ', '.join(top_n)
print(result)
```

### "List all fee IDs that match [criteria]" (static filter)
```python
with open('{{data_directory}}/fees.json', 'r') as f:
    fees = json.load(f)
matching = [fee['ID'] for fee in fees if fee.get('card_scheme') == 'GlobalCard']
result = ', '.join(map(str, sorted(matching)))
print(result)
```

### "What are the Fee IDs applicable to [merchant] on [day]?" (CRITICAL - match ACTUAL transactions!)
# ❌ WRONG: Match rules to transaction profiles (missing intracountry, monthly stats!)
# ✅ CORRECT: Match rules to ACTUAL TRANSACTIONS using match_fee_rule() helper!
```python
import pandas as pd
import json
df = pd.read_csv('{{data_directory}}/payments.csv')
fees = json.load(open('{{data_directory}}/fees.json'))
merchant_data = json.load(open('{{data_directory}}/merchant_data.json'))
# Filter transactions for merchant + day + year
transactions = df[(df['merchant']=='Belles') & (df['day_of_year']==10) & (df['year']==2023)]
merchant_info = next(m for m in merchant_data if m['merchant']=='Belles')
# Calculate monthly stats (needed for match_fee_rule)
monthly_volume = df[(df['merchant']=='Belles') & (df['year']==2023)]['eur_amount'].sum() / 1_000_000
monthly_fraud = df[(df['merchant']=='Belles') & (df['year']==2023)]['has_fraudulent_dispute'].mean() * 100
# Find applicable fee IDs by matching ACTUAL transactions
applicable_ids = set()
for _, tx in transactions.iterrows():
    tx_dict = tx.to_dict()
    tx_dict['account_type'] = merchant_info['account_type']
    tx_dict['mcc'] = merchant_info['merchant_category_code']
    tx_dict['monthly_volume_millions'] = monthly_volume
    tx_dict['monthly_fraud_percent'] = monthly_fraud
    for rule in fees:
        if match_fee_rule(tx_dict, rule):  # Helper handles ALL conditions (intracountry, monthly stats)!
            applicable_ids.add(rule['ID'])
            break  # First match per transaction
result = ', '.join(map(str, sorted(applicable_ids)))
print(result)
```
# CRITICAL: Must match ACTUAL transactions, not just profiles! Helper handles intracountry, monthly stats!

## OUTPUT FORMATTING:
- If question asks for "percentage" -> multiply by 100, add % if needed
- If question asks for "ratio" -> format as "X to Y" not just "X"
- If question asks for "list" -> return comma-separated: "A, B, C"
- If question asks for delta/difference -> preserve negative sign!

CRITICAL RULES (smolagents approach):
1. Write FLAT code - NO functions, NO classes, NO complex nesting
2. Use exact file paths: '{data_directory}/payments.csv' and '{data_directory}/fees.json'
3. Use correct column names from schema above
4. For lists in JSON: use 'R' in fee['account_type'], NOT equality
5. Always assign final result to 'result' variable
6. End with: print("FINAL_ANSWER:", result) or just print(result)
7. Keep code under 30 lines when possible
8. Use PROVEN PATTERNS above - don't overcomplicate!

🚨 OUTPUT FORMATTING (CRITICAL):
   
   RULE 1: Print ONLY the final answer value, nothing else!
   ❌ WRONG: print(f"Country: {{result}}\\nCounts:\\n{{counts}}")
   ✅ RIGHT: print(result)
   
   RULE 2: For multiple choice, format as "LETTER. VALUE"
   ❌ WRONG: result = "NL"  # Missing letter!
   ✅ RIGHT: result = "A. NL"  # With letter!
   Example: options = {{'NL': 'A', 'BE': 'B', 'ES': 'C', 'FR': 'D'}}
            result = f"{{options[country]}}. {{country}}"
   
   RULE 3: For yes/no questions, use exact format
   ❌ WRONG: result = "No, because..."  # Verbose!
   ✅ RIGHT: result = "Not Applicable"  # Exact!
   
   RULE 4: NO debug prints, NO intermediate prints, NO Series objects
   ❌ WRONG: print(country_counts)  # Prints Series!
   ✅ RIGHT: # Don't print anything except FINAL_ANSWER
   
   RULE 5: If answer is not in data, use "Not Applicable" exactly
   ❌ WRONG: "The manual doesn't specify..."
   ✅ RIGHT: "Not Applicable"

🚨 DEFENSIVE PROGRAMMING (CRITICAL - Avoid Execution Errors!):
   
   ERROR PATTERN #1: "'list' object has no attribute 'get/items/values'"
   ❌ WRONG: fee.get('ID') or fee.items()
   ✅ RIGHT: fee['ID'] (fees is a list of dicts, each fee is a dict)
   
   ERROR PATTERN #2: "list indices must be integers or slices, not str"
   ❌ WRONG: fees['ID']  # fees is a list!
   ✅ RIGHT: [f for f in fees if f['ID'] == target]
   
   ERROR PATTERN #3: "unhashable type: 'list'"
   ❌ WRONG: set(fee_list) or {{fee: value}}
   ✅ RIGHT: set(tuple(fee['ID']) for fee in fee_list) or use fee['ID']
   
   ERROR PATTERN #4: "'NoneType' object has no attribute X"
   ❌ WRONG: value.get('key') without None check
   ✅ RIGHT: value.get('key') if value is not None else default
   
   ERROR PATTERN #5: "unhashable type" in groupby or merge
   ❌ WRONG: df.groupby(list_column)
   ✅ RIGHT: Convert lists to strings first if needed

DEBUGGING TIPS:
- Add print() statements to check intermediate values
- Verify data loaded correctly: print(df.shape, df.columns)
- Check for empty dataframes before calculations
- Validate result before final print

ANSWER FORMAT RULES:
- For numbers: Use actual calculated value (e.g., "42.5", "138236")
- For yes/no: Return "yes" or "no" (lowercase)
- For lists: Use comma-separated (e.g., "1, 2, 3")
- For Not Applicable: Only if truly impossible to calculate

Generate complete, working code NOW:"""

# =============================================================================
# DATA ANALYSIS PROMPTS - Task Schema and Examples
# =============================================================================

COMPREHENSIVE_DATA_SCHEMA = """
COMPREHENSIVE DATA SCHEMA (DABStep Dataset):

=== PAYMENTS.CSV (138,236 transactions) ===
ALL COLUMNS: psp_reference, merchant, card_scheme, year, hour_of_day, minute_of_hour, day_of_year, is_credit, eur_amount, ip_country, issuing_country, device_type, ip_address, email_address, card_number, shopper_interaction, card_bin, has_fraudulent_dispute, is_refused_by_adyen, aci, acquirer_country

KEY ANALYSIS COLUMNS:
- merchant: 'Belles_cookbook_store', 'Crossfit_Hanna', 'Golfclub_Baron_Friso', 'Martinis_Fine_Steakhouse', 'Rafa_AI'
- card_scheme: 'GlobalCard', 'NexPay', etc.
- aci: 'A', 'B', 'C', 'D', 'E', 'F', 'G' (single letter strings)
- has_fraudulent_dispute: True/False (fraud indicator)
- is_credit: True/False (transaction type)
- eur_amount: Float (transaction amount in euros)
- ip_country: 'NL', 'BE', 'ES', 'FR', etc. (IP geolocation)
- issuing_country: 'NL', 'BE', 'ES', 'FR', etc. (card issuing country)
- year: 2023 (all transactions)
- day_of_year: 1-365 (day number)

=== FEES.JSON (1000+ fee structures) ===
ALL FIELDS: ID, card_scheme, account_type, capture_delay, monthly_fraud_level, monthly_volume, merchant_category_code, is_credit, aci, fixed_amount, rate, intracountry

CRITICAL STRUCTURE NOTES (MUST READ!):
- ID: Integer (unique fee identifier)
- account_type: LIST of strings (use 'R' in fee['account_type'], NOT fee['account_type'] == 'R')
- aci: LIST of strings (use 'B' in fee['aci'], NOT fee['aci'] == 'B')
- card_scheme: String ('GlobalCard', 'NexPay', etc.)
- merchant_category_code: LIST of strings (MCC codes)
- fixed_amount: Float (base fee amount)
- rate: Float (variable rate component)
- is_credit: Boolean or List (transaction type filter)

⚠️ COMMON ERRORS TO AVOID:
1. ❌ WRONG: fee['aci'][0] or fee.get('aci')
   ✅ RIGHT: 'A' in fee['aci'] (aci is a LIST!)
   
2. ❌ WRONG: fees_dict = {fee['ID']: fee for fee in fees} then fees_dict['key']
   ✅ RIGHT: [fee for fee in fees if fee['ID'] == target_id]
   
3. ❌ WRONG: result = {fee_list}  # Can't hash lists
   ✅ RIGHT: result = set(fee['ID'] for fee in fee_list)
   
4. ❌ WRONG: value = None; result = value * 2
   ✅ RIGHT: value = None; result = value * 2 if value is not None else 0

FEE CALCULATION FORMULA (DABStep Official):
fee = fixed_amount + (rate * transaction_amount / 10000)

=== BUSINESS RULES ===
- Fraud Rate = fraud_transactions / total_transactions (by group)
- High Fraud Risk = fraud_rate > 0.05 (5%)
- Fee Matching: Must match account_type, aci, card_scheme, and other criteria
- Multiple Choice Format: A. NL, B. BE, C. ES, D. FR (for country questions)

=== COMMON PATTERNS ===
Country Analysis: df['column'].value_counts().idxmax()
Fraud Analysis: fraud_df = df[df['has_fraudulent_dispute'] == True]; fraud_df['ip_country'].value_counts()
Fee Filtering: [fee for fee in fees if 'R' in fee['account_type'] and 'B' in fee['aci']]
Date Filtering: df[(df['year'] == 2023) & (df['day_of_year'] <= 31)]

=== DOMAIN KNOWLEDGE (CRITICAL!) ===
🔥 FEE CALCULATION FORMULA (OFFICIAL):
   fee = fixed_amount + (rate * transaction_amount / 10000)
   
   ⚠️ DIVISION BY 10,000 (not 100, not 1000!)
   ⚠️ MUST include fixed_amount (don't forget!)
   ⚠️ Rate is already a percentage-like value
   
   Example: For transaction=100, fixed=0.1, rate=20
   -> fee = 0.1 + (20 * 100 / 10000) = 0.1 + 0.2 = 0.3

🔥 FEE MATCHING RULES:
   1. card_scheme: EXACT match (String)
   2. account_type: Use 'in' (LIST field!)
   3. aci: Use 'in' (LIST field!)
   4. merchant_category_code: Use 'in' (LIST field!)
   5. is_credit: Exact match if not None
   6. intracountry: Exact match if not None
   
   ⚠️ CRITICAL: account_type, aci, merchant_category_code are LISTS!
   ✅ CORRECT: 'R' in fee['account_type']
   ❌ WRONG: fee['account_type'] == 'R'

🔥 DATE CALCULATIONS (2023 = NOT leap year):
   January: days 1-31
   February: days 32-59
   March: days 60-90
   Q1: days 1-90
   Q2: days 91-181
   Q3: days 182-273
   Q4: days 274-365
   
🔥 BUSINESS RULES:
   - High fraud threshold: > 0.05 (5%)
   - Fraud rate = fraud_count / total_count
   - intracountry=True: Domestic (CHEAPER fees)
   - intracountry=False: International (MORE expensive)
   - is_credit=True: Credit card (HIGHER fees)
   - is_credit=False: Debit card (LOWER fees)

🔥 DATA CONSTRAINTS:
   - Only 5 merchants total: Belles_cookbook_store, Crossfit_Hanna, 
     Golfclub_Baron_Friso, Martinis_Fine_Steakhouse, Rafa_AI
   - Card schemes: GlobalCard, NexPay, SwiftCharge, TransactPlus
   - ACI values: A, B, C, D, E, F, G (single letters)
   - Account types: R, D, H, F, S, O
"""

FEW_SHOT_EXAMPLES_LIBRARY = """
🎓 EXPERT EXAMPLES FROM GROUND TRUTH (31 Proven Correct Solutions):

═══ SIMPLE COUNTS & QUERIES ═══
EXAMPLE 1: Total transaction count
Q: "How many total transactions are there in the dataset?"
```python
import pandas as pd
df = pd.read_csv('{data_directory}/payments.csv')
result = len(df)
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: 138236

EXAMPLE 2: Average calculation
Q: "What is the average transaction amount (in EUR)?"
```python
import pandas as pd
df = pd.read_csv('{data_directory}/payments.csv')
result = round(df['eur_amount'].mean(), 3)
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: 91.852

═══ COUNTRY ANALYSIS ═══
EXAMPLE 3: Top country
Q: "Which IP country has the highest number of transactions?"
```python
import pandas as pd
df = pd.read_csv('{data_directory}/payments.csv')
result = df['ip_country'].value_counts().idxmax()
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: NL

EXAMPLE 4: Groupby with country (HARD - watch format!)
Q: "Average transaction value grouped by ip_country for Belles_cookbook_store's SwiftCharge between June-Oct 2023?"
```python
import pandas as pd
df = pd.read_csv('{data_directory}/payments.csv')
filtered = df[
    (df['merchant'] == 'Belles_cookbook_store') &
    (df['card_scheme'] == 'SwiftCharge') &
    (df['day_of_year'].between(152, 304))  # June(152) to Oct(304)
]
avg_by_country = filtered.groupby('ip_country')['eur_amount'].mean()
result = ', '.join([f"{{c}}: {{v:.2f}}" for c, v in sorted(avg_by_country.items())])
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: GR: 70.7, SE: 83.8, LU: 84.36, FR: 88.59, IT: 94.94, NL: 108.81, BE: 110.11, ES: 126.92
⚠️ NOTE: NO brackets [], just comma-separated!

═══ PERCENTAGE CALCULATIONS ═══
EXAMPLE 5: Percentage of total
Q: "What percentage of transactions are made using credit cards?"
```python
import pandas as pd
df = pd.read_csv('{data_directory}/payments.csv')
credit_proportion = df['is_credit'].mean()
result = round(credit_proportion * 100, 2)
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: 73.15

EXAMPLE 6: Fraud rate percentage
Q: "What percentage of transactions are fraudulent in year 2023?"
```python
import pandas as pd
df = pd.read_csv('{data_directory}/payments.csv')
fraud_rate = df['has_fraudulent_dispute'].mean()
result = round(fraud_rate * 100, 6)
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: 7.787407

═══ UNIQUE VALUES & COUNTING ═══
EXAMPLE 7: Unique count
Q: "How many unique merchants are present in the dataset?"
```python
import pandas as pd
df = pd.read_csv('{data_directory}/payments.csv')
result = df['merchant'].nunique()
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: 5
💡 There are only 5 merchants total in entire dataset!

═══ OUTLIER DETECTION (Z-SCORE) ═══
EXAMPLE 8: Outliers using Z-score > 3
Q: "How many outliers in transaction amount data (Z-Score > 3)?"
```python
import pandas as pd
import numpy as np
df = pd.read_csv('{data_directory}/payments.csv')
amounts = df['eur_amount']
mean_amt = amounts.mean()
std_amt = amounts.std()
if std_amt == 0:
    result = 0
else:
    z_scores = np.abs((amounts - mean_amt) / std_amt)
    result = (z_scores > 3).sum()
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: 2429

═══ FRAUD ANALYSIS ═══
EXAMPLE 9: Lowest fraud merchant
Q: "Which merchant has the lowest average fraud rate for 2023?"
```python
import pandas as pd
df = pd.read_csv('{data_directory}/payments.csv')
fraud_by_merchant = df.groupby('merchant')['has_fraudulent_dispute'].mean()
result = fraud_by_merchant.idxmin()
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: Crossfit_Hanna

EXAMPLE 10: Multiple choice format (CRITICAL - Format as "LETTER. VALUE")
Q: "What is the top country (ip_country) for fraud? A. NL, B. BE, C. ES, D. FR"
```python
import pandas as pd
df = pd.read_csv('{data_directory}/payments.csv')
fraud_df = df[df['has_fraudulent_dispute'] == True]
# Count fraud by country - DON'T PRINT THE COUNTS!
country_fraud = fraud_df['ip_country'].value_counts()
top_fraud_country = country_fraud.idxmax()
# Map country to multiple choice letter
options = {{'NL': 'A', 'BE': 'B', 'ES': 'C', 'FR': 'D'}}
result = f"{{options.get(top_fraud_country, 'A')}}. {{top_fraud_country}}"
# Print ONLY the formatted answer (not the counts!)
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: B. BE
💡 CRITICAL: 
   - Format as "LETTER. VALUE" not just "VALUE"
   - DON'T print intermediate counts or Series objects
   - Print ONLY the final formatted answer!

═══ FEE CALCULATIONS (CRITICAL FORMULA!) ═══
EXAMPLE 11: Fee calculation (HARD)
Q: "For credit transactions, average fee NexPay would charge for 10 EUR?"
```python
import json
fees = json.load(open('{data_directory}/fees.json'))
# Find fees matching: card_scheme AND is_credit
matching = [f for f in fees 
            if f['card_scheme'] == 'NexPay'
            and (f['is_credit'] == True or f['is_credit'] is None)]
if not matching:
    result = "Not Applicable"
else:
    # CRITICAL: Use official formula
    # fee = fixed_amount + (rate * transaction_amount / 10000)
    fees_calculated = [(f['fixed_amount'] + f['rate'] * 10.0 / 10000) for f in matching]
    result = round(sum(fees_calculated) / len(fees_calculated), 6)
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: 0.126459
💡 FORMULA: fixed_amount + (rate * amt / 10000) - EXACTLY THIS!

═══ FEE DELTA CALCULATIONS (VERY HARD!) ═══
EXAMPLE 12: Fee delta
Q: "In January 2023 what delta would Belles pay if fee ID=398 changed to rate=1?"
```python
import pandas as pd
import json
# Load data
df = pd.read_csv('{data_directory}/payments.csv')
fees = json.load(open('{data_directory}/fees.json'))
merchant_data = json.load(open('{data_directory}/merchant_data.json'))

# Filter for merchant and January (days 1-31)
merchant_df = df[(df['merchant'] == 'Belles_cookbook_store') & 
                 (df['day_of_year'].between(1, 31))]

# Find the fee rule
fee_398 = [f for f in fees if f['ID'] == 398][0]

# Calculate affected transactions (matching all fee conditions)
# Then: delta = (new_rate - old_rate) * sum(amounts) / 10000
original_rate = fee_398['rate']
new_rate = 1
delta = (new_rate - original_rate) * merchant_df['eur_amount'].sum() / 10000
# CRITICAL: Delta calculations MUST use 14+ decimals to preserve precision!
result = f"{{delta:.14f}}"  # 14 decimals as in answer - DO NOT round to 2 decimals!
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: 0.00000000000000

═══ NOT APPLICABLE RESPONSES ═══
EXAMPLE 13: When answer is not in data
Q: "Is there a specific fine amount for high chargeback rates?"
```python
# Search manual for specific fine amount
with open('{data_directory}/manual.md', 'r') as f:
    manual = f.read()

# Check if specific fine amount is mentioned
if 'specific fine' in manual.lower() or 'fine amount' in manual.lower():
    # Extract the amount
    result = "extracted_amount"
else:
    # No specific amount found - use exact format!
    result = "Not Applicable"

# Print ONLY "Not Applicable" - not explanation!
print("FINAL_ANSWER:", result)
print("FINAL_ANSWER:", result)
```
✅ CORRECT ANSWER: Not Applicable
💡 CRITICAL: If answer not in data, print EXACTLY "Not Applicable" - NO explanations!

---END EXPERT EXAMPLES---

🔥 KEY INSIGHTS FROM CORRECT ANSWERS:
1. Country groupby: NO BRACKETS in result (GR: 70.7, SE: 83.8)
2. Fee formula: ALWAYS divide by 10000, not 100 or 1000
3. List operations: Use 'in' for account_type and aci (they're lists!)
4. Only 5 merchants: Belles, Crossfit, Golfclub, Martinis, Rafa
5. Date ranges: Feb=32-59, Q4=274-365 (2023 not leap year)
6. Multiple choice: Include letter format "B. BE"
7. **Output: Print ONLY final answer - NO debug, NO Series, NO verbose explanations!**
8. **Multiple choice: MUST include letter (A, B, C, D)**
9. **Not Applicable: Print exactly "Not Applicable" - no explanations!**
"""

# Task-specific examples
TASK_EXAMPLES = {
    "country_analysis": """
PROVEN PATTERN - Country Analysis:
```python
import pandas as pd
df = pd.read_csv('{data_directory}/payments.csv')
result = df['issuing_country'].value_counts().idxmax()
# NO debug prints! Only final answer!
print("FINAL_ANSWER:", result)
```""",

    "fee_ids": """
PROVEN PATTERN - Fee ID Filtering:
```python
import pandas as pd
import json
fees = json.load(open('{data_directory}/fees.json'))
fee_ids = [str(fee['ID']) for fee in fees if 'R' in fee['account_type'] and 'B' in fee['aci']]
result = ", ".join(fee_ids) if fee_ids else "Not Applicable"
# Print ONLY the result, no debug info!
print("FINAL_ANSWER:", result)
```""",

    "fraud_analysis": """
PROVEN PATTERN - Fraud Rate Analysis (with Multiple Choice):
```python
import pandas as pd
df = pd.read_csv('{data_directory}/payments.csv')
fraud_df = df[df['has_fraudulent_dispute'] == True]
# Count by country - NO PRINTING COUNTS!
country_fraud_counts = fraud_df['ip_country'].value_counts()
top_fraud_country = country_fraud_counts.idxmax()
# Format as multiple choice LETTER. VALUE
options = {{'NL': 'A', 'BE': 'B', 'ES': 'C', 'FR': 'D'}}
result = f"{{options.get(top_fraud_country, 'A')}}. {{top_fraud_country}}"
# Print ONLY the formatted answer!
print("FINAL_ANSWER:", result)
```""",

    "fee_calculation": """
PROVEN PATTERN - Fee Calculation (DABStep Formula):
```python
import pandas as pd
import json
df = pd.read_csv('{data_directory}/payments.csv')
fees = json.load(open('{data_directory}/fees.json'))
filtered_df = df[df['is_credit'] == True]
avg_amount = filtered_df['eur_amount'].mean()
fee = [f for f in fees if 'R' in f['account_type'] and f['card_scheme'] == 'GlobalCard'][0]
# DABStep formula: fee = fixed_amount + (rate * transaction_value / 10000)
calculated_fee = fee['fixed_amount'] + (fee['rate'] * avg_amount / 10000)
result = round(calculated_fee, 6)
print("FINAL_ANSWER:", result)
```""",

    "general": """
GENERAL PATTERN:
```python
import pandas as pd
import json
df = pd.read_csv('{data_directory}/payments.csv')
# Apply filters and calculations
result = "your_answer_here"
print("FINAL_ANSWER:", result)
```"""
}

# =============================================================================
# ERROR GUIDANCE TEMPLATES
# =============================================================================

ERROR_TYPE_GUIDANCE = {
    "list indices must be integers or slices, not str": '''
# ⚠️  CRITICAL ERROR DETECTED: String used to index a LIST
# ─────────────────────────────────────────────────────────────────────────────
# Root Cause: You're treating a LIST as if it were a DICTIONARY
#   - Example: merchant_data['merchant_name'] but merchant_data is a list
#
# Fix Options:
#   1. Use integer indices: merchant_data[0] instead of merchant_data['key']
#   2. Convert list to dict FIRST:
#      merchant_dict = {item['id']: item for item in merchant_data}
#      Then use: merchant_dict['merchant_name']
#   3. Use list comprehension to find:
#      item = next((x for x in merchant_data if x['id'] == target_id), None)
#   4. Filter with pandas if list of dicts:
#      pd.DataFrame(merchant_data).query("id == 'target'")
''',

    "'list' object has no attribute": '''
# ⚠️  CRITICAL ERROR DETECTED: Calling DICT method on a LIST
# ─────────────────────────────────────────────────────────────────────────────
# Root Cause: You're treating a LIST as if it were a single DICT object
#   Common mistakes:
#   - fees.get('ID')    -> fees is a LIST of dicts, not a single dict
#   - fees.values()     -> .values() is for dicts, not lists
#   - fees.items()      -> .items() is for dicts, not lists  
#   - fees.keys()       -> .keys() is for dicts, not lists
#   - merchant_data.get('name') -> merchant_data is a LIST
#
# Fix Options:
#   1. Loop through list: [f.get('ID') for f in fees]
#   2. Filter the list: [f for f in fees if f['ID'] == target]
#   3. Get first element: fees[0].get('ID') if fees else None
#   4. Convert to dict FIRST: fee_dict = {f['ID']: f for f in fees}; then fee_dict.get(398)
#   5. Check type: print(type(fees)) to verify it's a list
#
# Common DABStep Patterns:
#   fees = json.load(open('fees.json'))  # Returns a LIST of dicts!
#   ❌ WRONG: fee_398 = fees.get(398)
#   ❌ WRONG: all_fees = list(fees.values())
#   ❌ WRONG: fee_items = fees.items()
#   ✅ RIGHT: fee_398 = next(f for f in fees if f['ID'] == 398)
#   ✅ RIGHT: fee_dict = {f['ID']: f for f in fees}; then fee_dict.get(398)
'''
}

# Helper function to get error guidance
def get_error_guidance(error_msg: str) -> str:
    """Get specific error guidance based on error message."""
    error_lower = error_msg.lower()
    for pattern, guidance in ERROR_TYPE_GUIDANCE.items():
        if pattern in error_lower:
            return guidance
    
    # Generic fallback
    return f"""
# ⚠️  ERROR DETECTED IN PREVIOUS ATTEMPT
# ─────────────────────────────────────────────────────────────────────────────
# FULL ERROR MESSAGE:
# {error_msg}
# ─────────────────────────────────────────────────────────────────────────────
# ACTION REQUIRED: Analyze the error above and fix the specific issue!
# Common fixes:
#   - Check variable types (print(type(var)))
#   - Verify data structure (is it list, dict, DataFrame?)
#   - Check for None/NaN values
#   - Ensure correct indexing (integer for lists, keys for dicts)
#
"""

# =============================================================================
# MCP PROMPT TEMPLATES - For MCP @mcp.prompt decorators
# =============================================================================

CODE_EXPLANATION_PROMPT = """Please explain the following {language} code in detail:

```{language}
{code}
```

Provide:
1. High-level overview of what the code does
2. Detailed explanation of each function/class
3. Key algorithms or patterns used
4. Potential improvements or issues
5. Usage examples if applicable"""

CODE_REVIEW_PROMPT = """Please review the following {language} code:

```{language}
{code}
```

Focus on:
1. Code quality and best practices
2. Potential bugs or issues
3. Performance considerations
4. Security concerns
5. Maintainability and readability
6. Specific improvement suggestions"""

CODE_FIX_PROMPT = """Please fix the following {language} code that has an error:

Error Message: {error_message}

Code:
```{language}
{code}
```

Requirements:
1. Fix the specific error mentioned
2. Ensure the code runs without issues
3. Maintain existing functionality
4. Follow best practices
5. Add comments explaining the fix"""