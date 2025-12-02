# SuperInference DABStep Submission

## Model Information
- **Model**: SuperInference with Gemini 2.5 Pro
- **Architecture**: SuperInference - Supervised Inference for Partially Observable Environments  
- **Submission Date**: 20251122_080517
- **Total Tasks**: 75

## Performance Summary
- **Overall Accuracy**: 86.7%
- **Average Response Time**: 263.99s
- **Average Confidence**: 0.457
- **Solution Quality**: 0.687
- **Context Relevance**: 0.630

## Technical Details
- **Vector Embeddings**: gemini-embedding-001
- **Critic Validation**: Gemini 2.5 Pro for step validation
- **Code Execution**: Server-side Python execution with safety checks with schema awareness
- **Context Integration**: 7 DABStep context files with 138k transactions

## Files for Submission
- `answers.jsonl`: Required submission file with task_id and agent_answer
- `submission_metadata.json`: Technical details and performance metrics

## Submission Format
Each line in `answers.jsonl` follows the official DABStep format:
```json
{"task_id": "task_id", "agent_answer": "answer", "reasoning_trace": "steps"}
```

## Evaluation Criteria
- **Scoring**: Percentage of correct answers via quasi-exact match
- **Answer Types**: String (words), number, or comma-separated list
- **Normalization**: Applied based on ground truth type
- **Single Correct Answer**: Only one correct answer per task

## Submission Instructions
1. Upload `answers.jsonl` to https://huggingface.co/spaces/adyen/DABstep
2. Model name: **SuperInference Gemini-2.5-pro**
3. Include technical details from `submission_metadata.json` in description

## Theoretical Validation
This submission demonstrates the SuperInference framework's capabilities on real-world
financial data analysis tasks, validating the theoretical claims about event-driven
multi-step reasoning architectures.

## Expected Performance
- **Easy Tasks**: Target 68% accuracy (baseline Llama 3.3 70B)
- **Hard Tasks**: Target 3.7% accuracy (baseline Llama 3.3 70B)
- **SuperInference Advantage**: Multi-step reasoning with critic validation
