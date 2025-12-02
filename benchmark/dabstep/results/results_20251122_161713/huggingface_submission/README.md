# SuperInference HuggingFace Submission

This directory contains the merged results from all benchmark chunks.

## Files

- **answers.jsonl**: All task answers in HuggingFace format
- **submission_metadata.json**: Combined statistics and configuration
- **README.md**: This file

## Format

Each line in `answers.jsonl` contains:
```json
{
  "task_id": "123",
  "agent_answer": "the answer",
  "reasoning_trace": "explanation"
}
```

## Submission

To submit to DABStep leaderboard:
1. Verify all tasks are present: `wc -l answers.jsonl`
2. Upload to HuggingFace dataset following DABStep guidelines
3. Include submission_metadata.json for reproducibility

## Statistics

See `submission_metadata.json` for detailed performance metrics.
