# DABStep Benchmark for SuperInference

A streamlined benchmark suite for evaluating SuperInference on **DABStep** (Data Agent Benchmark for Multi-step Reasoning), a challenging dataset of 450+ real-world data analysis tasks.

## 🎯 Why DABStep?

DABStep is specifically designed to evaluate multi-step reasoning and data analysis capabilities - exactly what SuperInference excels at:

- **Multi-step reasoning**: Requires 3-5 sequential analysis steps
- **Real-world data**: Actual business analysis scenarios from Adyen
- **Code execution**: Requires writing and running Python code
- **Domain knowledge**: Financial data analysis with complex business rules
- **Planning advantage**: Perfect fit for SuperInference's PRE loop architecture

**Current SOTA**: Only 16% accuracy (o3-mini) - huge opportunity for SuperInference!

## 🚀 Quick Start

### Prerequisites

1. **MCP Server Running**:
```bash
   cd /path/to/superinference
   python3 agent/mcp_server.py --http
```

2. **Install Dependencies**:
```bash
cd agent/benchmark
pip install -r requirements.txt
```

### Run Benchmark

**Basic Usage** (5 problems, both difficulty levels):
```bash
python3 dabstep_benchmark.py
```

**Custom Configuration**:
```bash
# Test 10 problems, hard difficulty only, specific methods
python3 dabstep_benchmark.py --problems 10 --difficulty hard --methods superinference iterative

# Test easy problems only
python3 dabstep_benchmark.py --problems 15 --difficulty easy --output-dir my_results
```

### Command Line Options

```bash
python3 dabstep_benchmark.py [options]

Options:
  --output-dir DIR         Output directory (default: dabstep_results)
  --problems N             Number of problems per difficulty (default: 5)
  --difficulty LEVEL       easy, hard, or both (default: both)
  --methods METHOD [...]   Methods to test (default: static iterative superinference)
```

## 📊 Understanding Results

### Output Files

Each run creates a timestamped directory with:
- `dabstep_results_TIMESTAMP.json` - Detailed results
- `dabstep_summary_TIMESTAMP.md` - Human-readable summary
- `dabstep_plots_TIMESTAMP.png` - Performance visualizations
- `dabstep_benchmark_TIMESTAMP.log` - Execution logs

### Example Results

```
STATIC: 12.0% accuracy (3/25)
ITERATIVE: 24.0% accuracy (6/25) 
SUPERINFERENCE: 32.0% accuracy (8/25)

📁 Results saved to: dabstep_results/20250106_143022
⏱️  Total time: 245.67s
```

## 🔧 Architecture

### Single Script Design

All functionality consolidated into `dabstep_benchmark.py`:
- **DABStep dataset loading** from Hugging Face
- **Three evaluation methods**: static, iterative, superinference
- **Robust answer extraction** with multiple fallback strategies
- **Comprehensive analysis** with difficulty-level breakdowns
- **Rich visualizations** showing performance across methods

### Evaluation Methods

1. **Static**: Single-pass analysis with direct prompting
2. **Iterative**: Initial analysis + refinement step
3. **SuperInference**: Full PRE loop with planning and execution

### Answer Extraction

Smart extraction pipeline handles various response formats:
- Pattern matching for "Final answer: X"
- Fuzzy matching with 80% similarity threshold
- Fallback to last non-code line in response
- Error handling for malformed responses

## 📈 Expected Performance

Based on DABStep leaderboard and SuperInference capabilities:

| Method | Expected Accuracy | Reasoning |
|--------|------------------|-----------|
| Static | 8-12% | Simple prompting struggles with multi-step tasks |
| Iterative | 15-20% | Refinement helps catch errors |
| **SuperInference** | **25-35%** | Planning + execution ideal for data analysis |

**SuperInference Advantages**:
- Multi-step planning matches task requirements
- Code execution capabilities essential for DABStep
- Critic validation helps with complex calculations
- Iterative refinement catches analytical errors

## 🔍 Task Examples

**Easy Task**:
> "Which card scheme had the highest average fraud rate in 2023?"
> *Requires: Data exploration + aggregation + comparison*

**Hard Task**:
> "For merchant Crossfit Hanna in 2023, which Authorization Characteristics Indicator would minimize fees while reducing fraud through user incentives?"
> *Requires: Multi-dataset joins + complex business logic + optimization*

## 🎛️ Configuration

### Difficulty Levels
- **Easy**: Single dataset, basic aggregations (human baseline: 62%)
- **Hard**: Multiple datasets, complex business logic (human baseline: ~40%)
- **Both**: Balanced mix for comprehensive evaluation

### Dataset Access
- Automatically downloads from `adyen/DABstep` on Hugging Face
- Uses efficient Parquet format for fast loading
- Caches locally to avoid repeated downloads

## 🔧 Troubleshooting

**MCP Connection Issues**:
   ```bash
# Check server status
curl http://localhost:3000/mcp -d '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"health_check","arguments":{}},"id":1}'
   ```

**Dataset Loading Issues**:
```bash
# Test dataset access
python3 -c "from datasets import load_dataset; print(load_dataset('adyen/DABstep', 'tasks', split='default')[:3])"
```

**Memory Issues**:
- Reduce `--problems` parameter
- Use `--difficulty easy` for simpler tasks
- Monitor system memory during execution

## 📚 Understanding DABStep

DABStep challenges models to:
1. **Understand** complex business scenarios
2. **Explore** multiple datasets to find relevant information  
3. **Code** data processing and analysis pipelines
4. **Reason** through multi-step analytical workflows
5. **Validate** results against business constraints

This makes it an ideal benchmark for SuperInference, which is specifically designed for complex, multi-step reasoning tasks that require planning and execution.

## 🎯 Next Steps

1. **Run initial benchmark** to establish baseline
2. **Analyze failure modes** in detailed logs
3. **Optimize prompting** for better task understanding
4. **Scale up evaluation** with more problems
5. **Compare with SOTA** models on the leaderboard

The goal is to demonstrate SuperInference's advantages on real-world, multi-step reasoning tasks where planning and systematic execution provide clear benefits over simpler approaches. 