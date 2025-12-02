# SuperInference Agent Deployment Guide

**Version**: 1.0 (Unified SuperInference-STAR)  
**Purpose**: Deploy agent for DABStep 450-problem benchmark  
**Platforms**: Docker, Kubernetes

---

## 🐳 Docker Build

### Build Image

```bash
cd /home/ccamacho/dev/superinference/agent

# Build image
docker build -t superinference-agent:latest .

# Or with specific tag
docker build -t superinference-agent:unified-v1.0 .
```

### Test Locally

**1. Run MCP Server**:
```bash
docker run -d \
  --name superinference-server \
  -p 3000:3000 \
  -e DEFAULT_PROVIDER=vllm \
  -e DEFAULT_TEMPERATURE=0.1 \
  -e VLLM_BASE_URL=http://your-vllm-server:8000/v1 \
  -v $(pwd)/logs:/app/logs \
  superinference-agent:latest
```

**2. Run Benchmark (10 tasks test)**:
```bash
docker run --rm \
  --name superinference-benchmark-test \
  -e RUN_BENCHMARK=true \
  -e BENCHMARK_PROBLEMS=10 \
  -e BENCHMARK_DIFFICULTY=both \
  -e DEFAULT_PROVIDER=vllm \
  -e VLLM_BASE_URL=http://your-vllm-server:8000/v1 \
  -v $(pwd)/benchmark/dabstep/results:/app/benchmark/dabstep/results \
  superinference-agent:latest
```

---

## ☸️ Kubernetes Deployment

### Prerequisites

1. **Kubernetes cluster** with sufficient resources
2. **vLLM deployment** (or Gemini API key)
3. **Persistent storage** for results

### Deploy to Kubernetes

**1. Create namespace**:
```bash
kubectl create namespace superinference
```

**2. Create secrets** (if using Gemini):
```bash
kubectl create secret generic superinference-secrets \
  --from-literal=gemini-api-key=YOUR_API_KEY \
  -n superinference
```

**3. Deploy benchmark job**:
```bash
cd /home/ccamacho/dev/superinference/agent

# Apply PVC first
kubectl apply -f k8s/benchmark-job.yaml -n superinference

# Wait for PVC to be bound
kubectl get pvc -n superinference

# Start benchmark job
kubectl apply -f k8s/benchmark-job.yaml -n superinference
```

**4. Monitor progress**:
```bash
# Watch job status
kubectl get jobs -n superinference -w

# View logs
kubectl logs -f job/superinference-dabstep-benchmark -n superinference

# Check pod status
kubectl get pods -n superinference
```

**5. Retrieve results**:
```bash
# Find the pod
POD=$(kubectl get pods -n superinference -l job-name=superinference-dabstep-benchmark -o jsonpath='{.items[0].metadata.name}')

# Copy results locally
kubectl cp superinference/$POD:/app/benchmark/dabstep/results ./results

# Or access via PVC
kubectl exec -it $POD -n superinference -- ls /app/benchmark/dabstep/results
```

---

## 🔧 Configuration

### Environment Variables

**Provider Configuration**:
```bash
# Use vLLM (recommended for 450 tasks)
DEFAULT_PROVIDER=vllm
VLLM_BASE_URL=http://vllm-llama-70b:8000/v1
VLLM_MODEL=meta-llama/Llama-3.3-70B-Instruct

# Or Gemini (with API limits)
DEFAULT_PROVIDER=gemini
GEMINI_API_KEY=your-key-here
GEMINI_MODEL=gemini-2.5-pro
```

**Generation Configuration**:
```bash
DEFAULT_TEMPERATURE=0.1        # Deterministic (recommended)
DEFAULT_MAX_TOKENS=100000      # Long outputs for complex code
DEFAULT_TOP_P=0.8
DEFAULT_TOP_K=40
```

**Benchmark Configuration**:
```bash
BENCHMARK_PROBLEMS=450         # Full dataset
BENCHMARK_DIFFICULTY=both      # Easy + Hard
BENCHMARK_MODE=true            # Enable optimizations
```

**Planning Configuration**:
```bash
CRITIC_ACCEPT_THRESHOLD=0.6    # Critic approval threshold
MAX_EVENTS=10                  # Event budget (SuperInference)
MAX_ROUNDS=20                  # Round limit (SUPER-INFERENCE)
```

### Resource Requirements

**For 450-task benchmark**:

**Minimum**:
- CPU: 4 cores
- Memory: 8GB
- Storage: 10GB
- Time: 24-48 hours

**Recommended**:
- CPU: 8 cores
- Memory: 16GB
- Storage: 20GB
- Time: 12-24 hours

**With GPU** (if vLLM local):
- GPU: 1x A100 40GB or 2x A10 24GB
- Memory: 32GB
- Time: 6-12 hours

---

## 📊 Benchmark Execution

### Full 450-Task Run

```bash
# Using Kubernetes
kubectl apply -f k8s/benchmark-job.yaml -n superinference

# Monitor
kubectl logs -f job/superinference-dabstep-benchmark -n superinference | \
  grep -E "(Task [0-9]/|accuracy|COMPLETE)"
```

### Subset Testing

**10 tasks** (quick validation):
```bash
# Update job yaml
BENCHMARK_PROBLEMS=10

# Or override
kubectl set env job/superinference-dabstep-benchmark \
  BENCHMARK_PROBLEMS=10 -n superinference
```

**50 tasks** (medium validation):
```bash
BENCHMARK_PROBLEMS=50
BENCHMARK_DIFFICULTY=easy  # Or 'hard' or 'both'
```

---

## 📁 Results Collection

### Outputs Generated

After completion, results include:

```
/app/benchmark/dabstep/results/dabstep_run_TIMESTAMP/
├── dabstep_results_TIMESTAMP.json              # Complete metrics
├── dabstep_summary_TIMESTAMP.md                # Human-readable summary
├── dabstep_plots_TIMESTAMP.png                 # Comprehensive dashboard
├── dabstep_theoretical_validation_TIMESTAMP.png # Formula validation
├── dabstep_information_theory_TIMESTAMP.png    # EIG, entropy plots
├── dabstep_calibration_analysis_TIMESTAMP.png  # Calibration metrics
├── dabstep_performance_dashboard_TIMESTAMP.png # Performance analysis
├── dabstep_detailed_analysis_TIMESTAMP.png     # Detailed breakdown
├── dabstep_benchmark_TIMESTAMP.log             # Complete logs
└── huggingface_submission/
    ├── answers.jsonl                           # Submission file
    ├── submission_metadata.json                # Metadata
    └── README.md                               # Submission guide
```

### Retrieve from Kubernetes

```bash
# Get pod name
POD=$(kubectl get pods -n superinference \
  -l job-name=superinference-dabstep-benchmark \
  -o jsonpath='{.items[0].metadata.name}')

# Copy entire results directory
kubectl cp superinference/$POD:/app/benchmark/dabstep/results \
  ./local-results

# Or via PVC
kubectl exec -it $POD -n superinference -- \
  tar czf /tmp/results.tar.gz /app/benchmark/dabstep/results

kubectl cp superinference/$POD:/tmp/results.tar.gz ./results.tar.gz
```

---

## 🔍 Monitoring & Debugging

### View Logs

**Real-time**:
```bash
kubectl logs -f job/superinference-dabstep-benchmark -n superinference
```

**Filtered for progress**:
```bash
kubectl logs job/superinference-dabstep-benchmark -n superinference | \
  grep -E "(Task [0-9]/|accuracy|SuperInference Metrics)"
```

**Check for errors**:
```bash
kubectl logs job/superinference-dabstep-benchmark -n superinference | \
  grep -E "(ERROR|Exception|Failed)"
```

### Debug Failed Job

```bash
# Describe job
kubectl describe job superinference-dabstep-benchmark -n superinference

# Get pod events
kubectl get events -n superinference --sort-by=.lastTimestamp

# Exec into pod
kubectl exec -it $POD -n superinference -- /bin/bash

# Check results so far
kubectl exec -it $POD -n superinference -- \
  ls -lh /app/benchmark/dabstep/results
```

---

## 🚀 Quick Start

### Option 1: Local Docker

```bash
# 1. Build
docker build -t superinference-agent:latest .

# 2. Run 10-task test
docker run --rm \
  -e RUN_BENCHMARK=true \
  -e BENCHMARK_PROBLEMS=10 \
  -e BENCHMARK_DIFFICULTY=both \
  -v $(pwd)/results:/app/benchmark/dabstep/results \
  superinference-agent:latest

# 3. Check results
ls -lh results/dabstep_run_*/
```

### Option 2: Docker Compose

```bash
# Build and run server
docker-compose up -d mcp-server

# Run benchmark
docker-compose --profile benchmark up benchmark

# View logs
docker-compose logs -f benchmark
```

### Option 3: Kubernetes

```bash
# 1. Build and push
docker build -t your-registry/superinference-agent:latest .
docker push your-registry/superinference-agent:latest

# 2. Update k8s/benchmark-job.yaml with your image

# 3. Deploy
kubectl apply -f k8s/benchmark-job.yaml -n superinference

# 4. Monitor
kubectl logs -f job/superinference-dabstep-benchmark -n superinference
```

---

## 📋 Checklist for 450-Task Run

**Before Starting**:
- [ ] Docker image built and tested
- [ ] Provider configured (vLLM or Gemini)
- [ ] Resources allocated (16GB RAM, 8 CPU recommended)
- [ ] PVC created (10GB storage)
- [ ] Secrets created (if using Gemini)

**During Execution**:
- [ ] Monitor logs for progress
- [ ] Check resource usage
- [ ] Verify no OOM errors
- [ ] Confirm results being saved

**After Completion**:
- [ ] Retrieve results from PVC
- [ ] Review dabstep_summary.md
- [ ] Check accuracy metrics
- [ ] Analyze theoretical validation plots
- [ ] Prepare HuggingFace submission

---

## 🎯 Expected Timeline

**For 450 tasks** (based on current performance):

**With vLLM** (local, fast):
- Average: ~165s per task
- Total: ~20.6 hours
- Recommended: 24-hour job limit

**With Gemini API** (rate limited):
- Average: ~180s per task (with retries)
- Total: ~22.5 hours
- Recommended: 36-hour job limit with higher retry

**Resource Usage**:
- Memory: 8-12GB steady
- CPU: 60-80% utilization
- Storage: ~5GB for all results

---

## ✅ Deployment Complete!

**What You Have**:
- ✅ Dockerfile (production-ready)
- ✅ K8s Job manifest (450-task benchmark)
- ✅ Docker Compose (local testing)
- ✅ .dockerignore (optimized builds)
- ✅ Complete deployment guide

**Ready to**:
- Build Docker image
- Run local tests
- Deploy to Kubernetes
- Execute full 450-task benchmark

**All infrastructure in place!** 🚀


