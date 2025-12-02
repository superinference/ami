# Kubernetes Deployment for SuperInference Benchmark

**Purpose**: Run 450-task DABStep benchmark on Kubernetes  
**Pattern**: Init container → Benchmark → Sidecar (for file retrieval)

---

## 🚀 Quick Start

### Step 1: Create Secret from Your .env File

```bash
# Navigate to agent directory
cd /home/ccamacho/dev/superinference/agent

kubectl delete secret superinference-env --namespace bench

# Create secret from your local .env file
kubectl create secret generic superinference-env \
  --from-file=.env=.env \
  --namespace bench \
  --dry-run=client -o yaml | kubectl apply -f -

# Verify secret created
kubectl get secret superinference-env -n bench
kubectl describe secret superinference-env -n bench
```

**Your .env should contain**:
```bash
# Example .env content
DEFAULT_PROVIDER=gemini
DEFAULT_TEMPERATURE=0.1
GEMINI_API_KEY=your-actual-api-key-here
GEMINI_MODEL=gemini-2.5-pro
VLLM_BASE_URL=http://your-vllm-endpoint/v1
```

### Step 2: Deploy Benchmark Job

```bash
# Apply the job manifest
oc delete job --all -n bench
kubectl apply -f benchmark-job-parallel-4.yaml

# Verify job created
kubectl get jobs -n bench
```

### Step 3: Monitor Execution

```bash
# Get pod name
POD=$(kubectl get pods -n bench -l job-name=superinference-dabstep-benchmark -o jsonpath='{.items[0].metadata.name}')

# Follow logs from benchmark container
kubectl logs -f $POD -c benchmark -n bench

# Filter for important events
kubectl logs -f $POD -c benchmark -n bench | grep -E "(Task [0-9]/|accuracy|Temperature|Critic)"
```

### Step 4: Retrieve Results (While Job Running or After)

```bash
# Get pod name
POD=$(kubectl get pods -n bench -l job-name=superinference-dabstep-benchmark -o jsonpath='{.items[0].metadata.name}')

# Copy entire results directory
kubectl cp bench/$POD:/output ./dabstep-results -c sidecar

# Or specific files
kubectl cp bench/$POD:/output/dabstep_results_*.json ./results.json -c sidecar
kubectl cp bench/$POD:/output/*.png ./plots/ -c sidecar

# List available files
kubectl exec -it $POD -c sidecar -n bench -- ls -lh /output
```

---

## 📊 Job Workflow

### Init Container: timestamp-generator
```
1. Generate timestamp (e.g., 20251024-133314)
2. Write to /shared/timestamp
3. Exit
```

### Main Container: benchmark
```
1. Load .env from secret
2. Record start time
3. Show configuration
4. Run: python3 dabstep_benchmark.py --problems 450 --difficulty both
5. Save results to /output
6. Record end time
7. Signal completion
8. Exit after 60s
```

### Sidecar Container: sidecar
```
1. Start
2. Sleep infinity (stays alive)
3. Allows file retrieval via kubectl cp
```

---

## 🔧 Configuration

### Modify Benchmark Size

Edit `k8s/benchmark-job.yaml`:

**10 tasks** (testing):
```yaml
env:
- name: BENCHMARK_PROBLEMS
  value: "10"
```

**50 tasks** (validation):
```yaml
env:
- name: BENCHMARK_PROBLEMS
  value: "50"
```

**450 tasks** (full run):
```yaml
env:
- name: BENCHMARK_PROBLEMS
  value: "450"
```

### Change Provider

**For vLLM**:
```bash
# Update secret
kubectl create secret generic superinference-env \
  --from-literal=DEFAULT_PROVIDER=vllm \
  --from-literal=VLLM_BASE_URL=http://your-vllm:8000/v1 \
  --from-literal=DEFAULT_TEMPERATURE=0.1 \
  -n bench --dry-run=client -o yaml | kubectl apply -f -
```

**For Gemini**:
```bash
# Update secret with API key
kubectl create secret generic superinference-env \
  --from-literal=DEFAULT_PROVIDER=gemini \
  --from-literal=GEMINI_API_KEY=your-key \
  --from-literal=DEFAULT_TEMPERATURE=0.1 \
  -n bench --dry-run=client -o yaml | kubectl apply -f -
```

---

## 📁 Results Retrieval

### During Execution

```bash
# While benchmark is running, you can copy intermediate results
POD=$(kubectl get pods -n bench -l job-name=superinference-dabstep-benchmark -o jsonpath='{.items[0].metadata.name}')

# Copy checkpoint files (updated periodically)
kubectl cp bench/$POD:/output/checkpoint_superinference.json ./checkpoint.json -c sidecar

# Monitor progress in real-time
kubectl exec -it $POD -c sidecar -n bench -- tail -f /output/benchmark.log
```

### After Completion

```bash
POD=$(kubectl get pods -n bench -l job-name=superinference-dabstep-benchmark -o jsonpath='{.items[0].metadata.name}')

# Get complete results
kubectl cp bench/$POD:/output ./complete-results -c sidecar

# Results include:
# - dabstep_results_*.json (metrics)
# - dabstep_summary_*.md (readable summary)
# - *.png (validation plots)
# - huggingface_submission/ (submission files)
# - benchmark.log (complete logs)
```

---

## 🐛 Troubleshooting

### Secret Not Loading

```bash
# Check secret exists
kubectl get secret superinference-env -n bench

# View secret content (base64 encoded)
kubectl get secret superinference-env -n bench -o yaml

# Test secret in pod
POD=$(kubectl get pods -n bench -l job-name=superinference-dabstep-benchmark -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $POD -c benchmark -n bench -- cat /secrets/.env
```

### Job Fails to Start

```bash
# Check job status
kubectl describe job superinference-dabstep-benchmark -n bench

# Check pod events
kubectl get events -n bench --sort-by=.lastTimestamp | tail -20

# Check pod status
kubectl get pods -n bench -l job-name=superinference-dabstep-benchmark
```

### Out of Memory

```bash
# Check resource usage
kubectl top pod -n bench

# Increase limits in benchmark-job.yaml:
resources:
  limits:
    memory: "32Gi"  # Increase
    cpu: "16"
```

### Results Not Appearing

```bash
# Check sidecar is running
kubectl get pods -n bench

# Exec into sidecar
POD=$(kubectl get pods -n bench -l job-name=superinference-dabstep-benchmark -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $POD -c sidecar -n bench -- sh

# Inside pod, check:
ls -lh /output
df -h /output
```

---

## ⏱️ Expected Timeline

**450 tasks**:
- Estimated time: 20-24 hours
- ~165 seconds per task average
- ~27.5 hours with overhead

**Set timeout accordingly**:
```yaml
spec:
  activeDeadlineSeconds: 259200  # 72 hours (safe margin)
```

---

## 📋 Deployment Checklist

**Before Deployment**:
- [ ] Docker image built and pushed to registry
- [ ] `.env` file with API keys ready
- [ ] Secret created: `kubectl create secret generic superinference-env --from-file=.env=.env -n bench`
- [ ] Namespace created: `kubectl create namespace bench`
- [ ] Image reference updated in `benchmark-job.yaml`

**Deploy**:
- [ ] `kubectl apply -f k8s/benchmark-job.yaml`
- [ ] Verify pod started: `kubectl get pods -n bench`
- [ ] Check logs: `kubectl logs -f <pod> -c benchmark -n bench`

**Monitor**:
- [ ] Watch progress: `kubectl logs -f <pod> -c benchmark -n bench | grep Task`
- [ ] Check resource usage: `kubectl top pod -n bench`
- [ ] Verify sidecar running: `kubectl get pods -n bench`

**Retrieve Results**:
- [ ] `kubectl cp bench/<pod>:/output ./results -c sidecar`
- [ ] Verify files downloaded
- [ ] Review summary.md
- [ ] Check validation plots

---

## 🎯 Command Summary

```bash
# 1. Create secret
kubectl create secret generic superinference-env --from-file=.env=.env -n bench

# 2. Deploy
kubectl apply -f k8s/benchmark-job.yaml

# 3. Monitor
kubectl logs -f $(kubectl get pods -n bench -l job-name=superinference-dabstep-benchmark -o jsonpath='{.items[0].metadata.name}') -c benchmark -n bench

# 4. Retrieve
kubectl cp bench/$(kubectl get pods -n bench -l job-name=superinference-dabstep-benchmark -o jsonpath='{.items[0].metadata.name}'):/output ./results -c sidecar
```

---

**Ready to deploy SuperInference benchmark on Kubernetes!** ✅


