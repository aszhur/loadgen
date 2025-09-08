# Wavefront Load Generator - Troubleshooting Guide

This guide provides detailed troubleshooting procedures for the Wavefront Load Generator system.

## Quick Diagnostics

### System Health Check

Run this script to quickly assess overall system health:

```bash
#!/bin/bash
# loadgen-health-check.sh

set -euo pipefail

echo "=== Wavefront Load Generator Health Check ==="
echo "Timestamp: $(date)"
echo ""

# Check xDS controller
echo "1. xDS Controller Status:"
XDS_IP=$(gcloud compute instances describe xds-controller --zone=${ZONE} --format='value(networkInterfaces[0].accessConfigs[0].natIP)')
if curl -s -f http://${XDS_IP}:8080/health > /dev/null; then
    echo "   ✅ xDS Controller healthy"
    CAPTURE_RATE=$(curl -s http://${XDS_IP}:8080/capture/rate)
    echo "   📊 Capture rate: ${CAPTURE_RATE}%"
else
    echo "   ❌ xDS Controller unhealthy"
fi

# Check Envoy MIG
echo "2. Envoy MIG Status:"
ENVOY_STATUS=$(gcloud compute instance-groups managed describe loadgen-envoy-mig --zone=${ZONE} --format='value(status.isStable)')
if [ "$ENVOY_STATUS" = "True" ]; then
    echo "   ✅ Envoy MIG stable"
else
    echo "   ⚠️  Envoy MIG not stable"
fi

# Check Capture MIG  
echo "3. Capture Agent Status:"
CAPTURE_INSTANCES=$(gcloud compute instances list --filter="labels.component=capture-agent" --format='value(name)')
HEALTHY_CAPTURE=0
for instance in $CAPTURE_INSTANCES; do
    INSTANCE_IP=$(gcloud compute instances describe $instance --zone=${ZONE} --format='value(networkInterfaces[0].networkIP)')
    if curl -s -f http://${INSTANCE_IP}:8080/health > /dev/null; then
        ((HEALTHY_CAPTURE++))
    fi
done
echo "   📊 Healthy capture agents: ${HEALTHY_CAPTURE}"

# Check GKE cluster
echo "4. GKE Cluster Status:"
if kubectl cluster-info > /dev/null 2>&1; then
    echo "   ✅ GKE cluster accessible"
    WORKER_PODS=$(kubectl get pods -l app=loadgen-worker --field-selector=status.phase=Running -o name | wc -l)
    echo "   📊 Running worker pods: ${WORKER_PODS}"
else
    echo "   ❌ Cannot access GKE cluster"
fi

# Check recent profiling
echo "5. Recent Profiling:"
RECENT_RECIPES=$(gsutil ls gs://loadgen-recipes-${PROJECT_ID}/recipes/v1/recipes/*.json.zst 2>/dev/null | wc -l)
echo "   📊 Available recipes: ${RECENT_RECIPES}"

echo ""
echo "=== Health Check Complete ==="
```

## Component-Specific Troubleshooting

### xDS Controller Issues

#### Problem: xDS Controller not responding

**Symptoms**:
- Envoy instances cannot connect to xDS server
- Configuration updates not propagating
- HTTP health check failures

**Diagnosis**:
```bash
# Check service status
gcloud compute instances describe xds-controller --zone=${ZONE}

# Check logs
gcloud logging read "resource.type=gce_instance AND resource.labels.instance_name=xds-controller" \
    --limit=50 --format=json

# Test connectivity
XDS_IP=$(gcloud compute instances describe xds-controller --zone=${ZONE} --format='value(networkInterfaces[0].accessConfigs[0].natIP)')
curl -v http://${XDS_IP}:8080/health
telnet ${XDS_IP} 18000
```

**Solutions**:
```bash
# Restart the service
gcloud compute instances reset xds-controller --zone=${ZONE}

# Check firewall rules
gcloud compute firewall-rules list --filter="name:xds"

# Verify service account permissions
gcloud projects get-iam-policy ${PROJECT_ID} \
    --flatten="bindings[].members" \
    --format="table(bindings.role)" \
    --filter="bindings.members:xds-controller-sa@${PROJECT_ID}.iam.gserviceaccount.com"
```

#### Problem: Capture rate not updating

**Symptoms**:
- RTDS key changes not reflected in Envoy
- Capture rate stuck at previous value
- Mirror traffic not responding to controls

**Diagnosis**:
```bash
# Check RTDS configuration
curl http://${XDS_IP}:8080/status

# Verify Envoy RTDS connection
ENVOY_IP=$(gcloud compute instances list --filter="labels.component=envoy" --limit=1 --format='value(networkInterfaces[0].networkIP)')
curl http://${ENVOY_IP}:9901/runtime | grep capture.enabled
```

**Solutions**:
```bash
# Force RTDS update
curl -X POST http://${XDS_IP}:8080/capture/rate -d '{"rate": 50}'

# Restart Envoy instances (rolling restart)
gcloud compute instance-groups managed rolling-action restart loadgen-envoy-mig \
    --zone=${ZONE} \
    --max-surge=2 \
    --max-unavailable=0
```

### Envoy MIG Issues

#### Problem: High primary path latency

**Symptoms**:
- Collector p95 latency increased >2ms
- Mirror processing blocking primary requests
- Timeout errors from clients

**Diagnosis**:
```bash
# Check Envoy statistics
curl http://${ENVOY_IP}:9901/stats | grep -E "(mirror|upstream|downstream)"

# Check cluster health
curl http://${ENVOY_IP}:9901/clusters

# Monitor request flow
curl http://${ENVOY_IP}:9901/stats/prometheus | grep envoy_http_requests
```

**Solutions**:
```bash
# Immediate: Disable capture
curl -X POST http://${XDS_IP}:8080/capture/disable

# Tune mirror cluster timeouts
# Edit envoy.yaml to reduce mirror timeouts to 50ms

# Scale up Envoy instances
gcloud compute instance-groups managed resize loadgen-envoy-mig \
    --size=15 --zone=${ZONE}

# Check if collector cluster is healthy
curl http://${ENVOY_IP}:9901/clusters | grep collector_cluster
```

#### Problem: Mirror requests failing

**Symptoms**:
- High mirror error rates in logs
- Capture agents not receiving traffic
- Mirror circuit breakers tripping

**Diagnosis**:
```bash
# Check mirror-specific stats
curl http://${ENVOY_IP}:9901/stats | grep mirror

# Check capture agent health
curl http://<CAPTURE_AGENT_IP>:8080/health

# Verify network connectivity
kubectl run test-pod --rm -i --tty --image=busybox -- /bin/sh
# Inside pod: wget -qO- http://<CAPTURE_AGENT_IP>:8080/health
```

**Solutions**:
```bash
# Reset circuit breakers
curl -X POST http://${ENVOY_IP}:9901/reset_counters

# Scale capture agents
gcloud compute instance-groups managed resize loadgen-capture-mig \
    --size=10 --zone=${ZONE}

# Adjust mirror policy in envoy.yaml:
# - Increase timeout to 200ms
# - Reduce connection pool limits
```

### Capture Agent Issues

#### Problem: High backlog/memory usage

**Symptoms**:
- `capture_backlog_seconds > 60`
- High memory usage on capture instances
- GCS upload failures or slowness

**Diagnosis**:
```bash
# Check capture metrics
CAPTURE_IP=$(gcloud compute instances list --filter="labels.component=capture-agent" --limit=1 --format='value(networkInterfaces[0].networkIP)')
curl http://${CAPTURE_IP}:9090/metrics | grep capture_

# Check system resources
gcloud compute ssh capture-agent-instance --zone=${ZONE} --command="free -h && df -h && iostat -x 1 1"

# Check GCS upload status
gsutil ls -l gs://loadgen-capture-${PROJECT_ID}/capture/dt=$(date +%Y-%m-%d)/ | tail -20
```

**Solutions**:
```bash
# Scale up capture agents
gcloud compute instance-groups managed resize loadgen-capture-mig \
    --size=20 --zone=${ZONE}

# Increase buffer flush frequency
# Edit capture agent config: --flush-interval=2s

# Check GCS bucket configuration
gsutil lifecycle get gs://loadgen-capture-${PROJECT_ID}

# Monitor network bandwidth
gcloud compute instances describe capture-agent-instance --zone=${ZONE} --format='value(networkInterfaces[0].networkTier)'
```

#### Problem: GCS upload failures

**Symptoms**:
- `capture_upload_errors_total` increasing
- Incomplete manifest files
- Missing data in GCS bucket

**Diagnosis**:
```bash
# Check upload error types
curl http://${CAPTURE_IP}:9090/metrics | grep upload_errors

# Check service account permissions
gcloud iam service-accounts get-iam-policy capture-agent-sa@${PROJECT_ID}.iam.gserviceaccount.com

# Test GCS connectivity manually
gcloud compute ssh capture-agent-instance --zone=${ZONE} --command="gsutil ls gs://loadgen-capture-${PROJECT_ID}/"

# Check bucket location and network egress
gsutil ls -L -b gs://loadgen-capture-${PROJECT_ID}
```

**Solutions**:
```bash
# Verify service account has storage.objectCreator role
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:capture-agent-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/storage.objectCreator"

# Enable resumable uploads (check if configured in code)
# Increase chunk size for better performance

# Move bucket to same region as capture agents
gsutil mb -c regional -l ${REGION} gs://loadgen-capture-${PROJECT_ID}-new
```

### Profiling Pipeline Issues

#### Problem: Profiling job fails

**Symptoms**:
- Dataproc batch job fails with errors
- Poor recipe quality (coverage < 90%)
- Missing families in output

**Diagnosis**:
```bash
# Get job details
BATCH_ID="wavefront-profiling-$(date +%Y%m%d)*"
gcloud dataproc batches describe ${BATCH_ID} --region=${REGION}

# Check driver logs
gcloud dataproc batches describe ${BATCH_ID} --region=${REGION} --format='value(runtimeInfo.diagnosticOutputUri)'

# Verify input data integrity
gsutil ls -l gs://loadgen-capture-${PROJECT_ID}/capture/dt=$(date -d yesterday +%Y-%m-%d)/**/*.wf.zst | head -10

# Check sample data format
gsutil cp gs://loadgen-capture-${PROJECT_ID}/capture/dt=$(date -d yesterday +%Y-%m-%d)/mig=tier-e/*/part-000.wf.zst ./
zstd -d part-000.wf.zst
head -100 part-000.wf
```

**Solutions**:
```bash
# Increase job resources
export MAX_EXECUTORS=100
export EXECUTOR_MEMORY="30g"
./submit-profiling-job.sh $(date -d yesterday +%Y-%m-%d)

# Enable Spark adaptive query execution
# (Already configured in profiler code)

# Split processing by smaller date ranges if needed
for hour in {00..23}; do
    export INPUT_PATH="gs://loadgen-capture-${PROJECT_ID}/capture/dt=$(date -d yesterday +%Y-%m-%d)/hour=${hour}"
    ./submit-profiling-job.sh $(date -d yesterday +%Y-%m-%d)-${hour}
done

# Debug parser issues
# Add more verbose logging to wavefront_profiler.py
```

#### Problem: Poor recipe quality

**Symptoms**:
- Low coverage percentages in QA report
- High divergence scores during generation
- Missing statistical distributions

**Diagnosis**:
```bash
# Download and review QA report
gsutil cp gs://loadgen-recipes-${PROJECT_ID}/recipes/v1/reports/profile_qa.html ./
open profile_qa.html

# Check recipe content
gsutil cp gs://loadgen-recipes-${PROJECT_ID}/recipes/v1/recipes/sample-family.json.zst ./
zstd -d sample-family.json.zst
cat sample-family.json | jq .

# Analyze drop reasons
gsutil cp gs://loadgen-recipes-${PROJECT_ID}/recipes/v1/reports/qa_summary.json ./
cat qa_summary.json | jq '.data_quality.issues'
```

**Solutions**:
```bash
# Increase minimum samples per family
# Edit profiler configuration: min_samples_threshold = 1000

# Improve data preprocessing
# Review parser logic for edge cases in wavefront_profiler.py

# Extend capture window
# Run capture for 48 hours instead of 24

# Filter out noisy/synthetic test data
# Add filters to exclude known test patterns
```

### Generator System Issues

#### Problem: Control plane not responding

**Symptoms**:
- API endpoints timeout or return errors
- Workers cannot get assignments
- Recipe loading failures

**Diagnosis**:
```bash
# Check control plane pod status
kubectl get pods -l app=loadgen-control-plane
kubectl logs -l app=loadgen-control-plane --tail=100

# Test API endpoints
CONTROL_PLANE_IP=$(kubectl get svc loadgen-control-plane -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl -v http://${CONTROL_PLANE_IP}:8080/health
curl -v http://${CONTROL_PLANE_IP}:8080/api/v1/recipes

# Check service mesh connectivity
kubectl exec -it $(kubectl get pods -l app=loadgen-worker -o name | head -1) -- \
    curl http://loadgen-control-plane:8080/health
```

**Solutions**:
```bash
# Restart control plane
kubectl rollout restart deployment/loadgen-control-plane
kubectl wait --for=condition=available --timeout=300s deployment/loadgen-control-plane

# Check resource limits
kubectl describe pod -l app=loadgen-control-plane

# Verify GCS access for recipes
kubectl logs -l app=loadgen-control-plane | grep -i "gcs\|recipe"

# Scale control plane if needed
kubectl scale deployment loadgen-control-plane --replicas=3
```

#### Problem: Workers not generating traffic

**Symptoms**:
- Zero `loadgen_lines_emitted_total` metrics
- Workers reporting no assignments
- HTTP errors to collector endpoints

**Diagnosis**:
```bash
# Check worker pod status
kubectl get pods -l app=loadgen-worker
kubectl describe pod -l app=loadgen-worker

# Check worker logs
kubectl logs -l app=loadgen-worker --tail=100

# Test endpoint connectivity
kubectl exec -it $(kubectl get pods -l app=loadgen-worker -o name | head -1) -- \
    curl -v http://collectors:8080/api/v2/wfproxy/report

# Check authentication
kubectl describe secret loadgen-auth-secret
```

**Solutions**:
```bash
# Update worker configuration
kubectl set env deployment/loadgen-worker CONTROL_PLANE_URL=http://loadgen-control-plane:8080

# Fix authentication issues
kubectl create secret generic loadgen-auth-secret \
    --from-literal=token="your-auth-token"

# Check collector endpoint configuration
kubectl get svc collectors
kubectl get endpoints collectors

# Restart workers to get fresh assignments
kubectl rollout restart deployment/loadgen-worker
```

#### Problem: High divergence scores

**Symptoms**:
- Multiple families showing red status
- `loadgen_divergence_*` metrics above thresholds
- Generated traffic not matching reference patterns

**Diagnosis**:
```bash
# Check divergence monitor status
MONITOR_IP=$(kubectl get svc divergence-monitor -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl http://${MONITOR_IP}:9101/families | jq '.[] | select(.status == "red")'

# Review specific family metrics
curl http://${MONITOR_IP}:9100/metrics | grep divergence

# Check recipe content for problematic families
FAMILY_ID=$(curl -s http://${MONITOR_IP}:9101/families | jq -r '.[] | select(.status == "red") | .family_id' | head -1)
curl http://${CONTROL_PLANE_IP}:8080/api/v1/recipes/${FAMILY_ID} | jq .
```

**Solutions**:
```bash
# Reduce generation parameters
kubectl set env deployment/loadgen-worker \
    BURST_FACTOR=1.0 \
    SCHEMA_DRIFT=0.0

# Re-profile with more recent data
./submit-profiling-job.sh $(date +%Y-%m-%d)

# Adjust divergence thresholds if appropriate
kubectl set env deployment/divergence-monitor \
    JS_THRESHOLD=0.1 \
    WASSERSTEIN_THRESHOLD=0.2

# Restart divergence computation
curl -X POST http://${MONITOR_IP}:9101/compute
```

## Performance Optimization

### Scaling Guidelines

**When to Scale Up**:
- Capture backlog > 30 seconds consistently
- Worker CPU utilization > 80%
- Target load not achieved within ±10%
- Mirror error rate > 0.5%

**Scaling Commands**:
```bash
# Scale Envoy MIG
gcloud compute instance-groups managed resize loadgen-envoy-mig \
    --size=$((CURRENT_SIZE + 5)) --zone=${ZONE}

# Scale Capture MIG  
gcloud compute instance-groups managed resize loadgen-capture-mig \
    --size=$((CURRENT_SIZE * 2)) --zone=${ZONE}

# Scale Worker Pods
kubectl scale deployment loadgen-worker --replicas=$((CURRENT_REPLICAS + 10))

# Scale GKE nodes
gcloud container clusters resize loadgen-generator \
    --num-nodes=$((CURRENT_NODES + 5)) --zone=${ZONE}
```

### Memory Optimization

```bash
# Monitor memory usage across components
kubectl top pods
gcloud compute instances list --format="table(name,status,machineType.scope(machineTypes))"

# Optimize Java heap for Spark jobs
export EXECUTOR_MEMORY="20g"
export DRIVER_MEMORY="8g"

# Tune Go garbage collection
kubectl set env deployment/loadgen-worker GOGC=50

# Configure kernel memory settings on capture agents
gcloud compute ssh capture-agent-instance --zone=${ZONE} --command="echo 'vm.swappiness=10' | sudo tee -a /etc/sysctl.conf"
```

## Monitoring and Alerting

### Key Metrics to Watch

```bash
# Capture health metrics
capture_backlog_seconds < 10
capture_upload_rate_bps > 100MB
capture_upload_errors_total growth rate < 1%

# Generation fidelity metrics  
loadgen_divergence_jensen_shannon < 0.05
loadgen_divergence_wasserstein < 0.1
loadgen_divergence_kolmogorov_smirnov < 0.05

# System performance metrics
envoy_http_requests_per_second growth rate > 0
envoy_upstream_rq_pending < 100
loadgen_lines_emitted_total growth rate matches target
```

### Setting Up Alerts

```yaml
# alertmanager.yml rules
groups:
- name: loadgen.rules
  rules:
  - alert: HighCaptureBacklog
    expr: capture_backlog_seconds > 60
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "Capture backlog is high"
      
  - alert: HighDivergence  
    expr: loadgen_divergence_jensen_shannon > 0.05
    for: 15m
    labels:
      severity: warning
    annotations:
      summary: "Traffic divergence detected"
```

## Recovery Procedures

### Complete System Recovery

```bash
#!/bin/bash
# complete-recovery.sh - Use only in emergency

echo "=== Emergency System Recovery ==="

# 1. Stop all traffic generation
kubectl scale deployment loadgen-worker --replicas=0
curl -X POST http://${XDS_IP}:8080/capture/disable

# 2. Verify primary traffic flows normally
curl http://collectors:8080/health

# 3. Reset MIGs to stable state
gcloud compute instance-groups managed set-target-pools loadgen-envoy-mig --target-pools='' --zone=${ZONE}
gcloud compute instance-groups managed rolling-action restart loadgen-envoy-mig --zone=${ZONE} --max-unavailable=1

# 4. Clear any problematic state
kubectl delete pods -l app=loadgen-control-plane
kubectl delete pods -l app=loadgen-worker

# 5. Wait for services to stabilize
sleep 300

# 6. Gradually restore functionality
echo "System stopped. Manual intervention required for restart."
```

This troubleshooting guide covers the most common issues and their solutions. For complex problems not covered here, refer to the component logs and contact the platform team with specific error messages and symptoms.