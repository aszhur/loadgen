# Wavefront Load Generator - Deployment Guide

This guide covers the complete deployment and operation of the Wavefront Load Generator system.

## Prerequisites

### Required Resources
- **GCP Project** with billing enabled
- **VPC Network** with private Google access
- **GCS Buckets**: 
  - Capture storage (≥10TB, regional, lifecycle policies)
  - Recipe storage (≥1GB, standard)
  - Temp processing (≥1TB, regional, short lifecycle)
- **Service Accounts** with appropriate IAM roles
- **GKE Cluster** for generator workers
- **Dataproc** enabled for profiling jobs

### Required Quotas
- **Compute Engine**: 100+ vCPUs in target region
- **GKE**: 1 cluster, 50+ nodes
- **Dataproc**: Serverless enabled
- **Cloud Storage**: 10TB+ regional storage

## Phase 1: Infrastructure Setup

### 1.1 Deploy Base Infrastructure

```bash
# Clone the repository
git clone <repo-url>
cd loadgen

# Set up environment variables
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export ZONE="us-central1-a"

# Initialize Terraform
cd infra/
terraform init

# Deploy VPC and foundational resources
terraform plan -var="project_id=${PROJECT_ID}"
terraform apply
```

### 1.2 Deploy xDS Controller

```bash
cd infra/xds-controller

# Build and push container image
docker build -t gcr.io/${PROJECT_ID}/xds-controller:latest .
docker push gcr.io/${PROJECT_ID}/xds-controller:latest

# Deploy to Compute Engine
gcloud compute instances create xds-controller \
    --zone=${ZONE} \
    --machine-type=e2-standard-2 \
    --image-family=container-optimized-os \
    --image-project=cos-cloud \
    --container-image=gcr.io/${PROJECT_ID}/xds-controller:latest \
    --container-env=PROJECT_ID=${PROJECT_ID},ZONE=${ZONE}
```

### 1.3 Deploy Envoy MIG

```bash
cd infra/envoy-mig

# Build VM image with Packer
packer build \
    -var="project_id=${PROJECT_ID}" \
    -var="zone=${ZONE}" \
    envoy-image.pkr.hcl

# Deploy MIG with Terraform
terraform init
terraform apply \
    -var="project_id=${PROJECT_ID}" \
    -var="xds_server_host=<XDS_CONTROLLER_IP>"
```

### 1.4 Deploy Capture-Agent MIG

```bash
cd infra/capture-mig

# Build and deploy capture agent
go build -o capture-agent .
docker build -t gcr.io/${PROJECT_ID}/capture-agent:latest .
docker push gcr.io/${PROJECT_ID}/capture-agent:latest

# Deploy via Terraform
terraform apply \
    -var="project_id=${PROJECT_ID}" \
    -var="bucket_name=loadgen-capture-${PROJECT_ID}"
```

## Phase 2: Enable Traffic Capture

### 2.1 Verify Infrastructure Health

```bash
# Check xDS controller
curl http://<XDS_CONTROLLER_IP>:8080/health

# Check Envoy MIG health
gcloud compute health-checks describe loadgen-envoy-health-check

# Check capture agent status
curl http://<CAPTURE_AGENT_IP>:8080/health
```

### 2.2 Start Capture (Canary)

```bash
# Enable 5% capture rate
curl -X POST http://<XDS_CONTROLLER_IP>:8080/capture/enable?rate=5

# Monitor capture metrics
gcloud logging read "resource.type=gce_instance AND jsonPayload.component=capture-agent" \
    --limit=50 --format=json

# Check GCS upload progress
gsutil ls gs://loadgen-capture-${PROJECT_ID}/capture/dt=$(date +%Y-%m-%d)/
```

### 2.3 Scale to Full Capture

```bash
# Gradually increase capture rate
curl -X POST http://<XDS_CONTROLLER_IP>:8080/capture/enable?rate=25
sleep 300  # Wait 5 minutes

curl -X POST http://<XDS_CONTROLLER_IP>:8080/capture/enable?rate=50
sleep 300

curl -X POST http://<XDS_CONTROLLER_IP>:8080/capture/enable?rate=100
```

### 2.4 Monitor Capture Health

Key metrics to watch:
- **Primary path latency**: Should remain < 2ms p95 impact
- **Capture backlog**: Should stay < 10 seconds
- **Upload rate**: Target ~116 MB/s aggregate
- **Error rates**: Mirror errors < 1%

```bash
# Check key metrics
gcloud monitoring metrics list --filter="metric.type:loadgen"

# View dashboards
echo "Monitor at: https://console.cloud.google.com/monitoring/dashboards/custom/loadgen"
```

## Phase 3: Data Profiling

### 3.1 Run 24-Hour Capture

```bash
# Verify full capture is running
CAPTURE_DATE=$(date +%Y-%m-%d)
echo "Capturing for date: ${CAPTURE_DATE}"

# Monitor capture progress
watch "gsutil du -sh gs://loadgen-capture-${PROJECT_ID}/capture/dt=${CAPTURE_DATE}/"
```

### 3.2 Execute Profiling Pipeline

```bash
cd profiling/spark-jobs

# Submit profiling job
export INPUT_PATH="gs://loadgen-capture-${PROJECT_ID}/capture"
export OUTPUT_PATH="gs://loadgen-recipes-${PROJECT_ID}/recipes"
export SERVICE_ACCOUNT="loadgen-profiling@${PROJECT_ID}.iam.gserviceaccount.com"

./submit-profiling-job.sh ${CAPTURE_DATE}
```

### 3.3 Monitor Profiling Job

```bash
# Check job status
gcloud dataproc batches list --region=${REGION} --filter="labels.capture-date:${CAPTURE_DATE}"

# View job logs
BATCH_ID="wavefront-profiling-$(date +%Y%m%d)-*"
gcloud dataproc batches describe ${BATCH_ID} --region=${REGION}

# Check for completion marker
gsutil ls gs://loadgen-recipes-${PROJECT_ID}/recipes/v1/_PROFILE_OK
```

### 3.4 Validate Profiling Results

```bash
# Count generated recipes
RECIPE_COUNT=$(gsutil ls gs://loadgen-recipes-${PROJECT_ID}/recipes/v1/recipes/*.json.zst | wc -l)
echo "Generated ${RECIPE_COUNT} recipes"

# Review QA report
gsutil cp gs://loadgen-recipes-${PROJECT_ID}/recipes/v1/reports/profile_qa.html ./
open profile_qa.html

# Verify raw data cleanup
gsutil ls gs://loadgen-capture-${PROJECT_ID}/capture/dt=${CAPTURE_DATE}/_DELETE_OK
```

## Phase 4: Deploy Generator System

### 4.1 Setup GKE Cluster

```bash
cd infra/gke-generator

# Create GKE cluster
gcloud container clusters create loadgen-generator \
    --zone=${ZONE} \
    --num-nodes=10 \
    --machine-type=e2-standard-4 \
    --enable-autoscaling \
    --min-nodes=5 \
    --max-nodes=50

# Get credentials
gcloud container clusters get-credentials loadgen-generator --zone=${ZONE}
```

### 4.2 Deploy Control Plane

```bash
cd generator/control-plane

# Build and push image
docker build -t gcr.io/${PROJECT_ID}/loadgen-control-plane:latest .
docker push gcr.io/${PROJECT_ID}/loadgen-control-plane:latest

# Deploy to GKE
kubectl apply -f k8s/
kubectl wait --for=condition=available --timeout=300s deployment/loadgen-control-plane
```

### 4.3 Deploy Worker Pods

```bash
cd generator/workers

# Build worker image
docker build -t gcr.io/${PROJECT_ID}/loadgen-worker:latest .
docker push gcr.io/${PROJECT_ID}/loadgen-worker:latest

# Deploy worker pool
kubectl apply -f k8s/
kubectl scale deployment loadgen-worker --replicas=10
```

### 4.4 Deploy Monitoring

```bash
cd validation/online-metrics

# Deploy divergence monitor
docker build -t gcr.io/${PROJECT_ID}/divergence-monitor:latest .
docker push gcr.io/${PROJECT_ID}/divergence-monitor:latest

kubectl apply -f k8s/
```

## Phase 5: Generate Load

### 5.1 Create Load Scenario

```bash
# Define load scenario
cat > scenario.json << EOF
{
  "name": "production-test",
  "namespace": "default", 
  "spec": {
    "families": ["*"],
    "multiplier": 1.0,
    "workerPods": 10,
    "endpoints": ["http://collectors:8080/api/v2/wfproxy/report"],
    "authentication": {
      "type": "bearer_token",
      "token": "your-token"
    }
  }
}
EOF

# Submit scenario
CONTROL_PLANE_IP=$(kubectl get svc loadgen-control-plane -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl -X POST http://${CONTROL_PLANE_IP}:8080/api/v1/scenarios \
    -H "Content-Type: application/json" \
    -d @scenario.json
```

### 5.2 Monitor Generation

```bash
# Check scenario status
curl http://${CONTROL_PLANE_IP}:8080/api/v1/scenarios/production-test

# View worker status
kubectl get pods -l app=loadgen-worker
kubectl logs -l app=loadgen-worker --tail=100

# Monitor metrics
kubectl port-forward svc/loadgen-control-plane 9090:9090 &
open http://localhost:9090/metrics
```

### 5.3 Scale Load

```bash
# Update multiplier to 2x
cat > scale-update.json << EOF
{
  "spec": {
    "multiplier": 2.0,
    "workerPods": 20
  }
}
EOF

curl -X PUT http://${CONTROL_PLANE_IP}:8080/api/v1/scenarios/production-test \
    -H "Content-Type: application/json" \
    -d @scale-update.json
```

## Phase 6: Validation & Monitoring

### 6.1 Setup Dashboards

```bash
# Import Grafana dashboards
kubectl apply -f monitoring/grafana-dashboards/

# Setup alerts
kubectl apply -f monitoring/alerting/
```

### 6.2 Monitor Divergence

```bash
# Check divergence metrics
MONITOR_IP=$(kubectl get svc divergence-monitor -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl http://${MONITOR_IP}:9100/metrics | grep divergence

# View family status
curl http://${MONITOR_IP}:9101/families | jq '.[] | select(.status != "green")'
```

## Operations & Troubleshooting

### Common Issues

#### 1. High Capture Backlog

**Symptoms**: `capture_backlog_seconds > 60`

**Resolution**:
```bash
# Check capture agent health
kubectl logs -l app=capture-agent --tail=100

# Scale up capture MIG
gcloud compute instance-groups managed resize loadgen-capture-mig \
    --size=20 --zone=${ZONE}

# Reduce capture rate temporarily
curl -X POST http://<XDS_CONTROLLER_IP>:8080/capture/enable?rate=75
```

#### 2. Primary Path Latency Impact

**Symptoms**: Collector p95 latency increased by >2ms

**Resolution**:
```bash
# Disable capture immediately
curl -X POST http://<XDS_CONTROLLER_IP>:8080/capture/disable

# Check Envoy configuration
curl http://<ENVOY_INSTANCE_IP>:9901/config_dump | jq '.configs[].dynamic_active_clusters'

# Investigate Envoy metrics
curl http://<ENVOY_INSTANCE_IP>:9901/stats | grep mirror
```

#### 3. Profiling Job Failures

**Symptoms**: Profiling job fails or produces poor quality recipes

**Resolution**:
```bash
# Check job logs
gcloud dataproc batches describe ${BATCH_ID} --region=${REGION} --format="value(runtimeInfo.diagnosticOutputUri)"

# Validate input data
gsutil ls -l gs://loadgen-capture-${PROJECT_ID}/capture/dt=${CAPTURE_DATE}/**/*.wf.zst | head -10

# Resubmit with increased resources
export MAX_EXECUTORS=100
./submit-profiling-job.sh ${CAPTURE_DATE}
```

#### 4. Worker Pod Failures

**Symptoms**: Workers not emitting traffic or high error rates

**Resolution**:
```bash
# Check worker logs
kubectl logs -l app=loadgen-worker --tail=100

# Verify recipe access
kubectl exec -it $(kubectl get pods -l app=loadgen-worker -o name | head -1) -- \
    curl http://loadgen-control-plane:8080/api/v1/recipes

# Check authentication
kubectl describe secret loadgen-auth-secret
```

#### 5. High Divergence Scores

**Symptoms**: Multiple families showing red status

**Resolution**:
```bash
# Check specific family divergence
curl http://${MONITOR_IP}:9101/families | jq '.[] | select(.status == "red")'

# Verify recipe quality
gsutil cp gs://loadgen-recipes-${PROJECT_ID}/recipes/v1/reports/qa_summary.json ./
cat qa_summary.json

# Adjust generation parameters
curl -X PUT http://${CONTROL_PLANE_IP}:8080/api/v1/scenarios/production-test \
    -d '{"spec": {"burstFactor": 1.0, "schemaDrift": 0.0}}'
```

### Maintenance Procedures

#### Daily Operations
- Monitor dashboard alerts
- Check capture backlog metrics
- Verify divergence scores
- Review error logs

#### Weekly Operations
- Rotate capture data (automated via lifecycle)
- Update recipe cache
- Review capacity utilization
- Security patch updates

#### Monthly Operations
- Re-profile with fresh capture
- Update synthetic patterns
- Capacity planning review
- Performance baseline updates

### Emergency Procedures

#### Stop All Traffic Generation
```bash
# Emergency stop
kubectl scale deployment loadgen-worker --replicas=0
curl -X DELETE http://${CONTROL_PLANE_IP}:8080/api/v1/scenarios/production-test
```

#### Disable Traffic Capture
```bash
# Immediate capture stop
curl -X POST http://<XDS_CONTROLLER_IP>:8080/capture/disable

# Verify traffic flows normally
curl http://<COLLECTOR_IP>:8080/health
```

#### Rollback Deployment
```bash
# Rollback to previous version
kubectl rollout undo deployment/loadgen-control-plane
kubectl rollout undo deployment/loadgen-worker
```

## Success Criteria

✅ **Capture Phase**
- 24-hour capture completes with ≥99.9% coverage
- Primary path latency impact <2ms p95
- No collector service degradation

✅ **Profiling Phase**  
- Recipes generated for >99% of metric families
- QA reports show good coverage and quality scores
- Raw capture data successfully deleted

✅ **Generation Phase**
- Synthetic load within ±5% of target multiplier
- Divergence scores within thresholds (JS<0.05, KS<0.05)
- Worker pods stable with <1% error rate

✅ **Operational Phase**
- All dashboards and alerts functional
- Runbooks validated through drills
- Performance baselines established

## Next Steps

After successful deployment:
1. **Automation**: Set up automated re-profiling cycles
2. **Scaling**: Test higher multipliers and burst scenarios  
3. **Integration**: Connect to existing load test pipelines
4. **Optimization**: Tune for cost and performance
5. **Documentation**: Create team-specific runbooks