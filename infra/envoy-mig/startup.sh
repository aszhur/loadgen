#!/bin/bash

set -euo pipefail

# Variables from metadata
XDS_SERVER_HOST="${xds_server_host}"
REGION="${region}"
ZONE="${zone}"
NODE_ID=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/id" -H "Metadata-Flavor: Google")
INSTANCE_NAME=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/name" -H "Metadata-Flavor: Google")

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [STARTUP] $1" | tee -a /var/log/envoy-startup.log
}

log "Starting Envoy setup for instance $INSTANCE_NAME (ID: $NODE_ID)"

# Update system
log "Updating system packages..."
apt-get update -y
apt-get install -y curl wget gnupg2 software-properties-common apt-transport-https ca-certificates

# Install Docker
log "Installing Docker..."
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io
systemctl enable docker
systemctl start docker

# Install Google Cloud Ops Agent for monitoring
log "Installing Google Cloud Ops Agent..."
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
bash add-google-cloud-ops-agent-repo.sh --also-install

# Create Envoy configuration directory
log "Setting up Envoy configuration..."
mkdir -p /etc/envoy
mkdir -p /var/log/envoy
mkdir -p /var/lib/envoy

# Generate Envoy configuration from template
cat > /etc/envoy/envoy.yaml <<EOF
node:
  id: "loadgen-envoy-$NODE_ID"
  cluster: "loadgen-envoy-cluster"
  locality:
    region: "$REGION"
    zone: "$ZONE"

admin:
  access_log:
  - name: envoy.access_loggers.stdout
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.access_loggers.stream.v3.StdoutAccessLog
  address:
    socket_address:
      address: 0.0.0.0
      port_value: 9901

dynamic_resources:
  eds_config:
    api_config_source:
      api_type: GRPC
      transport_api_version: V3
      grpc_services:
      - envoy_grpc:
          cluster_name: xds_cluster
      set_node_on_first_message_only: true
  rtds_config:
    api_config_source:
      api_type: GRPC
      transport_api_version: V3
      grpc_services:
      - envoy_grpc:
          cluster_name: xds_cluster
      set_node_on_first_message_only: true
    resource_api_version: V3

static_resources:
  listeners:
  - name: wavefront_listener
    address:
      socket_address:
        address: 0.0.0.0
        port_value: 8080
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: wavefront_ingress
          access_log:
          - name: envoy.access_loggers.stdout
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.access_loggers.stream.v3.StdoutAccessLog
          http_filters:
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
          route_config:
            name: wavefront_route
            virtual_hosts:
            - name: wavefront_service
              domains: ["*"]
              routes:
              - match:
                  prefix: "/api/v2/wfproxy/"
                route:
                  cluster: collector_cluster
                  timeout: 30s
                request_mirror_policies:
                - cluster: capture_cluster
                  runtime_fraction:
                    default_value:
                      numerator: 0
                      denominator: HUNDRED
                    runtime_key: capture.enabled
              - match:
                  prefix: "/health"
                direct_response:
                  status: 200
                  body:
                    inline_string: "OK"
              - match:
                  prefix: "/ready"  
                direct_response:
                  status: 200
                  body:
                    inline_string: "READY"

  clusters:
  - name: collector_cluster
    connect_timeout: 5s
    type: EDS
    eds_cluster_config:
      eds_config:
        api_config_source:
          api_type: GRPC
          transport_api_version: V3
          grpc_services:
          - envoy_grpc:
              cluster_name: xds_cluster
    lb_policy: LEAST_REQUEST
    health_checks:
    - timeout: 2s
      interval: 10s
      unhealthy_threshold: 3
      healthy_threshold: 2
      http_health_check:
        path: "/health"
    outlier_detection:
      consecutive_5xx: 3
      interval: 10s
      base_ejection_time: 30s
      max_ejection_percent: 50
  
  - name: capture_cluster
    connect_timeout: 200ms
    type: EDS
    eds_cluster_config:
      eds_config:
        api_config_source:
          api_type: GRPC
          transport_api_version: V3
          grpc_services:
          - envoy_grpc:
              cluster_name: xds_cluster
    lb_policy: ROUND_ROBIN
    circuit_breakers:
      thresholds:
      - priority: DEFAULT
        max_connections: 32
        max_pending_requests: 64
        max_requests: 128
        max_retries: 0
    outlier_detection:
      consecutive_5xx: 10
      interval: 30s
      base_ejection_time: 10s
      max_ejection_percent: 25

  - name: xds_cluster
    connect_timeout: 1s
    type: LOGICAL_DNS
    lb_policy: ROUND_ROBIN
    load_assignment:
      cluster_name: xds_cluster
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: "$XDS_SERVER_HOST"
                port_value: 18000

layered_runtime:
  layers:
  - name: rtds_layer
    rtds_layer:
      name: loadgen_runtime
      rtds_config:
        api_config_source:
          api_type: GRPC
          transport_api_version: V3
          grpc_services:
          - envoy_grpc:
              cluster_name: xds_cluster
EOF

# Create systemd service
log "Creating Envoy systemd service..."
cat > /etc/systemd/system/envoy.service <<EOF
[Unit]
Description=Envoy Proxy
After=docker.service
Requires=docker.service

[Service]
Type=simple
User=root
ExecStartPre=/usr/bin/docker pull envoyproxy/envoy:v1.27-latest
ExecStart=/usr/bin/docker run --rm --name envoy \\
    -p 8080:8080 -p 9901:9901 \\
    -v /etc/envoy:/etc/envoy:ro \\
    -v /var/log/envoy:/var/log/envoy \\
    envoyproxy/envoy:v1.27-latest \\
    /usr/local/bin/envoy -c /etc/envoy/envoy.yaml -l info --service-cluster loadgen-envoy-cluster --service-node loadgen-envoy-$NODE_ID
ExecStop=/usr/bin/docker stop envoy
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Configure log rotation
log "Setting up log rotation..."
cat > /etc/logrotate.d/envoy <<EOF
/var/log/envoy/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0644 root root
    postrotate
        /bin/kill -HUP \$(cat /var/run/rsyslogd.pid 2>/dev/null) 2>/dev/null || true
    endscript
}
EOF

# Set up monitoring configuration
log "Configuring monitoring..."
cat > /etc/google-cloud-ops-agent/config.yaml <<EOF
metrics:
  receivers:
    envoy:
      type: prometheus
      config:
        scrape_configs:
        - job_name: 'envoy-admin'
          scrape_interval: 15s
          static_configs:
          - targets: ['localhost:9901']
  service:
    pipelines:
      default_pipeline:
        receivers: [envoy]

logging:
  receivers:
    envoy_access:
      type: files
      config:
        include_paths: ['/var/log/envoy/*.log']
    envoy_startup:
      type: files
      config:
        include_paths: ['/var/log/envoy-startup.log']
  processors:
    envoy_parser:
      type: parse_json
      config:
        field: message
        target_field: json_payload
  service:
    pipelines:
      default_pipeline:
        receivers: [envoy_access, envoy_startup]
        processors: [envoy_parser]
EOF

# Enable and start services
log "Starting services..."
systemctl daemon-reload
systemctl enable envoy.service
systemctl restart google-cloud-ops-agent
systemctl start envoy.service

# Wait for services to be ready
log "Waiting for Envoy to be ready..."
for i in {1..30}; do
    if curl -f -s http://localhost:8080/ready > /dev/null 2>&1; then
        log "Envoy is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        log "ERROR: Envoy failed to become ready after 30 attempts"
        exit 1
    fi
    sleep 2
done

# Verify xDS connection
log "Verifying xDS connection..."
for i in {1..10}; do
    if curl -s http://localhost:9901/clusters | grep -q "collector_cluster"; then
        log "xDS connection established successfully!"
        break
    fi
    if [ $i -eq 10 ]; then
        log "WARNING: xDS connection not established after 10 attempts, but continuing..."
    fi
    sleep 5
done

log "Envoy setup completed successfully!"

# Set up health check endpoint monitoring
cat > /usr/local/bin/envoy-health-monitor.sh <<'EOF'
#!/bin/bash
while true; do
    if ! curl -f -s http://localhost:8080/ready > /dev/null 2>&1; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') [HEALTH] Envoy health check failed, restarting service..."
        systemctl restart envoy.service
        sleep 30
    fi
    sleep 10
done
EOF

chmod +x /usr/local/bin/envoy-health-monitor.sh

# Start health monitor in background
nohup /usr/local/bin/envoy-health-monitor.sh > /var/log/envoy-health-monitor.log 2>&1 &

log "Startup script completed successfully!"