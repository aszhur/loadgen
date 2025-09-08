terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP Zone"
  type        = string
  default     = "us-central1-a"
}

variable "network_name" {
  description = "VPC network name"
  type        = string
  default     = "loadgen-network"
}

variable "subnet_name" {
  description = "Subnet name"
  type        = string
  default     = "loadgen-subnet"
}

variable "xds_server_host" {
  description = "xDS controller hostname/IP"
  type        = string
}

variable "min_replicas" {
  description = "Minimum number of Envoy instances"
  type        = number
  default     = 2
}

variable "max_replicas" {
  description = "Maximum number of Envoy instances"
  type        = number
  default     = 10
}

variable "machine_type" {
  description = "Machine type for Envoy instances"
  type        = string
  default     = "e2-standard-2"
}

# Service account for Envoy MIG
resource "google_service_account" "envoy_sa" {
  account_id   = "loadgen-envoy-sa"
  display_name = "Loadgen Envoy Service Account"
  project      = var.project_id
}

resource "google_project_iam_member" "envoy_monitoring_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.envoy_sa.email}"
}

resource "google_project_iam_member" "envoy_logging_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.envoy_sa.email}"
}

# Startup script for Envoy instances
locals {
  startup_script = base64encode(templatefile("${path.module}/startup.sh", {
    xds_server_host = var.xds_server_host
    region          = var.region
    zone            = var.zone
  }))
}

# Instance template
resource "google_compute_instance_template" "envoy_template" {
  name         = "loadgen-envoy-template-${formatdate("YYYYMMDD-hhmmss", timestamp())}"
  description  = "Template for Loadgen Envoy instances"
  machine_type = var.machine_type

  tags = ["loadgen-envoy", "http-server"]

  disk {
    source_image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2004-lts"
    auto_delete  = true
    boot         = true
    disk_size_gb = 20
    disk_type    = "pd-standard"
  }

  network_interface {
    network    = var.network_name
    subnetwork = var.subnet_name
    
    access_config {
      // Ephemeral external IP
    }
  }

  service_account {
    email  = google_service_account.envoy_sa.email
    scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
      "https://www.googleapis.com/auth/monitoring.write",
      "https://www.googleapis.com/auth/logging.write"
    ]
  }

  metadata = {
    startup-script = base64decode(local.startup_script)
    shutdown-script = "#!/bin/bash\nsudo systemctl stop envoy\n"
    enable-oslogin = "TRUE"
    
    # Template variables for envoy.yaml
    XDS_SERVER_HOST = var.xds_server_host
    REGION         = var.region
    ZONE           = var.zone
  }

  lifecycle {
    create_before_destroy = true
  }
}

# Managed Instance Group
resource "google_compute_instance_group_manager" "envoy_mig" {
  name               = "loadgen-envoy-mig"
  base_instance_name = "loadgen-envoy"
  zone               = var.zone
  target_size        = var.min_replicas

  version {
    instance_template = google_compute_instance_template.envoy_template.id
  }

  named_port {
    name = "http"
    port = 8080
  }

  named_port {
    name = "admin"
    port = 9901
  }

  auto_healing_policies {
    health_check      = google_compute_health_check.envoy_health_check.id
    initial_delay_sec = 60
  }

  update_policy {
    type                    = "PROACTIVE"  
    minimal_action          = "REPLACE"
    max_surge_fixed         = 2
    max_unavailable_fixed   = 0
    replacement_method      = "RECREATE"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# Health check for auto-healing
resource "google_compute_health_check" "envoy_health_check" {
  name                = "loadgen-envoy-health-check"
  description         = "Health check for Envoy instances"
  timeout_sec         = 5
  check_interval_sec  = 10
  healthy_threshold   = 2
  unhealthy_threshold = 3

  http_health_check {
    port         = "8080"
    request_path = "/health"
  }
}

# Autoscaler
resource "google_compute_autoscaler" "envoy_autoscaler" {
  name   = "loadgen-envoy-autoscaler"
  zone   = var.zone
  target = google_compute_instance_group_manager.envoy_mig.id

  autoscaling_policy {
    min_replicas    = var.min_replicas
    max_replicas    = var.max_replicas
    cooldown_period = 60

    cpu_utilization {
      target = 0.7
    }

    # Scale based on HTTP requests per second
    load_balancing_utilization {
      target = 0.8
    }

    # Custom metric scaling for backlog (to be implemented)
    metric {
      name   = "compute.googleapis.com/instance/up"
      target = 1
      type   = "GAUGE"
    }
  }
}

# Firewall rules
resource "google_compute_firewall" "envoy_http" {
  name    = "loadgen-envoy-http"
  network = var.network_name

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["loadgen-envoy"]
}

resource "google_compute_firewall" "envoy_admin" {
  name    = "loadgen-envoy-admin"
  network = var.network_name

  allow {
    protocol = "tcp"
    ports    = ["9901"]
  }

  source_ranges = ["10.0.0.0/8"]  # Internal only
  target_tags   = ["loadgen-envoy"]
}

resource "google_compute_firewall" "envoy_health_check" {
  name    = "loadgen-envoy-health-check"
  network = var.network_name

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  source_ranges = [
    "35.191.0.0/16",   # GCP health checks
    "130.211.0.0/22"   # GCP health checks
  ]
  target_tags = ["loadgen-envoy"]
}

# Backend service for Load Balancer
resource "google_compute_backend_service" "envoy_backend" {
  name                            = "loadgen-envoy-backend"
  description                     = "Backend service for Envoy MIG"
  port_name                       = "http"
  protocol                        = "HTTP"
  timeout_sec                     = 30
  connection_draining_timeout_sec = 10
  load_balancing_scheme          = "EXTERNAL"

  backend {
    group           = google_compute_instance_group_manager.envoy_mig.instance_group
    balancing_mode  = "UTILIZATION"
    capacity_scaler = 1.0
    max_utilization = 0.8
  }

  health_checks = [google_compute_health_check.envoy_lb_health_check.id]

  log_config {
    enable      = true
    sample_rate = 0.1
  }
}

# Separate health check for load balancer
resource "google_compute_health_check" "envoy_lb_health_check" {
  name                = "loadgen-envoy-lb-health-check"
  description         = "Health check for Envoy load balancer"
  timeout_sec         = 3
  check_interval_sec  = 5
  healthy_threshold   = 2
  unhealthy_threshold = 2

  http_health_check {
    port         = "8080"
    request_path = "/ready"
  }
}

# URL map
resource "google_compute_url_map" "envoy_url_map" {
  name            = "loadgen-envoy-url-map"
  description     = "URL map for Envoy load balancer"
  default_service = google_compute_backend_service.envoy_backend.id

  host_rule {
    hosts        = ["*"]
    path_matcher = "allpaths"
  }

  path_matcher {
    name            = "allpaths"
    default_service = google_compute_backend_service.envoy_backend.id

    path_rule {
      paths   = ["/api/v2/wfproxy/*"]
      service = google_compute_backend_service.envoy_backend.id
    }
  }
}

# HTTPS proxy
resource "google_compute_target_https_proxy" "envoy_https_proxy" {
  name             = "loadgen-envoy-https-proxy"
  url_map          = google_compute_url_map.envoy_url_map.id
  ssl_certificates = [google_compute_ssl_certificate.envoy_ssl_cert.id]
}

# SSL certificate (self-managed, replace with managed certificate in production)
resource "google_compute_ssl_certificate" "envoy_ssl_cert" {
  name_prefix = "loadgen-envoy-ssl-"
  description = "SSL certificate for Envoy load balancer"

  certificate = file("${path.module}/certs/certificate.pem")
  private_key = file("${path.module}/certs/private-key.pem")

  lifecycle {
    create_before_destroy = true
  }
}

# Global forwarding rule
resource "google_compute_global_forwarding_rule" "envoy_https_forwarding_rule" {
  name       = "loadgen-envoy-https-forwarding-rule"
  target     = google_compute_target_https_proxy.envoy_https_proxy.id
  port_range = "443"
}

# HTTP to HTTPS redirect
resource "google_compute_url_map" "envoy_http_redirect" {
  name = "loadgen-envoy-http-redirect"

  default_url_redirect {
    https_redirect         = true
    redirect_response_code = "MOVED_PERMANENTLY_DEFAULT"
    strip_query            = false
  }
}

resource "google_compute_target_http_proxy" "envoy_http_proxy" {
  name    = "loadgen-envoy-http-proxy"
  url_map = google_compute_url_map.envoy_http_redirect.id
}

resource "google_compute_global_forwarding_rule" "envoy_http_forwarding_rule" {
  name       = "loadgen-envoy-http-forwarding-rule"
  target     = google_compute_target_http_proxy.envoy_http_proxy.id
  port_range = "80"
}

# Outputs
output "load_balancer_ip" {
  description = "External IP address of the load balancer"
  value       = google_compute_global_forwarding_rule.envoy_https_forwarding_rule.ip_address
}

output "mig_instance_group" {
  description = "Instance group of the Envoy MIG"
  value       = google_compute_instance_group_manager.envoy_mig.instance_group
}

output "service_account_email" {
  description = "Service account email for Envoy instances"
  value       = google_service_account.envoy_sa.email
}