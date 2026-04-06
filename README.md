# Three-Tier EKS Platform - Production-Oriented Starter

This package upgrades the earlier starter into a more production-oriented platform layout.

## Included
- Terraform for EKS, ECR, IAM OIDC / IRSA, AWS Load Balancer Controller, optional MSK, and Secrets Manager
- Helm charts for frontend, backend, worker, and otel-collector
- GitOps via Argo CD app-of-apps
- External Secrets for AWS Secrets Manager integration
- Argo CD Image Updater support
- kube-prometheus-stack and Grafana via GitOps wrappers
- Grafana dashboards and SLO-style alerts
- OpenTelemetry-ready application code for logs, metrics, traces, Kafka, and DLQ flows

## Recommended deployment order
1. Terraform apply in `terraform/environments/dev`
2. Update repo URL and account ID using helper scripts
3. Install Argo CD
4. Apply `gitops/root-app.yaml`
5. Confirm sync of namespaces, monitoring, external-secrets, otel collector, and app charts
6. Push images from GitHub Actions
7. Validate dashboards, alerts, traces, logs, and metrics

## Important notes
- Replace sample policies and placeholder ARNs with your org-approved values.
- MSK is included as an optional Terraform starter.
- Argo CD Image Updater is configured as an option; some orgs prefer Git commit-based promotion instead.
