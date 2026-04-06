# After Terraform apply

1. Update kubeconfig:
```bash
aws eks update-kubeconfig --region eu-west-1 --name <cluster-name>
```

2. Install Argo CD:
```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
```

3. Update repo URL and account ID:
```bash
./scripts/update_repo_urls.sh https://github.com/<org-or-user>/three-tier-eks-platform-prod.git
./scripts/update_account_id.sh <aws-account-id> eu-west-1
```

4. Apply root app:
```bash
kubectl apply -f gitops/root-app.yaml
```

5. If MSK is enabled, update `gitops/values/common-values.yaml` with the Terraform MSK bootstrap broker output.
