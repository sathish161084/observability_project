#!/usr/bin/env bash
set -euo pipefail
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION="${2:-us-east-1}"
find gitops -type f -name '*.yaml' -print0 | while IFS= read -r -d '' file; do
  sed -i.bak "s|ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com|${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com|g" "$file"
done
echo "updated account id references"
