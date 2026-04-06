#!/usr/bin/env bash
set -euo pipefail
ACCOUNT_ID="${1:?usage: update_account_id.sh <account-id>}"
AWS_REGION="${2:-eu-west-1}"
find gitops -type f -name '*.yaml' -print0 | while IFS= read -r -d '' file; do
  sed -i.bak "s|ACCOUNT_ID.dkr.ecr.eu-west-1.amazonaws.com|${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com|g" "$file"
done
echo "updated account id references"
