#!/usr/bin/env bash
set -euo pipefail
REPO_URL="${1:?usage: update_repo_urls.sh <repo-url>}"
find gitops -type f -name '*.yaml' -print0 | while IFS= read -r -d '' file; do
  sed -i.bak "s|https://github.com/sathish161084/observability_project.git|${REPO_URL}|g" "$file"
done
echo "updated repo URLs"
