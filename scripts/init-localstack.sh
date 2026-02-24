#!/bin/bash
set -e

# Wait for LocalStack to be ready
echo "Waiting for LocalStack S3..."
until curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "available"' 2>/dev/null || \
      curl -s http://localhost:4566/ 2>/dev/null | head -1 >/dev/null; do
  sleep 2
done

echo "LocalStack ready. Running Terraform..."
cd "$(dirname "$0")/../terraform"

if command -v tflocal &> /dev/null; then
  tflocal init -input=false
  tflocal apply -auto-approve -input=false
else
  echo "tflocal not found. Using terraform with TF_VAR overrides."
  export TF_VAR_s3_endpoint="http://localhost:4566"
  terraform init -input=false
  terraform apply -auto-approve -input=false
fi

echo "Provisioning complete."
