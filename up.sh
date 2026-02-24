#!/usr/bin/env sh
set -e

echo 'Starting LocalStack...'
docker compose up -d localstack

echo 'Waiting for LocalStack to be healthy...'
for i in 1 2 3 4 5 6 7 8 9 10; do
  curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1 && break || sleep 2
done

echo 'Running Terraform...'
cd terraform
terraform init -input=false
terraform apply -auto-approve -input=false
cd ..

echo 'Starting full stack...'
docker compose up --build
