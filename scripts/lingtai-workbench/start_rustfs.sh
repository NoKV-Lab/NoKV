#!/usr/bin/env bash
set -euo pipefail

if repo_root="$(git rev-parse --show-toplevel 2>/dev/null)"; then
  :
else
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi
data_root="${LINGTAI_WORKBENCH_DATA_ROOT:-${repo_root}/target/lingtai-workbench}"
container="${LINGTAI_WORKBENCH_RUSTFS_CONTAINER:-lingtai-workbench-rustfs}"
image="${LINGTAI_WORKBENCH_RUSTFS_IMAGE:-rustfs/rustfs:latest}"
host="${LINGTAI_WORKBENCH_RUSTFS_HOST:-127.0.0.1}"
port="${LINGTAI_WORKBENCH_RUSTFS_PORT:-9000}"
console_port="${LINGTAI_WORKBENCH_RUSTFS_CONSOLE_PORT:-9001}"
access_key="${LINGTAI_WORKBENCH_S3_ACCESS_KEY_ID:-rustfsadmin}"
secret_key="${LINGTAI_WORKBENCH_S3_SECRET_ACCESS_KEY:-rustfsadmin}"
bucket="${LINGTAI_WORKBENCH_S3_BUCKET:-nokv-lingtai-workbench}"
endpoint="${LINGTAI_WORKBENCH_S3_ENDPOINT:-http://${host}:${port}}"
rustfs_data="${LINGTAI_WORKBENCH_RUSTFS_DATA_DIR:-${data_root}/rustfs}"

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI is required to create and verify the RustFS bucket" >&2
  exit 1
fi

export AWS_ACCESS_KEY_ID="${access_key}"
export AWS_SECRET_ACCESS_KEY="${secret_key}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=true

endpoint_ready=0
if aws --endpoint-url "${endpoint}" s3api list-buckets >/dev/null 2>&1; then
  endpoint_ready=1
fi

if [ "${endpoint_ready}" != "1" ]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required to start RustFS when ${endpoint} is not already reachable" >&2
    exit 1
  fi

  mkdir -p "${rustfs_data}"

  if docker container inspect "${container}" >/dev/null 2>&1; then
    if [ "$(docker inspect -f '{{.State.Running}}' "${container}")" != "true" ]; then
      docker start "${container}" >/dev/null
    fi
  else
    docker run -d \
      --name "${container}" \
      -p "${host}:${port}:9000" \
      -p "${host}:${console_port}:9001" \
      -e "RUSTFS_ACCESS_KEY=${access_key}" \
      -e "RUSTFS_SECRET_KEY=${secret_key}" \
      -e "RUSTFS_CONSOLE_ENABLE=true" \
      -v "${rustfs_data}:/data" \
      "${image}" \
      --address :9000 \
      --console-enable \
      --access-key "${access_key}" \
      --secret-key "${secret_key}" \
      /data >/dev/null
  fi
fi

ready=0
for _ in $(seq 1 60); do
  if aws --endpoint-url "${endpoint}" s3api list-buckets >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [ "${ready}" != "1" ]; then
  echo "RustFS did not become ready at ${endpoint}" >&2
  docker logs --tail 80 "${container}" >&2 || true
  exit 1
fi

if ! aws --endpoint-url "${endpoint}" s3api head-bucket --bucket "${bucket}" >/dev/null 2>&1; then
  aws --endpoint-url "${endpoint}" s3api create-bucket --bucket "${bucket}" >/dev/null
fi

cat <<EOF
LingTai workbench RustFS ready
  container: ${container}
  endpoint:  ${endpoint}
  bucket:    ${bucket}
  data:      ${rustfs_data}
EOF
