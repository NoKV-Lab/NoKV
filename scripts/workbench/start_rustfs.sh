#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 0 ]; then
  echo "start_rustfs.sh accepts no arguments; use NOKV_WORKBENCH_* environment variables" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
data_root="${NOKV_WORKBENCH_DATA_ROOT:-${repo_root}/target/workbench-live}"
container="${NOKV_WORKBENCH_RUSTFS_CONTAINER:-nokv-workbench-rustfs}"
image="${NOKV_WORKBENCH_RUSTFS_IMAGE:-rustfs/rustfs@sha256:e620d37756fff072b10bf648c7bb9d370d7e91a928b7e6a5e1ac85bdfb4e4dab}"
host="${NOKV_WORKBENCH_RUSTFS_HOST:-127.0.0.1}"
port="${NOKV_WORKBENCH_RUSTFS_PORT:-9000}"
console_port="${NOKV_WORKBENCH_RUSTFS_CONSOLE_PORT:-9001}"
access_key="${NOKV_WORKBENCH_S3_ACCESS_KEY_ID:-rustfsadmin}"
secret_key="${NOKV_WORKBENCH_S3_SECRET_ACCESS_KEY:-rustfsadmin}"
bucket="${NOKV_WORKBENCH_S3_BUCKET:-nokv-workbench-live}"
endpoint="${NOKV_WORKBENCH_S3_ENDPOINT:-http://${host}:${port}}"
rustfs_data="${NOKV_WORKBENCH_RUSTFS_DATA_DIR:-${data_root}/rustfs}"

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI is required to create and verify the RustFS bucket" >&2
  exit 1
fi

export AWS_ACCESS_KEY_ID="${access_key}"
export AWS_SECRET_ACCESS_KEY="${secret_key}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=true
export AWS_MAX_ATTEMPTS=1

aws_s3() {
  aws \
    --cli-connect-timeout 1 \
    --cli-read-timeout 2 \
    --endpoint-url "${endpoint}" \
    "$@"
}

endpoint_ready=0
if aws_s3 s3api list-buckets >/dev/null 2>&1; then
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
  if aws_s3 s3api list-buckets >/dev/null 2>&1; then
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

if ! aws_s3 s3api head-bucket --bucket "${bucket}" >/dev/null 2>&1; then
  aws_s3 s3api create-bucket --bucket "${bucket}" >/dev/null
fi

cat <<EOF
NoKV Workbench RustFS ready
  container: ${container}
  endpoint:  ${endpoint}
  bucket:    ${bucket}
  data:      ${rustfs_data}
EOF
