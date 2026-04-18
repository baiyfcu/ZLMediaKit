#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
TARGET="esfileferry_packer_tests"
SECONDS_TO_SAMPLE=5
OUTPUT_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir)
      BUILD_DIR="$2"
      shift 2
      ;;
    --target)
      TARGET="$2"
      shift 2
      ;;
    --seconds)
      SECONDS_TO_SAMPLE="$2"
      shift 2
      ;;
    --output)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage:
  ./run_profile.sh [--build-dir <dir>] [--target <name>] [--seconds <n>] [--output <file>]

Examples:
  ./run_profile.sh
  ./run_profile.sh --target esfileferry_http_fetcher_tests
  ./run_profile.sh --seconds 10
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "${OUTPUT_FILE}" ]]; then
  OUTPUT_FILE="/tmp/${TARGET}.sample.txt"
fi

cmake -S "${SCRIPT_DIR}" -B "${BUILD_DIR}"
cmake --build "${BUILD_DIR}" --target "${TARGET}" -j4

BIN_PATH="${BUILD_DIR}/${TARGET}"
if [[ ! -x "${BIN_PATH}" ]]; then
  echo "Binary not found: ${BIN_PATH}" >&2
  exit 1
fi

if ! command -v sample >/dev/null 2>&1; then
  echo "'sample' command is not available on this system." >&2
  echo "Binary is ready: ${BIN_PATH}" >&2
  exit 1
fi

echo "Launching ${BIN_PATH} ..."
"${BIN_PATH}" >/tmp/${TARGET}.stdout.log 2>/tmp/${TARGET}.stderr.log &
TARGET_PID=$!
trap 'kill "${TARGET_PID}" >/dev/null 2>&1 || true' EXIT

sleep 0.2
if ! kill -0 "${TARGET_PID}" >/dev/null 2>&1; then
  echo "Target exited before sampling started." >&2
  echo "stdout: /tmp/${TARGET}.stdout.log" >&2
  echo "stderr: /tmp/${TARGET}.stderr.log" >&2
  exit 1
fi

echo "Sampling pid ${TARGET_PID} for ${SECONDS_TO_SAMPLE}s ..."
sample "${TARGET_PID}" "${SECONDS_TO_SAMPLE}" -file "${OUTPUT_FILE}"
set +e
wait "${TARGET_PID}"
TARGET_EXIT_CODE=$?
set -e
trap - EXIT
if [[ ${TARGET_EXIT_CODE} -ne 0 ]]; then
  echo "Target exited with code ${TARGET_EXIT_CODE}" >&2
  echo "stdout: /tmp/${TARGET}.stdout.log" >&2
  echo "stderr: /tmp/${TARGET}.stderr.log" >&2
fi
echo "Sample output written to ${OUTPUT_FILE}"
