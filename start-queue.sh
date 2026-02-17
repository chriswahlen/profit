#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${ROOT}/venv/bin/activate"

# Expose the vendored AgentAPI submodule packages (service/, agentapi/, llm/).
export PYTHONPATH="${ROOT}:${ROOT}/libs/agentapi${PYTHONPATH:+:${PYTHONPATH}}"

DB_PATH="${AGENTAPI_DB_PATH:-/tmp/agentapi.sqlite}"
REGISTRY_MODULE="${AGENTAPI_JOB_REGISTRY:-agents.financial_adviser.job_registry}"
POLL_INTERVAL="${AGENTAPI_POLL_INTERVAL:-0.5}"

python3 -m service.main \
  --db "${DB_PATH}" \
  --registry "${REGISTRY_MODULE}" \
  --poll-interval "${POLL_INTERVAL}" \
  -v \
  "$@"
