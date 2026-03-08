#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${ROOT}/venv/bin/activate"

# Expose the vendored AgentAPI submodule packages (service/, agentapi/, llm/).
export PYTHONPATH="${ROOT}:${ROOT}/libs/agentapi${PYTHONPATH:+:${PYTHONPATH}}"

DB_PATH="${AGENTAPI_DB_PATH:-}"
if [[ -z "${DB_PATH}" ]]; then
  DB_PATH="$(python3 -c 'from config import Config; from pathlib import Path; print(Path(Config().data_path()) / "agentapi.sqlite")')"
fi
REGISTRY_MODULE="${AGENTAPI_JOB_REGISTRY:-agents.financial_adviser.job_registry}"
HOST="${AGENTAPI_FRONTEND_HOST:-127.0.0.1}"
PORT="${AGENTAPI_FRONTEND_PORT:-8070}"
DEFAULT_JOB_TYPE="${AGENTAPI_DEFAULT_JOB_TYPE:-financial_adviser}"

python3 -m service.frontend.server \
  --db "${DB_PATH}" \
  --registry "${REGISTRY_MODULE}" \
  --host "${HOST}" \
  --port "${PORT}" \
  --default-job-type "${DEFAULT_JOB_TYPE}" \
  -v \
  "$@"
