#!/usr/bin/env bash
set -euo pipefail

cmd=${1:-}
pgdata=${2:-/home/hmaduri/pg18/data}
prefix=${3:-/home/hmaduri/pg18/inst}
port=${4:-5518}

# Resolve repo root = parent of this script dir
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

installed_postgres="${prefix}/bin/postgres"
build_postgres="${repo_root}/build/src/backend/postgres"
installed_pgctl="${prefix}/bin/pg_ctl"

# Prefer installed; fall back to build-tree binary if missing
if [[ -x "${installed_postgres}" ]]; then
  POSTGRES_BIN="${installed_postgres}"
  PGCTL_BIN="${installed_pgctl}"
  export PATH="${prefix}/bin:${PATH}"
  export LD_LIBRARY_PATH="${prefix}/lib:${LD_LIBRARY_PATH:-}"
else
  if [[ -x "${build_postgres}" ]]; then
    POSTGRES_BIN="${build_postgres}"
    PGCTL_BIN="${prefix}/bin/pg_ctl"  # may still exist; not required for start
    # Add build dir for shared libs
    export LD_LIBRARY_PATH="${repo_root}/build:${LD_LIBRARY_PATH:-}"
    echo "[info] Using build-tree postgres: ${POSTGRES_BIN}"
  else
    echo "ERROR: postgres not found at ${installed_postgres} or ${build_postgres}" >&2
    echo "Run: ninja -C ${repo_root}/build install    (or just build)" >&2
    exit 127
  fi
fi

export PGDATA="${pgdata}"

case "${cmd}" in
  start)
    if [[ ! -d "${PGDATA}" ]]; then
      echo "PGDATA ${PGDATA} not found. Initialize once with:" >&2
      echo "  ${prefix}/bin/initdb -D ${PGDATA}" >&2
      exit 2
    fi
    echo "Starting postgres on port ${port} (PGDATA=${PGDATA})"
    exec "${POSTGRES_BIN}" -D "${PGDATA}" -p "${port}"
    ;;
  stop)
    if [[ -x "${installed_pgctl}" ]]; then
      "${installed_pgctl}" -D "${PGDATA}" stop -m fast
    else
      # Fallback: signal the postmaster
      if [[ -f "${PGDATA}/postmaster.pid" ]]; then
        pid=$(head -n1 "${PGDATA}/postmaster.pid")
        echo "Stopping PID ${pid}"
        kill -INT "${pid}"
      else
        echo "postmaster.pid not found; is the server running?" >&2
      fi
    fi
    ;;
  *)
    echo "Usage: $0 {start|stop} [PGDATA] [PREFIX] [PORT]" >&2
    exit 1
    ;;
esac

