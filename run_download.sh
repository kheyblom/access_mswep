#!/bin/bash
# Start the MSWEP download on a login node, fully detached from this shell.
#
# A download is network bound, not CPU bound: the workers spend nearly all their
# time waiting on Google, and Drive's per user request limit caps the rate well
# below anything a login node would notice. It is still shared hardware, so the
# job runs at the lowest scheduling priority, and rclone.bwlimit in the config
# caps bandwidth if the transfer needs to be made quieter still.
#
# setsid puts the job in a session of its own. nohup alone only blocks SIGHUP,
# which is not enough: anything that kills this shell's process group -- an
# editor session ending, a tool harness tearing down -- would take the download
# with it.
#
# Usage:
#   ./run_download.sh                                  # full record
#   ./run_download.sh config/config_download_tiny.yaml # one year, smoke test
#   ./run_download.sh --status                         # is a run in progress?

set -euo pipefail

cd "$(dirname "$0")"

CONFIG="${1:-config/config_download.yaml}"

# the log and pid file names come from the config, so --status has to know which
# config to look at; default to the full one
status_only=false
if [[ "${CONFIG}" == "--status" ]]; then
    status_only=true
    CONFIG="${2:-config/config_download.yaml}"
fi

if [[ ! -f "${CONFIG}" ]]; then
    echo "no such config: ${CONFIG}" >&2
    exit 1
fi

# pull these out of the yaml with sed rather than importing a parser, so the
# script still works before the environment is built
LOG_FILE=$(sed -n 's/^log_file:[[:space:]]*//p' "${CONFIG}" | head -1)
LOG_DIR=$(sed -n 's/^[[:space:]]*logs:[[:space:]]*//p' "${CONFIG}" | head -1)
PID_FILE="${LOG_DIR}/${LOG_FILE%.log}.pid"

report_status() {
    if [[ ! -f "${PID_FILE}" ]]; then
        echo "no run in progress (no ${PID_FILE})"
        if [[ -f "${LOG_DIR}/${LOG_FILE}" ]]; then
            echo "last log line:"
            tail -n 1 "${LOG_DIR}/${LOG_FILE}" | sed 's/^/  /'
        fi
        return 1
    fi
    read -r RUN_HOST RUN_PID < "${PID_FILE}"
    echo "run recorded on ${RUN_HOST} as pid ${RUN_PID}"
    if [[ "${RUN_HOST}" == "$(hostname)" ]]; then
        if kill -0 "${RUN_PID}" 2>/dev/null; then
            echo "  alive on this node"
        else
            echo "  NOT alive -- stale pid file, the run died; rerun to resume"
        fi
    else
        # process tables are per node even though GLADE is shared
        echo "  started on a different login node; check it with:"
        echo "    ssh ${RUN_HOST} 'kill -0 ${RUN_PID} && echo alive || echo dead'"
        echo "  stop it with:"
        echo "    ssh ${RUN_HOST} 'kill -INT ${RUN_PID}'"
    fi
    echo "  progress: tail -n 40 ${LOG_DIR}/${LOG_FILE}"
    return 0
}

if [[ "${status_only}" == true ]]; then
    report_status
    exit $?
fi

export RCLONE_CONFIG="${RCLONE_CONFIG:-${HOME}/.config/rclone/rclone.conf}"
if [[ ! -f "${RCLONE_CONFIG}" ]]; then
    echo "no rclone config at ${RCLONE_CONFIG}; see the setup section of README.md" >&2
    exit 1
fi

# refuse to start a second run over the same download directory: two runs would
# race on the same .partial files and sweep each other's
if [[ -f "${PID_FILE}" ]]; then
    read -r RUN_HOST RUN_PID < "${PID_FILE}"
    if [[ "${RUN_HOST}" == "$(hostname)" ]] && kill -0 "${RUN_PID}" 2>/dev/null; then
        echo "already running here as pid ${RUN_PID}; stop it first with kill -INT ${RUN_PID}" >&2
        exit 1
    fi
    echo "note: found a stale ${PID_FILE} from ${RUN_HOST}, continuing"
fi

mkdir -p "${LOG_DIR}"

# nice 19 so the download yields to anything interactive on the shared node
setsid nohup nice -n 19 uv run python mswep_download.py --config "${CONFIG}" \
    > nohup.out 2>&1 < /dev/null &

echo "started on $(hostname) with ${CONFIG}"
echo "  the run writes ${PID_FILE} with its host and pid once enumeration finishes"
echo "  progress:  tail -n 40 ${LOG_DIR}/${LOG_FILE}"
echo "  status:    ./run_download.sh --status ${CONFIG}"
