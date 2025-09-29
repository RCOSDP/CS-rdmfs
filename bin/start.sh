#!/bin/bash

set -ue

RDM_MOUNT_PATH=${MOUNT_PATH:-/mnt}
mkdir -p "${RDM_MOUNT_PATH}"

RDM_MOUNT_FILE_MODE=${MOUNT_FILE_MODE:-0666}
RDM_MOUNT_DIR_MODE=${MOUNT_DIR_MODE:-0777}
RDM_API_URL=${RDM_API_URL:-https://api.rdm.nii.ac.jp/v2/}
RDM_NODE_ID=${RDM_NODE_ID:-}

DEBUG=--debug
export OSF_TOKEN=${RDM_TOKEN}


ALL_PROJECTS=false
EXTRA_ARGS=()
for arg in "$@"; do
    if [ "$arg" = "--all-projects" ]; then
        ALL_PROJECTS=true
    else
        EXTRA_ARGS+=("$arg")
    fi
done

if [ -z "${RDM_NODE_ID}" ]; then
    ALL_PROJECTS=true
fi

CMD=(python3 -m rdmfs
    --file-mode "${RDM_MOUNT_FILE_MODE}"
    --dir-mode "${RDM_MOUNT_DIR_MODE}"
    --allow-other
    --base-url "${RDM_API_URL}"
    ${DEBUG}
)

if [ "${ALL_PROJECTS}" = true ]; then
    CMD+=(--all-projects)
else
    CMD+=(--project "${RDM_NODE_ID}")
fi

CMD+=("${RDM_MOUNT_PATH}")
CMD+=("${EXTRA_ARGS[@]}")

exec "${CMD[@]}"
