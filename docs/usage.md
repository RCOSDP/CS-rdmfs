# CS-rdmfs Usage Guide

## Overview

CS-rdmfs mounts projects from the Open Science Framework (OSF) as a FUSE filesystem. You can mount either a single project by ID or expose every accessible project beneath the mount root. Each project directory provides additional virtual entries that surface API metadata and related projects.

## Running via `bin/start.sh`

The recommended entrypoint (used by the Docker image) is `bin/start.sh`. It
reads environment variables, prepares the mount directory, and invokes
`python -m rdmfs` with the appropriate options.

Required environment variables:

- `RDM_NODE_ID` – project GUID to mount. (Omit or leave empty to mount all projects.)
- `RDM_TOKEN` – personal access token (forwarded to `OSF_TOKEN`).
- `RDM_API_URL` – API base URL (defaults to `https://api.rdm.nii.ac.jp/v2/`).
- `MOUNT_PATH` – mountpoint inside the container (default `/mnt`).

Optional overrides:

- `MOUNT_FILE_MODE` / `MOUNT_DIR_MODE` – forwarded to `--file-mode` and
  `--dir-mode`.
- Any extra arguments appended to `start.sh` are passed through to the Python
  module, enabling `--debug` or additional FUSE options.

If `RDM_NODE_ID` is unset (and you do not explicitly pass `--all-projects`),
`start.sh` automatically enables the all-projects mode.

### Docker Examples

Mount a single project inside `/mnt/osf`:

```bash
docker run --rm -it --privileged \
  -v "$(pwd)/mnt":/mnt \
  -e RDM_NODE_ID=abc123 \
  -e RDM_TOKEN=$RDM_TOKEN \
  -e RDM_API_URL=https://api.rdm.nii.ac.jp/v2/ \
  -e MOUNT_PATH=/mnt/osf \
  rcosdp/cs-rdmfs
```

Mount every accessible project (note: leaving `RDM_NODE_ID` unset has the same effect):

```bash
docker run --rm -it --privileged \
  -v "$(pwd)/mnt":/mnt \
  -e RDM_TOKEN=$RDM_TOKEN \
  -e RDM_API_URL=https://api.rdm.nii.ac.jp/v2/ \
  rcosdp/cs-rdmfs --all-projects
```

Or simply omit `RDM_NODE_ID`:

```bash
docker run --rm -it --privileged \
  -v "$(pwd)/mnt":/mnt \
  -e RDM_TOKEN=$RDM_TOKEN \
  -e RDM_API_URL=https://api.rdm.nii.ac.jp/v2/ \
  rcosdp/cs-rdmfs
```

## Direct CLI Usage

When running outside Docker (or bypassing `start.sh`), set `OSF_TOKEN` manually:

```bash
export OSF_TOKEN=your_token
python -m rdmfs [mountpoint] \
  (--project <project-id> | --all-projects) \
  [--base-url https://api.rdm.nii.ac.jp/v2/] \
  [--file-mode 0644] [--dir-mode 0755] \
  [--allow-other] [--debug] [--debug-fuse]
```

`--project` and `--all-projects` are mutually exclusive. Remaining options match
those used by `start.sh`.

## Virtual Directory Layout

Each mounted project contains virtual entries ahead of storage providers:

```
/project-id/
  .attributes.json   # live view of OSF node attributes (nodes_read)
  .children/         # child projects returned by nodes_children_list
  .linked/           # linked projects from collections_linked_nodes_list
  osfstorage/        # standard storage providers follow the virtual entries
  ...
```

- `.attributes.json` is read-only. Every read triggers `GET /v2/nodes/{id}/` and
  returns `data.attributes` formatted as indented JSON.
- `.children/` lists child node IDs; each entry behaves like a project directory
  with the same virtual structure.
- `.linked/` lists linked nodes; likewise they expose `.attributes.json`,
  `.children`, `.linked`, and storages.

## API Endpoints

The filesystem relies on:

- `GET /v2/nodes/{id}/` (nodes_read) for node attributes.
- `GET /v2/nodes/{id}/children/` (nodes_children_list) for child projects.
- `GET /v2/nodes/{id}/linked_nodes/` (collections_linked_nodes_list) for linked
  projects.

All collection requests apply `page[size]=100` to reduce page churn and follow
`links.next` until completion.

## Testing

Run the repository tests inside Docker. Supplying `RDM_NODE_ID` and `RDM_TOKEN`
allows the Docker-specific test to execute; omitting them results in a single
expected failure while the rest succeed.

```bash
docker run --rm -v "$(pwd)":/code -w /code \
  -e RDM_NODE_ID=your_project_id \
  -e RDM_TOKEN=$RDM_TOKEN \
  rcosdp/cs-rdmfs py.test --cov
```
