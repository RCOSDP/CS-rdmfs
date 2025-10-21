# RDMFS

RDMFS is a FUSE filesystem that allows you to mount your GakuNin RDM project as a filesystem.

# How to run

RDMFS requires libfuse-dev to be installed on your system.

## Mount a single project

You can easily try out RDMFS by using a Docker container with libfuse-dev installed. The following example mounts a single project by supplying its node ID via environment variables.

```
$ docker build -t rcosdp/cs-rdmfs .
$ docker run -it -v $(pwd)/mnt:/mnt -e RDM_NODE_ID=xxxxx -e RDM_TOKEN=YOUR_PERSONAL_TOKEN -e RDM_API_URL=http://192.168.168.167:8000/v2/ -e MOUNT_PATH=/mnt/test --name rdmfs --privileged rcosdp/cs-rdmfs
```

You can manipulate the files in your project from /mnt/test in the `rdmfs` container that has been started.

```
$ docker exec -it rdmfs bash
# cd /mnt/test
# ls
googledrive osfstorage
# cd osfstorage
# ls
file1.txt file2.txt
```

## Mount all accessible projects

Omit `RDM_NODE_ID` when you launch the Docker container (or pass `--all-projects` to the CLI) to expose every project you can access under the mount root.
This layout adds `.children` / `.linked` directories that contain symbolic links to related projects, while single-project mounts keep the previous structure and hide them.

```
$ docker run -it -v $(pwd)/mnt:/mnt -e RDM_TOKEN=YOUR_PERSONAL_TOKEN -e RDM_API_URL=http://192.168.168.167:8000/v2/ -e MOUNT_PATH=/mnt/all --name rdmfs --privileged rcosdp/cs-rdmfs
$ docker exec -it rdmfs bash
# cd /mnt/all
# ls
abcde fghij klmno
# ls abcde
googledrive osfstorage
# ls abcde/.linked
klmno -> ../../klmno/
```

> Links to projects where you are not a contributor (public projects) are not supported; GakuNin RDM deployments do not expose publicly accessible nodes.

# Run Tests on Docker

You can run the tests on a Docker container by executing the following commands.

```
$ docker build --build-arg DEV=true -t rcosdp/cs-rdmfs .
$ docker run --rm -v $(pwd):/code -w /code -it rcosdp/cs-rdmfs py.test --cov
```
