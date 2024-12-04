# RDMFS

RDMFS is a FUSE filesystem that allows you to mount your GakuNin RDM project as a filesystem.

# How to run

RDMFS requires libfuse-dev to be installed on your system.

## Run RDMFS on Docker

You can easily try out RDMFS by using a Docker container with libfuse-dev installed.
You can try RDMFS by executing the following commands.

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

# Run Tests on Docker

You can run the tests on a Docker container by executing the following commands.

```
$ docker build --build-arg DEV=true -t rcosdp/cs-rdmfs .
$ docker run --rm -v $(pwd):/code -w /code -it rcosdp/cs-rdmfs py.test --cov
```
