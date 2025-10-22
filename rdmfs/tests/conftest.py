# content of conftest.py
import pytest, docker, os, time

@pytest.fixture
def rdm_storage():
    return os.getenv("RDM_STORAGE", "osfstorage")

@pytest.fixture
def docker_container():
    if os.getenv("SKIP_TEST_DOCKER"):
        pytest.skip("SKIP_TEST_DOCKER is set.")

    # Retrieve RDM_NODE_ID and RDM_TOKEN from environment variables
    rdm_node_id = os.getenv("RDM_NODE_ID", "")
    rdm_token = os.getenv("RDM_TOKEN", "")
    if not rdm_node_id or not rdm_token:
        raise ValueError("RDM_NODE_ID and RDM_TOKEN must be set as secrets.")

    # Create a Docker container
    client = docker.from_env()
    container = client.containers.run(
        "rcosdp/cs-rdmfs",
        name="rdmfs",
        detach=True,
        tty=True,
        remove=True,
        privileged=True,
        auto_remove=True,
        environment={
            "RDM_NODE_ID": rdm_node_id,
            "RDM_TOKEN": rdm_token,
            "RDM_API_URL": "https://api.rdm.nii.ac.jp/v2/",
            "MOUNT_PATH": "/mnt/test"
        },
        volumes={
            f"{os.getcwd()}/mnt": {'bind': '/mnt', 'mode': 'rw'}
        }
    )

    # Wait for the container to start
    message_to_wait_for = "[pyfuse3] pyfuse-02: No tasks waiting, starting another worker"
    timeout = 30

    start_time = time.time()
    while time.time() - start_time < timeout:
        logs = container.logs().decode("utf-8")
        if message_to_wait_for in logs:
            print("Log message found:", message_to_wait_for)
            break
        time.sleep(1)
    else:
        container.kill()
        raise RuntimeError(f"'{message_to_wait_for}' was not found in the container logs within {timeout} seconds.")
    
    yield container
    
    container.kill()