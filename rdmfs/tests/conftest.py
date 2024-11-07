# content of conftest.py
import pytest, docker, os, time

@pytest.fixture
def rdm_storage():
    return os.getenv("RDM_STORAGE", "osfstorage")

@pytest.fixture
def docker_container():
    # 引数からパラメータを取得
    rdm_node_id = os.getenv("RDM_NODE_ID", "rdm_node_id")
    rdm_token = os.getenv("RDM_TOKEN", "rdm_token")

    # Dockerクライアントを作成
    client = docker.from_env()
    
    # Dockerコンテナを起動
    container = client.containers.run(
        "rcosdp/cs-rdmfs",  # 使用するDockerイメージを指定
        name="rdmfs",
        detach=True,
        tty=True,
        remove=True,
        privileged=True,
        auto_remove=True,  # コンテナ停止後に自動削除
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
    
    # 特定のログメッセージが現れるまで待機
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