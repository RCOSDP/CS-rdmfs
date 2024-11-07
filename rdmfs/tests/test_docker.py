import pytest, time

def wait_for_condition(condition_func, timeout=5, interval=1):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition_func():
            return True
        time.sleep(interval)
    return False

def test_docker(docker_container, rdm_storage):
    # List in directory /mnt/test/
    exit_code, output = docker_container.exec_run("ls /mnt/test/")
    assert exit_code == 0
    output = output.decode("utf-8").strip()
    output_lines = output.splitlines()
    assert rdm_storage in output_lines
    
    # create and list file
    file_name_1 = "File-1.txt"
    exit_code, output = docker_container.exec_run(f"touch /mnt/test/{rdm_storage}/{file_name_1}")
    assert exit_code == 0
    def ls_file_1_func():
        exit_code, output = docker_container.exec_run(f"ls /mnt/test/{rdm_storage}/")
        assert exit_code == 0
        output = output.decode("utf-8").strip()
        output_lines = output.splitlines()
        return file_name_1 in output_lines
    assert wait_for_condition(ls_file_1_func)

    # create and list file (Japanese file name)
    file_name_2 = "日本語の含まれているファイル-2.txt"
    exit_code, output = docker_container.exec_run(f"touch /mnt/test/{rdm_storage}/{file_name_2}")
    assert exit_code == 0
    def ls_file_2_func():
        exit_code, output = docker_container.exec_run(f"ls /mnt/test/{rdm_storage}/")
        assert exit_code == 0
        output = output.decode("utf-8").strip()
        output_lines = output.splitlines()
        return file_name_2 in output_lines
    assert wait_for_condition(ls_file_2_func)

    # create and remove and recreate file
    file_name_3 = "File-3.txt"
    exit_code, output = docker_container.exec_run(f"touch /mnt/test/{rdm_storage}/{file_name_3}")
    assert exit_code == 0
    def ls_file_3_func():
        exit_code, output = docker_container.exec_run(f"ls /mnt/test/{rdm_storage}/")
        assert exit_code == 0
        output = output.decode("utf-8").strip()
        output_lines = output.splitlines()
        return file_name_3 in output_lines
    assert wait_for_condition(ls_file_3_func)
    exit_code, output = docker_container.exec_run(f"rm /mnt/test/{rdm_storage}/{file_name_3}")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run(f"touch /mnt/test/{rdm_storage}/{file_name_3}")
    #TODO assert 1 == 0
    #assert exit_code == 0

    # write text to file
    file_content_1 = "123ABCあいう亜伊宇"
    command = f"echo '{file_content_1}' > /mnt/test/{rdm_storage}/{file_name_1}"
    exit_code, output = docker_container.exec_run(f"/bin/sh -c \'{command}'")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run(f"cat /mnt/test/{rdm_storage}/{file_name_1}")
    assert exit_code == 0
    output = output.decode("utf-8").strip()
    assert output == file_content_1

    # copy heavy file
    exit_code, output = docker_container.exec_run("dd if=/dev/urandom of=/tmp/test_200mb_file bs=1M count=200")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run("md5sum /tmp/test_200mb_file")
    assert exit_code == 0
    output = output.decode("utf-8").strip()
    hash_org = output.split()[0]
    file_name_heavy = "test_200mb_file_copied"
    exit_code, output = docker_container.exec_run(f"cp /tmp/test_200mb_file /mnt/test/{rdm_storage}/{file_name_heavy}")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run(f"md5sum /mnt/test/{rdm_storage}/{file_name_heavy}")
    assert exit_code == 0
    output = output.decode("utf-8").strip()
    hash_copied = output.split()[0]
    assert hash_org == hash_copied
    exit_code, output = docker_container.exec_run("rm /tmp/test_200mb_file")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run(f"rm /mnt/test/{rdm_storage}/{file_name_heavy}")
    assert exit_code == 0

    # make directory and file exists error
    dir_name_1 = "Folder-1"
    dir_name_1_2 = "Folder-1-2"
    exit_code, output = docker_container.exec_run(f"mkdir -p /mnt/test/{rdm_storage}/{dir_name_1}/{dir_name_1_2}")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run(f"ls /mnt/test/{rdm_storage}/{dir_name_1}/")
    output = output.decode("utf-8").strip()
    output_lines = output.splitlines()
    assert dir_name_1_2 in output_lines
    exit_code, output = docker_container.exec_run(f"mkdir /mnt/test/{rdm_storage}/{dir_name_1}/{dir_name_1_2}")
    output = output.decode("utf-8").strip()
    assert exit_code != 0 and "File exists" in output

    # make directory (long Japanese folder name)
    dir_name_long = "日本語で非常に長い名前のフォルダ名のフォルダ"
    exit_code, output = docker_container.exec_run(f"mkdir /mnt/test/{rdm_storage}/{dir_name_1}/{dir_name_long}")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run(f"ls /mnt/test/{rdm_storage}/{dir_name_1}/")
    output = output.decode("utf-8").strip()
    output_lines = output.splitlines()
    assert dir_name_long in output_lines

    # move file to file
    file_name_1_renamed = "File-1-renamed.txt"
    exit_code, output = docker_container.exec_run(f"mv /mnt/test/{rdm_storage}/{file_name_1} /mnt/test/{rdm_storage}/{file_name_1_renamed}")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run(f"ls /mnt/test/{rdm_storage}/")
    assert exit_code == 0
    output = output.decode("utf-8").strip()
    output_lines = output.splitlines()
    assert file_name_1 not in output_lines
    assert file_name_1_renamed in output_lines
    exit_code, output = docker_container.exec_run(f"mv /mnt/test/{rdm_storage}/{file_name_1_renamed} /mnt/test/{rdm_storage}/{file_name_1}")
    assert exit_code == 0

    # move file to folder
    # path_1 = f"/mnt/test/{rdm_storage}/{file_name_1}"
    # path_2 = f"/mnt/test/{rdm_storage}/{dir_name_1}/{dir_name_1_2}/"
    # exit_code, output = docker_container.exec_run(f"mv {path_1} {path_2}")
    # assert exit_code == 0
    # exit_code, output = docker_container.exec_run(f"ls /mnt/test/{rdm_storage}/")
    # output = output.decode("utf-8").strip()
    # output_lines = output.splitlines()
    # assert file_name_1 not in output_lines
    # exit_code, output = docker_container.exec_run(f"ls /mnt/test/{rdm_storage}/{dir_name_1}/{dir_name_1_2}")
    # output = output.decode("utf-8").strip()
    # output_lines = output.splitlines()
    #TODO assert AssertionError: assert 'File-1.txt' in []
    #assert file_name_1 in output_lines

    # delete files
    exit_code, output = docker_container.exec_run(f"rm /mnt/test/{rdm_storage}/{file_name_1}")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run(f"ls /mnt/test/{rdm_storage}/")
    output = output.decode("utf-8").strip()
    output_lines = output.splitlines()
    assert file_name_1 not in output_lines
    exit_code, output = docker_container.exec_run(f"rm /mnt/test/{rdm_storage}/{file_name_2}")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run(f"rm /mnt/test/{rdm_storage}/{file_name_3}")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run(f"rm -r /mnt/test/{rdm_storage}/{dir_name_1}")
    assert exit_code == 0
    exit_code, output = docker_container.exec_run(f"ls /mnt/test/{rdm_storage}/")
    output = output.decode("utf-8").strip()
    assert output == ""
    
