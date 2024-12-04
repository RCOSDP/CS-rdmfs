import pytest

from rdmfs.filehandle import FileHandlers


def test_filehandlers():
    fh = FileHandlers()

    new_fh = fh.get_node_fh('test_1')
    assert new_fh == 1
    assert fh.file_handlers[new_fh] == 'test_1'

    new_fh = fh.get_node_fh('test_2')
    assert new_fh == 2
    assert fh.file_handlers[new_fh] == 'test_2'

    fh.release_fh(1)

    new_fh = fh.get_node_fh('test_3')
    assert new_fh == 1
    assert fh.file_handlers[new_fh] == 'test_3'
