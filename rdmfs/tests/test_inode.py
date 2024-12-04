import pytest
from mock import MagicMock, patch

import pyfuse3

from rdmfs.inode import Inodes, FolderInode, FileInode, StorageInode
from .mocks import FutureWrapper, MockProject


@pytest.mark.asyncio
@patch.object(Inodes, '_create_object_inode')
async def test_find_by_name(mock_create_object_inode):
    MockOSF = MagicMock()
    MockOSF.project = MagicMock(side_effect=lambda p: FutureWrapper(MockProject(p)))
    MockOSF.aclose = lambda: FutureWrapper()
    def _create_object_inode(i, p, o):
        if o.name.startswith('Folder-'):
            return FolderInode(i, p, o)
        if o.name.startswith('File-'):
            return FileInode(i, p, o)
        return StorageInode(i, p, o)
    mock_create_object_inode.side_effect = _create_object_inode

    inodes = Inodes(MockOSF, 'test')
    project_inode = await inodes.get(pyfuse3.ROOT_INODE)

    storage_inode = await inodes.find_by_name(project_inode, 'osfstorage')
    assert storage_inode.name == 'osfstorage'

    folder_inode = await inodes.find_by_name(storage_inode, 'b')
    assert folder_inode.name == 'b'

    sub_folder_inode = await inodes.find_by_name(folder_inode, 'b')
    assert sub_folder_inode.name == 'b'

    file_inode = await inodes.find_by_name(sub_folder_inode, 'b')
    assert file_inode.name == 'b'
