import pytest
from mock import MagicMock, AsyncMock, patch

import pyfuse3

from rdmfs.inode import Inodes, FolderInode, FileInode, StorageInode
from .mocks import FutureWrapper, MockProject, MockFolder
from rdmfs.node import FileContext


@pytest.mark.asyncio
@patch.object(Inodes, '_create_object_inode')
@patch.object(Inodes, 'get_children_of')
@patch.object(pyfuse3, 'readdir_reply')
async def test_readdir_from_project(
    mock_readdir_reply, mock_get_children_of, mock_create_object_inode
):
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
    def _get_children_of(p):
        return p.object.storages
    mock_get_children_of.side_effect = _get_children_of

    inodes = Inodes(MockOSF, 'test')
    project_inode = await inodes.get(pyfuse3.ROOT_INODE)

    context = MagicMock()
    context.inodes = inodes
    context.getattr = AsyncMock(return_value={
        'text': 'test metadata'
    })
    fc = FileContext(context, project_inode)
    await fc.readdir(0, 'token_a')

    mock_readdir_reply.assert_called_once_with(
        'token_a', b'osfstorage', {
            'text': 'test metadata'
        }, 1
    )
    mock_readdir_reply.reset_mock()

    await fc.readdir(1, 'token_b')

    mock_readdir_reply.assert_called_once_with(
        'token_b', b'gh', {
            'text': 'test metadata'
        }, 2
    )


@pytest.mark.asyncio
@patch.object(Inodes, '_create_object_inode')
@patch.object(pyfuse3, 'readdir_reply')
async def test_readdir_from_storage(
    mock_readdir_reply, mock_create_object_inode
):
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

    context = MagicMock()
    context.inodes = inodes
    context.getattr = AsyncMock(return_value={
        'text': 'test metadata'
    })
    fc = FileContext(context, storage_inode)
    await fc.readdir(0, 'token_a')

    mock_readdir_reply.assert_called_once_with(
        'token_a', b'a', {
            'text': 'test metadata'
        }, 1
    )
    mock_readdir_reply.reset_mock()

    await fc.readdir(1, 'token_b')

    mock_readdir_reply.assert_called_once_with(
        'token_b', b'b', {
            'text': 'test metadata'
        }, 2
    )
    mock_readdir_reply.reset_mock()

    await fc.readdir(2, 'token_c')

    mock_readdir_reply.assert_called_once_with(
        'token_c', b'c', {
            'text': 'test metadata'
        }, 3
    )
