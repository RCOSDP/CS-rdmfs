import pytest
from mock import MagicMock, AsyncMock, patch

import pyfuse3

from rdmfs.inode import (
    Inodes,
    ProjectInode,
    FolderInode,
    FileInode,
    StorageInode,
    ProjectAttributesInode,
    ProjectAttributesEntry,
    ProjectChildrenInode,
    ProjectChildrenEntry,
    ProjectLinkedInode,
    ProjectLinkedEntry,
)
from .mocks import FutureWrapper, MockProject, MockFolder, AsyncIterator
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
    MockOSF._build_url = MagicMock(side_effect=lambda *parts: 'https://api.test/' + '/'.join(parts) + '/')
    metadata_response = MagicMock()
    metadata_response.status_code = 200
    metadata_response.json.return_value = {
        'data': {
            'attributes': {
                'title': 'Project Test',
            }
        }
    }
    MockOSF._get = AsyncMock(return_value=metadata_response)
    MockOSF._json = MagicMock(side_effect=lambda resp, status: resp.json())
    def _create_object_inode(i, p, o):
        if isinstance(o, ProjectAttributesEntry):
            return ProjectAttributesInode(i, o.project_inode, o)
        if isinstance(o, ProjectChildrenEntry):
            return ProjectChildrenInode(i, o.project_inode)
        if isinstance(o, ProjectLinkedEntry):
            return ProjectLinkedInode(i, o.project_inode)
        if o.name.startswith('Folder-'):
            return FolderInode(i, p, o)
        if o.name.startswith('File-'):
            return FileInode(i, p, o)
        return StorageInode(i, p, o)
    mock_create_object_inode.side_effect = _create_object_inode
    async def _metadata_fetcher():
        return {}

    def _get_children_of(p):
        if isinstance(p, ProjectChildrenInode):
            return AsyncIterator([])
        if isinstance(p, ProjectLinkedInode):
            return AsyncIterator([])
        if isinstance(p, ProjectInode):
            metadata = ProjectAttributesEntry(p, _metadata_fetcher, {})
            children = ProjectChildrenEntry(p)
            linked = ProjectLinkedEntry(p)
            storages = []
            if hasattr(p.object, 'storages'):
                storages = list(p.object.storages.__aiter__.return_value)
            return AsyncIterator([metadata, children, linked, *storages])
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
        'token_a', b'.attributes.json', {
            'text': 'test metadata'
        }, 1
    )
    mock_readdir_reply.reset_mock()

    await fc.readdir(1, 'token_b')

    mock_readdir_reply.assert_called_once_with(
        'token_b', b'.children', {
            'text': 'test metadata'
        }, 2
    )
    mock_readdir_reply.reset_mock()

    await fc.readdir(2, 'token_c')

    mock_readdir_reply.assert_called_once_with(
        'token_c', b'.linked', {
            'text': 'test metadata'
        }, 3
    )
    mock_readdir_reply.reset_mock()

    await fc.readdir(3, 'token_d')

    mock_readdir_reply.assert_called_once_with(
        'token_d', b'osfstorage', {
            'text': 'test metadata'
        }, 4
    )
    mock_readdir_reply.reset_mock()

    await fc.readdir(4, 'token_e')

    mock_readdir_reply.assert_called_once_with(
        'token_e', b'gh', {
            'text': 'test metadata'
        }, 5
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
    MockOSF._build_url = MagicMock(side_effect=lambda *parts: 'https://api.test/' + '/'.join(parts) + '/')
    metadata_response = MagicMock()
    metadata_response.status_code = 200
    metadata_response.json.return_value = {
        'data': {
            'attributes': {
                'title': 'Project Test',
            }
        }
    }
    MockOSF._get = AsyncMock(return_value=metadata_response)
    MockOSF._json = MagicMock(side_effect=lambda resp, status: resp.json())
    def _create_object_inode(i, p, o):
        if isinstance(o, ProjectAttributesEntry):
            return ProjectAttributesInode(i, o.project_inode, o)
        if isinstance(o, ProjectChildrenEntry):
            return ProjectChildrenInode(i, o.project_inode)
        if isinstance(o, ProjectLinkedEntry):
            return ProjectLinkedInode(i, o.project_inode)
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


@pytest.mark.asyncio
@patch.object(pyfuse3, 'readdir_reply')
async def test_readdir_from_all_projects_root(mock_readdir_reply):
    mock_osf = MagicMock()
    mock_osf.session = MagicMock()
    mock_osf._build_url = MagicMock(side_effect=lambda *parts: 'https://api.test/' + '/'.join(parts) + '/')
    node_payload = {
        'id': 'proj1',
        'type': 'nodes',
        'attributes': {
            'title': 'Project One',
            'registration': False,
        },
        'relationships': {
            'files': {
                'links': {
                    'related': {
                        'href': 'https://api.test/nodes/proj1/files/'
                    }
                }
            }
        }
    }
    response_payload = {
        'data': [node_payload],
        'links': {'next': None},
    }
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = response_payload
    mock_osf._get = AsyncMock(side_effect=[response])
    mock_osf._json = MagicMock(side_effect=lambda resp, status: resp.json())

    inodes = Inodes(mock_osf, None, list_all_projects=True)
    root_inode = await inodes.get(pyfuse3.ROOT_INODE)

    context = MagicMock()
    context.inodes = inodes
    context.getattr = AsyncMock(return_value={'text': 'meta'})

    fc = FileContext(context, root_inode)
    await fc.readdir(0, 'token_proj')

    mock_readdir_reply.assert_called_once_with(
        'token_proj', b'proj1', {'text': 'meta'}, 1
    )
    mock_osf._get.assert_awaited_once()
