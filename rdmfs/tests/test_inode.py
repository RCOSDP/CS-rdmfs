import io
import json

import pytest
from mock import MagicMock, patch, AsyncMock

import pyfuse3

from rdmfs.inode import Inodes, FolderInode, FileInode, StorageInode, ProjectsRootInode
from .mocks import FutureWrapper, MockProject


@pytest.mark.asyncio
@patch.object(Inodes, '_create_object_inode')
async def test_find_by_name(mock_create_object_inode):
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


@pytest.mark.asyncio
async def test_list_all_projects_creates_virtual_root():
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
    metadata_payload = {
        'data': {
            'attributes': {
                'title': 'Project One (updated)',
                'date_created': '2020-01-01T00:00:00Z',
            }
        }
    }
    children_payload = {
        'data': [
            {
                'id': 'child1',
                'type': 'nodes',
                'attributes': {
                    'title': 'Child Project',
                    'registration': False,
                },
                'relationships': {
                    'files': {
                        'links': {
                            'related': {
                                'href': 'https://api.test/nodes/child1/files/'
                            }
                        }
                    }
                }
            }
        ],
        'links': {'next': None},
    }
    child_metadata_payload = {
        'data': {
            'attributes': {
                'title': 'Child Project Latest',
            }
        }
    }
    linked_payload = {
        'data': [
            {
                'id': 'linked1',
                'type': 'nodes',
                'attributes': {
                    'title': 'Linked Project',
                    'registration': False,
                },
                'relationships': {
                    'files': {
                        'links': {
                            'related': {
                                'href': 'https://api.test/nodes/linked1/files/'
                            }
                        }
                    }
                }
            }
        ],
        'links': {'next': None},
    }
    linked_metadata_payload = {
        'data': {
            'attributes': {
                'title': 'Linked Project Latest',
            }
        }
    }
    response_list = MagicMock()
    response_list.status_code = 200
    response_list.json.return_value = response_payload
    response_meta = MagicMock()
    response_meta.status_code = 200
    response_meta.json.return_value = metadata_payload
    response_children = MagicMock()
    response_children.status_code = 200
    response_children.json.return_value = children_payload
    response_child_meta = MagicMock()
    response_child_meta.status_code = 200
    response_child_meta.json.return_value = child_metadata_payload
    response_linked = MagicMock()
    response_linked.status_code = 200
    response_linked.json.return_value = linked_payload
    response_linked_meta = MagicMock()
    response_linked_meta.status_code = 200
    response_linked_meta.json.return_value = linked_metadata_payload
    storage_response = MagicMock()
    storage_response.status_code = 200
    storage_response.json.return_value = {'data': []}
    mock_osf.session.get = AsyncMock(return_value=storage_response)
    mock_osf._get = AsyncMock(side_effect=[
        response_list,
        response_meta,
        response_children,
        response_child_meta,
        response_linked,
        response_linked_meta,
    ])
    mock_osf._json = MagicMock(side_effect=lambda resp, status: resp.json())

    inodes = Inodes(mock_osf, None, list_all_projects=True)
    root_inode = await inodes.get(pyfuse3.ROOT_INODE)

    assert isinstance(root_inode, ProjectsRootInode)

    project_inode = await inodes.find_by_name(root_inode, 'proj1')
    assert project_inode is not None
    assert project_inode.name == 'proj1'
    assert project_inode.display_path == '/proj1/'

    # second call should use cache
    metadata_inode = await inodes.find_by_name(project_inode, '.attributes.json')
    assert metadata_inode.name == '.attributes.json'
    assert metadata_inode.readonly

    buffer = io.BytesIO()
    await metadata_inode.object.write_to(buffer)
    payload = json.loads(buffer.getvalue().decode('utf-8'))
    assert payload['title'] == 'Project One (updated)'
    assert metadata_inode.object.attributes['title'] == 'Project One (updated)'

    children_dir = await inodes.find_by_name(project_inode, '.children')
    assert children_dir.name == '.children'

    child_project = await inodes.find_by_name(children_dir, 'child1')
    assert child_project.name == 'child1'

    child_metadata_inode = await inodes.find_by_name(child_project, '.attributes.json')
    buffer_child = io.BytesIO()
    await child_metadata_inode.object.write_to(buffer_child)
    assert json.loads(buffer_child.getvalue().decode('utf-8'))['title'] == 'Child Project Latest'

    linked_dir = await inodes.find_by_name(project_inode, '.linked')
    assert linked_dir.name == '.linked'

    linked_project = await inodes.find_by_name(linked_dir, 'linked1')
    assert linked_project.name == 'linked1'

    linked_metadata_inode = await inodes.find_by_name(linked_project, '.attributes.json')
    buffer_linked = io.BytesIO()
    await linked_metadata_inode.object.write_to(buffer_linked)
    assert json.loads(buffer_linked.getvalue().decode('utf-8'))['title'] == 'Linked Project Latest'

    project_inode_again = await inodes.find_by_name(root_inode, 'proj1')
    assert project_inode_again.id == project_inode.id
    assert mock_osf._get.await_count == 6


@pytest.mark.asyncio
async def test_list_all_projects_handles_pagination():
    mock_osf = MagicMock()
    mock_osf.session = MagicMock()
    mock_osf._build_url = MagicMock(return_value='https://api.test/users/me/nodes/')

    node_payload_page1 = {
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
    node_payload_page2 = {
        'id': 'proj2',
        'type': 'nodes',
        'attributes': {
            'title': 'Project Two',
            'registration': False,
        },
        'relationships': {
            'files': {
                'links': {
                    'related': {
                        'href': 'https://api.test/nodes/proj2/files/'
                    }
                }
            }
        }
    }

    page1_payload = {
        'data': [node_payload_page1],
        'links': {'next': 'https://api.test/users/me/nodes/?page=2'},
    }
    page2_payload = {
        'data': [node_payload_page2],
        'links': {'next': None},
    }

    metadata_payload_proj1 = {
        'data': {
            'attributes': {
                'title': 'Project One latest',
            }
        }
    }
    metadata_payload_proj2 = {
        'data': {
            'attributes': {
                'title': 'Project Two latest',
            }
        }
    }

    response_page1 = MagicMock()
    response_page1.status_code = 200
    response_page1.json.return_value = page1_payload
    response_page2 = MagicMock()
    response_page2.status_code = 200
    response_page2.json.return_value = page2_payload
    response_meta1 = MagicMock()
    response_meta1.status_code = 200
    response_meta1.json.return_value = metadata_payload_proj1
    response_meta2 = MagicMock()
    response_meta2.status_code = 200
    response_meta2.json.return_value = metadata_payload_proj2

    storage_response = MagicMock()
    storage_response.status_code = 200
    storage_response.json.return_value = {'data': []}
    mock_osf.session.get = AsyncMock(return_value=storage_response)
    mock_osf._get = AsyncMock(side_effect=[response_page1, response_page2, response_meta1, response_meta2])
    mock_osf._json = MagicMock(side_effect=lambda resp, status: resp.json())

    inodes = Inodes(mock_osf, None, list_all_projects=True)
    root_inode = await inodes.get(pyfuse3.ROOT_INODE)

    first_project = await inodes.find_by_name(root_inode, 'proj1')
    assert first_project is not None

    second_project = await inodes.find_by_name(root_inode, 'proj2')
    assert second_project is not None

    metadata_first = await inodes.find_by_name(first_project, '.attributes.json')
    metadata_second = await inodes.find_by_name(second_project, '.attributes.json')

    buffer1 = io.BytesIO()
    buffer2 = io.BytesIO()
    await metadata_first.object.write_to(buffer1)
    await metadata_second.object.write_to(buffer2)

    assert json.loads(buffer1.getvalue().decode('utf-8'))['title'] == 'Project One latest'
    assert json.loads(buffer2.getvalue().decode('utf-8'))['title'] == 'Project Two latest'

    assert mock_osf._get.await_count == 4
