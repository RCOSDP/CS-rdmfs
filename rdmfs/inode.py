from datetime import datetime
import json
import sys
import logging
import errno
import time
import inspect
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from typing import Optional, Union, List, Dict, AsyncGenerator, Any

import pyfuse3
from cacheout import Cache
from osfclient import OSF
from osfclient.models import Storage, Project, File, Folder


log = logging.getLogger(__name__)
FILE_ATTRIBUTE_CACHE_TTL = 60 # 1 minute
LIST_CACHE_TTL = 180 # 3 minutes
NODE_PAGE_SIZE = 100


def fromisoformat(datestr):
    datestr = datestr.replace('Z', '+00:00')
    return int(datetime.fromisoformat(datestr).timestamp() * 1e9)


class BaseInode:
    """The class for managing single inode."""
    def __init__(self, id: int):
        if not isinstance(id, int):
            raise ValueError('Invalid inode id: {}'.format(id))
        self.id = id
        self.removed = False

    def __str__(self) -> str:
        return f'<{self.__class__.__name__} [id={self.id}, path={self.path}]>'

    async def refresh(self, context: 'Inodes', force=False):
        log.debug(f'nothing to refresh: inode={self.id}')

    def invalidate(self, name: Optional[str] = None):
        pass

    def remove(self):
        self.removed = True

    @property
    def parent(self) -> Optional['BaseInode']:
        raise NotImplementedError

    @property
    def storage(self) -> 'StorageInode':
        raise NotImplementedError

    @property
    def object(self):
        raise NotImplementedError

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def size(self) -> Optional[Union[int, str]]:
        return None

    def has_children(self) -> bool:
        return False

    @property
    def date_created(self) -> Optional[str]:
        return None

    @property
    def date_modified(self) -> Optional[str]:
        return None

    @property
    def path(self) -> str:
        raise NotImplementedError

    @property
    def display_path(self) -> str:
        raise self.path

    @property
    def can_create(self) -> bool:
        return False

    @property
    def can_move(self) -> bool:
        return False


class ProjectInode(BaseInode):
    """The class for managing single project inode."""
    def __init__(
        self,
        id: int,
        project: Project,
        parent: Optional[BaseInode] = None,
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super(ProjectInode, self).__init__(id)
        self.project = project
        self._parent = parent
        default_name = getattr(project, 'id', None)
        if default_name is None:
            default_name = getattr(project, 'title', '')
        self._name = str(name or default_name or '')
        self.metadata: Dict[str, Any] = metadata or {}

    @property
    def parent(self) -> Optional[BaseInode]:
        return self._parent

    @property
    def storage(self):
        raise ValueError('Project inode does not have storage')

    @property
    def object(self):
        return self.project

    @property
    def name(self):
        return self._name

    def has_children(self):
        return True

    @property
    def path(self):
        if self.parent is not None:
            return f'{self.parent.path}{self.name}/'
        return f'/{self.project.id}/'

    @property
    def display_path(self):
        if self.parent is not None:
            return f'{self.parent.display_path}{self.name}/'
        return '/'

    def update_metadata(self, metadata: Dict[str, Any]):
        self.metadata = metadata or {}
        setattr(self.project, '_rdmfs_attributes', self.metadata)


class ProjectAttributesEntry:
    """Virtual file representing project attributes as JSON."""

    def __init__(
        self,
        project_inode: 'ProjectInode',
        fetcher,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        self.project_inode = project_inode
        self._fetcher = fetcher
        self.attributes = attributes or {}
        self.name = '.attributes.json'

    @property
    def path(self) -> str:
        return f'{self.project_inode.path}{self.name}'

    @property
    def display_path(self) -> str:
        return f'{self.project_inode.display_path}{self.name}'

    @property
    def date_created(self) -> Optional[str]:
        return self.attributes.get('date_created')

    @property
    def date_modified(self) -> Optional[str]:
        return self.attributes.get('date_modified')

    @property
    def size(self) -> int:
        text = json.dumps(self.attributes, sort_keys=True, indent=2)
        return len(text.encode('utf-8'))

    async def write_to(self, fp):
        attrs = await self._fetcher()
        self.attributes = attrs or {}
        self.project_inode.update_metadata(self.attributes)
        text = json.dumps(self.attributes, sort_keys=True, indent=2)
        data = text.encode('utf-8')
        result = fp.write(data)
        if inspect.isawaitable(result):
            await result

    def invalidate(self):
        self.attributes = self.project_inode.metadata


class ProjectAttributesInode(BaseInode):
    """Inode exposing project attributes as a read-only JSON file."""

    def __init__(self, id: int, project_inode: ProjectInode, content: ProjectAttributesEntry):
        super(ProjectAttributesInode, self).__init__(id)
        self.project_inode = project_inode
        self._content = content
        self.readonly = True

    def invalidate(self, name: Optional[str] = None):
        self._content.invalidate()

    def set_content(self, content: ProjectAttributesEntry):
        self._content = content

    @property
    def parent(self) -> Optional[BaseInode]:
        return self.project_inode

    @property
    def storage(self):
        raise ValueError('Metadata inode does not have storage')

    @property
    def object(self):
        return self._content

    @property
    def name(self):
        return self._content.name

    def set_content(self, content: ProjectAttributesEntry):
        self._content = content

    def has_children(self):
        return False

    @property
    def path(self):
        return self._content.path

    @property
    def display_path(self):
        return self._content.display_path

    @property
    def size(self) -> Optional[int]:
        return self._content.size

    @property
    def date_created(self) -> Optional[str]:
        return self._content.date_created

    @property
    def date_modified(self) -> Optional[str]:
        return self._content.date_modified

    @property
    def can_move(self) -> bool:
        return False


class ProjectChildrenEntry:
    """Virtual directory exposing project child nodes."""

    def __init__(self, project_inode: 'ProjectInode'):
        self.project_inode = project_inode
        self.name = '.children'

    @property
    def path(self) -> str:
        return f'{self.project_inode.path}{self.name}/'

    @property
    def display_path(self) -> str:
        return f'{self.project_inode.display_path}{self.name}/'


class ProjectChildrenInode(BaseInode):
    """Inode representing the `.children` directory for a project."""

    def __init__(self, id: int, project_inode: ProjectInode):
        super(ProjectChildrenInode, self).__init__(id)
        self.project_inode = project_inode

    @property
    def parent(self) -> Optional[BaseInode]:
        return self.project_inode

    @property
    def storage(self):
        raise ValueError('Children inode does not have storage')

    @property
    def object(self):
        return self

    @property
    def name(self) -> str:
        return '.children'

    def has_children(self) -> bool:
        return True

    def invalidate(self, name: Optional[str] = None):
        pass

    @property
    def path(self) -> str:
        return f'{self.parent.path}{self.name}/'

    @property
    def display_path(self) -> str:
        return f'{self.parent.display_path}{self.name}/'


class ProjectLinkedEntry:
    """Virtual directory exposing project linked nodes."""

    def __init__(self, project_inode: 'ProjectInode'):
        self.project_inode = project_inode
        self.name = '.linked'

    @property
    def path(self) -> str:
        return f'{self.project_inode.path}{self.name}/'

    @property
    def display_path(self) -> str:
        return f'{self.project_inode.display_path}{self.name}/'


class ProjectLinkedInode(BaseInode):
    """Inode representing the `.linked` directory for a project."""

    def __init__(self, id: int, project_inode: ProjectInode):
        super(ProjectLinkedInode, self).__init__(id)
        self.project_inode = project_inode

    @property
    def parent(self) -> Optional[BaseInode]:
        return self.project_inode

    @property
    def storage(self):
        raise ValueError('Linked inode does not have storage')

    @property
    def object(self):
        return self

    @property
    def name(self) -> str:
        return '.linked'

    def has_children(self) -> bool:
        return True

    def invalidate(self, name: Optional[str] = None):
        pass

    @property
    def path(self) -> str:
        return f'{self.parent.path}{self.name}/'

    @property
    def display_path(self) -> str:
        return f'{self.parent.display_path}{self.name}/'


class ProjectsRootInode(BaseInode):
    """The virtual root inode when mounting all accessible projects."""
    def __init__(self, id: int):
        super(ProjectsRootInode, self).__init__(id)

    @property
    def parent(self) -> Optional[BaseInode]:
        return None

    @property
    def storage(self) -> 'StorageInode':
        raise ValueError('Root inode does not have storage')

    @property
    def object(self):
        return None

    @property
    def name(self):
        return ''

    def has_children(self):
        return True

    @property
    def path(self):
        return '/'

    @property
    def display_path(self):
        return '/'


class StorageInode(BaseInode):
    """The class for managing single storage inode."""
    def __init__(self, id: int, project: ProjectInode, storage: Storage):
        super(StorageInode, self).__init__(id)
        self.project = project
        self._storage = storage

    @property
    def parent(self) -> Optional[BaseInode]:
        return self.project

    @property
    def storage(self):
        return self

    @property
    def object(self):
        return self._storage

    @property
    def name(self):
        return self._storage.name

    def has_children(self):
        return True

    @property
    def path(self):
        return f'{self.parent.path}{self._storage.name}/'

    @property
    def display_path(self):
        return f'{self.parent.display_path}{self._storage.name}/'

    @property
    def can_create(self):
        return True


class BaseFileInode(BaseInode):
    """The class for managing single file inode."""
    _updated: Optional[Union[File, Folder]]
    _last_loaded: float
    _updated_name: Optional[str]

    def __init__(
        self,
        id: int,
        parent: BaseInode,
    ):
        super(BaseFileInode, self).__init__(id)
        self._parent = parent
        self._updated = None
        self._last_loaded = time.time()
        self._updated_name = None

    def invalidate(self, name: Optional[str] = None):
        self._last_loaded = None
        self._updated_name = name

    def set_parent(self, parent: BaseInode):
        self._parent = parent

    async def refresh(self, context: 'Inodes', force=False):
        expired = self._last_loaded is None or \
            time.time() - self._last_loaded > FILE_ATTRIBUTE_CACHE_TTL
        if not force and not self._is_new_file and not expired:
            return
        child = await self._get_child_by_name(self.name)
        if child is None:
            child = await self._get_child_by_path(self._path)
            if child is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
        self._validate(child)
        self._updated = child
        self._updated_name = None
        self._last_loaded = time.time()

    @property
    def parent(self) -> Optional[BaseInode]:
        return self._parent

    @property
    def storage(self):
        return self._parent.storage

    @property
    def object(self):
        return self._latest

    @property
    def name(self) -> Optional[str]:
        return self._updated_name or self._latest.name

    @property
    def _latest(self):
        return self._updated or self._original

    @property
    def date_created(self) -> Optional[str]:
        return self._latest.date_created

    @property
    def date_modified(self) -> Optional[str]:
        return self._latest.date_modified

    @property
    def _path(self) -> str:
        return self._get_path(self._latest)

    @property
    def path(self):
        path = self._path if not self._path.startswith('/') else self._path[1:]
        return f'{self.storage.path}{path}'

    @property
    def can_move(self):
        return True

    def _validate(self, object: Union[File, Folder]):
        raise NotImplementedError

    @property
    def _original(self) -> Any:
        raise NotImplementedError

    @property
    def _is_new_file(self) -> bool:
        raise NotImplementedError

    def _get_path(self, object: Any) -> str:
        try:
            return object.osf_path
        except AttributeError:
            return object.path

    async def _get_child_by_name(self, name: str) -> Optional[Union[File, Folder]]:
        async for child in self._parent.object.children:
            log.debug(f'searching({name})... child: {child}, name: {child.name}, path: {child.path}')
            if child.name == name:
                return child
        return None

    async def _get_child_by_path(self, path: str) -> Optional[Union[File, Folder]]:
        async for child in self._parent.object.children:
            log.debug(f'searching({path})... child: {child}, name: {child.name}, path: {child.path}')
            if self._get_path(child) == path:
                return child
        return None


class FolderInode(BaseFileInode):
    """The class for managing single folder inode."""
    def __init__(self, id: int, parent: BaseInode, folder: Folder):
        super(FolderInode, self).__init__(id, parent)
        self.folder = folder

    @property
    def _original(self):
        return self.folder

    def has_children(self):
        return True

    @property
    def _is_new_file(self):
        return False

    def _validate(self, object: Union[File, Folder]):
        pass

    @property
    def display_path(self):
        return f'{self.parent.display_path}{self.name}/'

    @property
    def can_create(self) -> bool:
        return True


class NewFile:
    """Dummy class for new file."""
    def __init__(self, parent: BaseInode, name: str):
        self.parent = parent
        self.name = name

    @property
    def path(self):
        return f'{self.parent.path}{self.name}'


class FileInode(BaseFileInode):
    """The class for managing single file inode."""
    _invalidated: bool

    def __init__(
        self,
        id: int,
        parent: BaseInode,
        file: Union[File, NewFile],
    ):
        super(FileInode, self).__init__(id, parent)
        self.file = file
        self._invalidated = False

    def invalidate(self, name: Optional[str] = None):
        super(FileInode, self).invalidate(name=name)
        self._invalidated = True

    async def refresh(self, context: 'Inodes', force=False):
        if self._is_new_file and not self._invalidated:
            # New file and not invalidated(=not written) -> no need to refresh
            return
        await super(FileInode, self).refresh(context, force=force)
        if self._is_new_file:
            self.file = self._updated

    @property
    def _original(self):
        return self.file

    @property
    def _is_new_file(self):
        return isinstance(self.file, NewFile)

    @property
    def name(self):
        if self._is_new_file:
            return self.file.name
        return super(FileInode, self).name

    @property
    def path(self):
        if self._is_new_file:
            return self.file.path
        return super(FileInode, self).path

    @property
    def date_created(self):
        if self._is_new_file:
            return None
        return super(FileInode, self).date_created

    @property
    def date_modified(self):
        if self._is_new_file:
            return None
        return super(FileInode, self).date_modified

    @property
    def size(self) -> Optional[Union[int, str]]:
        if not hasattr(self._latest, 'size'):
            return None
        if self._latest.size is None:
            return None
        if type(self._latest.size) != int and type(self._latest.size) != str:
            return None
        return self._latest.size

    def _validate(self, object: Union[File, Folder]):
        if isinstance(object, Folder):
            raise pyfuse3.FUSEError(errno.EISDIR)

    @property
    def display_path(self):
        return f'{self.parent.display_path}{self.name}'


class ChildRelation:
    """The class for managing child relations."""
    def __init__(self, parent: BaseInode, children: List[BaseInode]):
        """Initialize ChildRelation object."""
        self.parent = parent
        self.children = children


class Inodes:
    """The class for managing multiple inodes."""
    INODE_DUMMY = -1
    osf: OSF
    project: Optional[str]
    list_all_projects: bool
    osfproject: Optional[Project]
    _inodes: Dict[int, BaseInode]
    _child_relations: Cache

    def __init__(self, osf: OSF, project: Optional[str], list_all_projects: bool=False):
        """Initialize Inodes object."""
        super(Inodes, self).__init__()
        self.osf = osf
        self.project = project
        self.list_all_projects = list_all_projects
        self.osfproject = None
        self.offset_inode = pyfuse3.ROOT_INODE + 1
        self._inodes = {}
        self._child_relations = Cache(maxsize=256, ttl=LIST_CACHE_TTL, timer=time.time, default=None)
        self._projects_cache: Optional[List[Project]] = None
        self._projects_cache_loaded_at = 0.0
        self._project_children_cache = Cache(maxsize=256, ttl=LIST_CACHE_TTL, timer=time.time, default=None)
        self._project_linked_cache = Cache(maxsize=256, ttl=LIST_CACHE_TTL, timer=time.time, default=None)

    async def _get_osfproject(self):
        """Get OSF project object."""
        if self.osfproject is not None:
            return self.osfproject
        if self.project is None:
            raise ValueError('Project ID is not specified')
        self.osfproject = await self.osf.project(self.project)
        metadata = await self._fetch_node_attributes(self.osfproject.id)
        setattr(self.osfproject, '_rdmfs_attributes', metadata)
        return self.osfproject

    async def register(self, parent_inode: BaseInode, name: str):
        """Register new inode."""
        log.debug(f'register: path={parent_inode}, name={name}')
        newfile = NewFile(parent_inode, name)
        inode = await self._get_object_inode(parent_inode, newfile)
        log.debug(f'registered: inode={inode}')
        return inode

    def invalidate(self, inode: Union[int, BaseInode], name: Optional[str] = None):
        """Invalidate inode.

        If inode is integer, it is treated as inode number."""
        log.debug(f'invalidate: inode={inode}, name={name}')
        if isinstance(inode, int):
            if inode not in self._inodes:
                raise ValueError('Unexpected inode: {}'.format(inode))
            inode = self._inodes[inode]
        self._child_relations.delete(inode.id)
        inode.invalidate(name=name)

    async def get(self, inode_num: int) -> Optional[BaseInode]:
        """Find inode by inode number."""
        if inode_num in self._inodes:
            return self._inodes[inode_num]
        if inode_num == pyfuse3.ROOT_INODE:
            if self.list_all_projects:
                inode = ProjectsRootInode(pyfuse3.ROOT_INODE)
            else:
                project = await self._get_osfproject()
                metadata = getattr(project, '_rdmfs_attributes', None)
                inode = ProjectInode(pyfuse3.ROOT_INODE, project, metadata=metadata)
            self._inodes[inode_num] = inode
            return inode
        return None

    async def find_by_name(self, parent: BaseInode, name: str) -> Optional[BaseInode]:
        """Find inode by name."""
        if not parent.has_children():
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        cache: Optional[ChildRelation] = self._child_relations.get(parent.id)
        if cache is not None:
            # Use cache
            for child in cache.children:
                # Refresh child attributes for cached objects - occasionally they may be out of date
                try:
                    await child.refresh(self)
                except:
                    log.warning(f'Failed to refresh: {child}', exc_info=True)
                    continue
                if child.name == name:
                    return child
        # Request to GRDM
        found = None
        children: List[BaseInode] = []
        async for child in self.get_children_of(parent):
            children.append(await self._get_object_inode(parent, child))
            log.debug(f'find_by_name: name={name}, child={child.name}')
            if child.name == name:
                found = child
        cache = ChildRelation(parent, children)
        self._child_relations.set(parent.id, cache)
        if found is not None:
            return await self._get_object_inode(parent, found)
        # Find from new files
        return await self._find_new_file_by_name(parent, name)

    async def get_children_of(self, parent: BaseInode) -> AsyncGenerator[Union[Storage, File, Folder], None]:
        """Get children of the parent inode."""
        log.debug(f'get_children_of: parent={parent}')
        if not parent.has_children():
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        if isinstance(parent, ProjectsRootInode):
            for project in await self._list_projects():
                yield project
            return
        if isinstance(parent, ProjectInode):
            project = parent.object
            initial_attributes = getattr(project, '_rdmfs_attributes', None) or parent.metadata
            if initial_attributes:
                parent.update_metadata(initial_attributes)

            async def fetch_metadata():
                attrs = await self._fetch_node_attributes(project.id)
                parent.update_metadata(attrs)
                setattr(project, '_rdmfs_attributes', attrs)
                return attrs

            yield ProjectAttributesEntry(parent, fetch_metadata, initial_attributes)
            yield ProjectChildrenEntry(parent)
            yield ProjectLinkedEntry(parent)
            async for storage in project.storages:
                yield storage
            return
        if isinstance(parent, ProjectChildrenInode):
            project = parent.project_inode.object
            for child in await self._list_child_projects(project.id):
                yield child
            return
        if isinstance(parent, ProjectLinkedInode):
            project = parent.project_inode.object
            for linked in await self._list_linked_projects(project.id):
                yield linked
            return
        async for child in parent.object.children:
            yield child

    async def _get_object_inode(self, parent: BaseInode, object: Union[Project, Storage, File, Folder, ProjectAttributesEntry, ProjectChildrenEntry, ProjectLinkedEntry]) -> BaseInode:
        """Get inode for the object."""
        dummy_inode = self._create_object_inode(self.INODE_DUMMY, parent, object)
        for inode in self._inodes.values():
            if inode.removed:
                continue
            if inode.path == dummy_inode.path:
                if isinstance(inode, ProjectAttributesInode) and isinstance(dummy_inode, ProjectAttributesInode):
                    inode.project_inode.update_metadata(dummy_inode.object.attributes)
                    inode.set_content(dummy_inode.object)
                    inode.invalidate()
                return inode
        new_file_inode = await self._find_new_file_by_name(parent, dummy_inode.name)
        if new_file_inode is not None:
            return new_file_inode
        # Register new inode
        new_inode = None
        for inode in range(self.offset_inode, sys.maxsize):
            if inode not in self._inodes:
                new_inode = inode
                break
        if new_inode is None:
            raise ValueError('Cannot allocate new inodes')
        self._inodes[new_inode] = r = self._create_object_inode(new_inode, parent, object)
        log.debug(f'new inode: inode={r}')
        return r

    def _create_object_inode(self, inode_num: int, parent: BaseInode, object: Union[Project, Storage, File, Folder, ProjectAttributesEntry, ProjectChildrenEntry, ProjectLinkedEntry]) -> BaseInode:
        """Create inode object for the object."""
        if isinstance(object, Project):
            metadata = getattr(object, '_rdmfs_attributes', None)
            return ProjectInode(inode_num, object, parent, metadata=metadata)
        if isinstance(object, ProjectAttributesEntry):
            return ProjectAttributesInode(inode_num, object.project_inode, object)
        if isinstance(object, ProjectChildrenEntry):
            return ProjectChildrenInode(inode_num, object.project_inode)
        if isinstance(object, ProjectLinkedEntry):
            return ProjectLinkedInode(inode_num, object.project_inode)
        if isinstance(object, Storage):
            return StorageInode(inode_num, parent, object)
        if isinstance(object, Folder):
            return FolderInode(inode_num, parent, object)
        return FileInode(inode_num, parent, object)

    async def _find_new_file_by_name(self, parent: BaseInode, name: str) -> Optional[BaseInode]:
        """Find new file by name."""
        for inode in self._inodes.values():
            if inode.removed:
                continue
            if not isinstance(inode, FileInode):
                continue
            if inode.parent != parent:
                continue
            if inode._is_new_file and inode.name == name:
                # Refresh child attributes for cached objects - occasionally they may be out of date
                try:
                    await inode.refresh(self)
                except:
                    log.debug(f'Failed to refresh: {inode}', exc_info=True)
                return inode
        return None

    async def _list_projects(self) -> List[Project]:
        """Fetch and cache projects available to the authenticated user."""
        if not self.list_all_projects:
            raise ValueError('Listing projects is only available when mounting all projects')

        now = time.time()
        if self._projects_cache is not None and (now - self._projects_cache_loaded_at) < LIST_CACHE_TTL:
            return self._projects_cache

        url = self.osf._build_url('users', 'me', 'nodes')
        projects: Dict[str, Project] = {}
        async for node in self._paginate_nodes(url, page_size=NODE_PAGE_SIZE):
            project = self._build_project_from_node(node)
            if project is None:
                continue
            project_id = getattr(project, 'id', None)
            if not project_id:
                continue
            attributes = node.get('attributes', {}) or {}
            setattr(project, '_rdmfs_attributes', attributes)
            if 'title' not in attributes and hasattr(project, 'title'):
                attributes['title'] = project.title
            projects[project_id] = project

        ordered = sorted(projects.values(), key=lambda p: getattr(p, 'id', ''))
        self._projects_cache = ordered
        self._projects_cache_loaded_at = now
        return ordered

    async def _list_child_projects(self, project_id: str) -> List[Project]:
        cached = self._project_children_cache.get(project_id)
        if cached is not None:
            return cached
        url = self.osf._build_url('nodes', project_id, 'children')
        children: Dict[str, Project] = {}
        async for node in self._paginate_nodes(url, page_size=NODE_PAGE_SIZE):
            project = self._build_project_from_node(node)
            if project is None:
                continue
            child_id = getattr(project, 'id', None)
            if not child_id:
                continue
            children[child_id] = project
        ordered = sorted(children.values(), key=lambda p: getattr(p, 'id', ''))
        self._project_children_cache.set(project_id, ordered)
        return ordered

    async def _list_linked_projects(self, project_id: str) -> List[Project]:
        cached = self._project_linked_cache.get(project_id)
        if cached is not None:
            return cached
        url = self.osf._build_url('nodes', project_id, 'linked_nodes')
        linked: Dict[str, Project] = {}
        async for node in self._paginate_nodes(url, page_size=NODE_PAGE_SIZE):
            project = self._build_project_from_node(node)
            if project is None:
                continue
            linked_id = getattr(project, 'id', None)
            if not linked_id:
                continue
            linked[linked_id] = project
        ordered = sorted(linked.values(), key=lambda p: getattr(p, 'id', ''))
        self._project_linked_cache.set(project_id, ordered)
        return ordered

    async def _fetch_node_attributes(self, project_id: str) -> Dict[str, Any]:
        url = self.osf._build_url('nodes', project_id)
        response = await self.osf._get(url)
        payload = self.osf._json(response, 200)
        data = payload.get('data', {}) or {}
        attributes = data.get('attributes', {}) or {}
        return attributes

    def _build_project_from_node(self, node: Dict[str, Any]) -> Optional[Project]:
        if not isinstance(node, dict):
            return None
        if node.get('type') != 'nodes':
            return None
        attributes = node.get('attributes', {})
        # Skip registrations because they are read-only and do not expose files in the same way
        if attributes.get('registration'):
            return None
        node_id = node.get('id')
        if not node_id:
            return None
        related = (
            node.get('relationships', {})
                .get('files', {})
                .get('links', {})
                .get('related', {})
                .get('href')
        )
        if not related:
            related = self.osf._build_url('nodes', node_id, 'files')
        project_payload = {
            'data': {
                'id': node_id,
                'relationships': {
                    'files': {
                        'links': {
                            'related': {
                                'href': related
                            }
                        }
                    }
                }
            }
        }
        project = Project(project_payload, self.osf.session)
        title = attributes.get('title')
        if title is not None:
            setattr(project, 'title', title)
        setattr(project, 'name', node_id)
        setattr(project, '_rdmfs_attributes', attributes)
        return project

    async def _paginate_nodes(self, url: str, page_size: Optional[int] = None) -> AsyncGenerator[Dict[str, Any], None]:
        """Iterate through paginated OSF node listings following `links.next`."""
        next_url = self._with_page_size(url, page_size) if page_size is not None else url
        visited: set[str] = set()
        while next_url:
            response = await self.osf._get(next_url)
            payload = self.osf._json(response, 200)
            for node in payload.get('data', []) or []:
                yield node
            links = payload.get('links') or {}
            next_link = links.get('next')
            if not next_link or next_link in visited:
                break
            visited.add(next_url)
            next_url = next_link

    def _with_page_size(self, url: str, page_size: int) -> str:
        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query['page[size]'] = str(page_size)
        new_query = urlencode(query, doseq=True)
        return urlunparse(parsed._replace(query=new_query))
